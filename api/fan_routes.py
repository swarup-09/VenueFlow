"""
VenueFlow – Fan / Attendee API (Phase 3 · Module B)
====================================================
Personalised intelligence endpoints for individual fans.

Routes
------
  GET  /fan/{seat_id}/nearby        — top-3 nearest stands with walking routes
  POST /fan/{seat_id}/order         — place a food order, get delivery ETA
  GET  /fan/{seat_id}/orders        — order history for this seat

Seat-to-zone inference
----------------------
Seat IDs follow a loose convention: prefix letter(s) indicate the stand area.
  V* / VIP*  → VIP-Lounge
  U*         → Upper-Stands
  L* or *    → Lower-Stands  (default)

A stand row prefix before the hyphen maps to a concourse zone:
  A* / C* (Concourse-A/C) stands → Concourse-A / Concourse-C
  B* (Concourse-B)               → Concourse-B

The Digital Twin is also queried as the authoritative source when available.
"""

import json
import logging
import uuid
from datetime import datetime
from typing import Optional

from confluent_kafka import Producer, KafkaException
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from db.database import (
    get_pool,
    get_all_stands,
    get_stand,
    get_zone,
    insert_fan_order,
    get_fan_orders,
)
from intelligence.wait_estimator import WaitEstimator, STAND_MU, STAND_SERVERS
from intelligence.flow_predictor import FlowPredictor
from sensors.orchestrator import SensorOrchestrator

log = logging.getLogger("vf.fan")

router = APIRouter(prefix="/fan", tags=["Phase 3 – Fan API"])

# ── Shared intelligence instances (stateful — one per process) ────────────────
_wait_estimator = WaitEstimator(window=5)
_flow_predictor = FlowPredictor()
_orchestrator = SensorOrchestrator()

# ── Constants ─────────────────────────────────────────────────────────────────
ORDERS_TOPIC = "vf_orders"

# Maps stand_id → zone  (mirrors kafka_bridge.py STAND_ZONE_MAP)
STAND_ZONE_MAP: dict[str, str] = {
    "C4": "Concourse-A",
    "B2": "Concourse-B",
    "A7": "Concourse-A",
    "B5": "Concourse-B",
    "C1": "Concourse-C",
    "A3": "Concourse-A",
    "D2": "VIP-Lounge",
    "B8": "Concourse-B",
}

# Stand name lookup (matches DB seed data)
STAND_NAMES: dict[str, str] = {
    "C4": "Hot Dogs & Snacks",
    "B2": "Beer & Beverages",
    "A7": "Pizza Corner",
    "B5": "Burgers & Fries",
    "C1": "Coffee & Desserts",
    "A3": "Nachos & Wraps",
    "D2": "VIP Bar",
    "B8": "South Grill",
}

# Average walking speed for delivery estimation (seconds per venue-graph hop)
DELIVERY_OVERHEAD_SECONDS = 60.0  # packaging + handoff buffer


# ── Pydantic models ───────────────────────────────────────────────────────────

class OrderItem(BaseModel):
    name: str = Field(..., example="Nachos")
    quantity: int = Field(..., ge=1, le=20)
    price: float = Field(..., ge=0.0, example=8.50)


class OrderRequest(BaseModel):
    stand_id: str = Field(..., example="C4")
    items: list[OrderItem] = Field(..., min_length=1)
    special_instructions: Optional[str] = Field(None, max_length=200)


class NearbyStandResult(BaseModel):
    stand_id: str
    stand_name: str
    zone: str
    wait_minutes: float
    predicted_wait_5min: float
    path: list[str]
    estimated_walk_minutes: float
    total_eta_minutes: float
    is_open: bool
    halftime_spike_expected: bool


class NearbyResponse(BaseModel):
    seat_id: str
    fan_zone: str
    stands: list[NearbyStandResult]
    generated_at: str


class OrderResponse(BaseModel):
    order_id: str
    seat_id: str
    stand_id: str
    stand_name: str
    items: list[OrderItem]
    status: str
    queue_wait_minutes: float
    walk_minutes: float
    delivery_eta_minutes: float
    delivery_eta_timestamp: str
    placed_at: str
    total_price: float
    special_instructions: Optional[str] = None


# ── Kafka producer (lazy singleton) ──────────────────────────────────────────

import os
KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
_producer: Optional[Producer] = None


def _get_producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = Producer({
            "bootstrap.servers": KAFKA_BROKERS,
            "linger.ms": 10,
            "compression.type": "lz4",
        })
    return _producer


def _publish(topic: str, key: str, payload: dict) -> None:
    try:
        p = _get_producer()
        p.produce(topic, key=key.encode(), value=json.dumps(payload, default=str).encode())
        p.poll(0)
    except KafkaException as exc:
        log.warning("Kafka publish to %s failed: %s", topic, exc)


# ── Zone helpers ──────────────────────────────────────────────────────────────

def _infer_zone_from_seat(seat_id: str) -> str:
    """
    Heuristic zone inference from seat identifier.

    Convention (used throughout the virtual venue):
      VIP-* or V*   → VIP-Lounge
      U*            → Upper-Stands
      everything else → Lower-Stands  (largest seated area)
    """
    sid = seat_id.upper()
    if sid.startswith("VIP") or (sid.startswith("V") and not sid.startswith("VIP-")):
        return "VIP-Lounge"
    if sid.startswith("U"):
        return "Upper-Stands"
    return "Lower-Stands"


async def _resolve_fan_zone(seat_id: str) -> str:
    """
    Authoritative zone for a seat.

    1. Check Digital Twin zones table for a matching prefix entry.
    2. Fall back to heuristic inference.
    """
    # Try Digital Twin lookup (zone may be stored if we tracked this seat before)
    try:
        pool = await get_pool()
        row = await pool.fetchrow(
            "SELECT zone FROM fan_orders WHERE seat_id = $1 ORDER BY placed_at DESC LIMIT 1",
            seat_id,
        )
        if row and row["zone"]:
            return row["zone"]
    except Exception:
        pass  # table may not exist yet or DB not available
    return _infer_zone_from_seat(seat_id)


# ── Route helpers ─────────────────────────────────────────────────────────────

def _get_walk_route(from_zone: str, to_zone: str) -> tuple[list[str], float]:
    """
    Return (path, estimated_minutes) using live crowd data from the orchestrator.
    Falls back to a direct single-hop if from_zone == to_zone or route fails.
    """
    if from_zone == to_zone:
        return [from_zone], 0.0

    try:
        crowd_readings = _orchestrator.crowd_array.read_all()
        raw = [r.model_dump() for r in crowd_readings]
        result = _flow_predictor.best_route(from_zone, to_zone, raw)
        if result["reachable"]:
            return result["path"], result["estimated_minutes"] or 0.0
    except Exception as exc:
        log.debug("Route computation failed (%s→%s): %s", from_zone, to_zone, exc)
    return [from_zone, to_zone], 2.0   # conservative fallback


# ── Endpoint: GET /fan/{seat_id}/nearby ──────────────────────────────────────

@router.get(
    "/{seat_id}/nearby",
    response_model=NearbyResponse,
    summary="Top-3 nearest concession stands for a fan's seat",
)
async def fan_nearby(
    seat_id: str,
    top_n: int = Query(default=3, ge=1, le=8, description="Number of stands to return"),
):
    """
    Returns the **top `top_n` concession stands** ranked by current wait time,
    enriched with the crowd-aware walking route from the fan's zone and a
    combined **total_eta_minutes** (queue wait + walk).

    Uses the Digital Twin for live stand state and the Flow Predictor for routing.
    """
    fan_zone = await _resolve_fan_zone(seat_id)

    # Pull all stands from Digital Twin; fall back to orchestrator if DB is cold
    try:
        all_stands = await get_all_stands()
        # Filter open stands only
        open_stands = [s for s in all_stands if s.get("is_open", True)]
    except Exception as exc:
        log.warning("DB unavailable for nearby — using orchestrator: %s", exc)
        queue_readings = _orchestrator.queue_array.read_all()
        open_stands = [r.model_dump() for r in queue_readings if r.is_open]

    # Sort by current wait_minutes ascending and take top N
    open_stands.sort(key=lambda s: s.get("wait_minutes") or 0.0)
    top_stands = open_stands[:top_n]

    # Enrich each stand with route + M/M/c prediction
    results: list[NearbyStandResult] = []
    for stand in top_stands:
        stand_id    = stand["stand_id"]
        stand_zone  = STAND_ZONE_MAP.get(stand_id, stand.get("zone", "Concourse-A"))
        wait_now    = float(stand.get("wait_minutes") or 0.0)

        # Walking route from fan zone → stand zone
        path, walk_min = _get_walk_route(fan_zone, stand_zone)

        # M/M/c 5-minute prediction (use DB value if present, else run estimator)
        pred_5 = stand.get("predicted_wait_5min")
        if pred_5 is None:
            try:
                pred = _wait_estimator.predict(
                    stand_id,
                    stand.get("stand_name", STAND_NAMES.get(stand_id, stand_id)),
                    stand.get("queue_length", 0),
                    wait_now,
                )
                pred_5 = pred.predicted_wait_5min
                halftime_spike = pred.halftime_spike_expected
            except Exception:
                pred_5, halftime_spike = wait_now, False
        else:
            halftime_spike = bool(stand.get("halftime_spike_expected", False))

        results.append(NearbyStandResult(
            stand_id=stand_id,
            stand_name=stand.get("stand_name", STAND_NAMES.get(stand_id, stand_id)),
            zone=stand_zone,
            wait_minutes=round(wait_now, 2),
            predicted_wait_5min=round(float(pred_5), 2),
            path=path,
            estimated_walk_minutes=round(walk_min, 2),
            total_eta_minutes=round(wait_now + walk_min, 2),
            is_open=bool(stand.get("is_open", True)),
            halftime_spike_expected=halftime_spike,
        ))

    return NearbyResponse(
        seat_id=seat_id,
        fan_zone=fan_zone,
        stands=results,
        generated_at=datetime.utcnow().isoformat(),
    )


# ── Endpoint: POST /fan/{seat_id}/order ──────────────────────────────────────

@router.post(
    "/{seat_id}/order",
    response_model=OrderResponse,
    status_code=201,
    summary="Place a food order for a specific seat",
)
async def fan_place_order(seat_id: str, body: OrderRequest):
    """
    Places a mobile food order for a fan's seat.

    1. Validates the stand is known and open.
    2. Runs the **M/M/c Wait Estimator** to compute the queue wait.
    3. Computes walking delivery time via the **Flow Predictor**.
    4. Publishes to the **`vf_orders`** Kafka topic for downstream fulfilment.
    5. Persists the order to TimescaleDB for history queries.

    Returns a full `delivery_eta_minutes` = queue wait + walk + overhead.
    """
    stand_id = body.stand_id.upper()

    # ── Validate stand ────────────────────────────────────────────────────────
    stand_name = STAND_NAMES.get(stand_id)
    stand_zone = STAND_ZONE_MAP.get(stand_id)
    if not stand_name or not stand_zone:
        raise HTTPException(
            status_code=404,
            detail=f"Stand '{stand_id}' not found. Valid IDs: {sorted(STAND_NAMES.keys())}",
        )

    # Try to verify stand is open via Digital Twin
    try:
        dt_stand = await get_stand(stand_id)
        if dt_stand and not dt_stand.get("is_open", True):
            raise HTTPException(status_code=409, detail=f"Stand '{stand_id}' is currently closed")
        queue_length = dt_stand.get("queue_length", 0) if dt_stand else 0
        current_wait = dt_stand.get("wait_minutes", 0.0) if dt_stand else 0.0
    except HTTPException:
        raise
    except Exception as exc:
        log.warning("DB stand lookup failed, using defaults: %s", exc)
        queue_length, current_wait = 0, 0.0

    # ── M/M/c delivery wait ───────────────────────────────────────────────────
    try:
        pred = _wait_estimator.predict(stand_id, stand_name, queue_length, current_wait)
        queue_wait_min = pred.predicted_wait_5min   # order-ahead ≈ 5-min window
    except Exception as exc:
        log.warning("WaitEstimator failed: %s", exc)
        queue_wait_min = float(current_wait) if current_wait else 5.0

    # ── Walking delivery time (stand zone → fan seat zone) ───────────────────
    fan_zone = await _resolve_fan_zone(seat_id)
    _, walk_min = _get_walk_route(stand_zone, fan_zone)
    overhead_min = DELIVERY_OVERHEAD_SECONDS / 60.0

    delivery_eta_min = round(queue_wait_min + walk_min + overhead_min, 2)
    delivery_eta_ts = datetime.utcnow()
    from datetime import timedelta
    eta_at = delivery_eta_ts + timedelta(minutes=delivery_eta_min)

    # ── Build order record ────────────────────────────────────────────────────
    order_id     = str(uuid.uuid4())
    placed_at    = datetime.utcnow()
    total_price  = round(sum(i.price * i.quantity for i in body.items), 2)
    items_json   = [i.model_dump() for i in body.items]

    order_record = {
        "order_id": order_id,
        "seat_id": seat_id,
        "stand_id": stand_id,
        "stand_name": stand_name,
        "zone": fan_zone,
        "items": items_json,
        "status": "placed",
        "queue_wait_minutes": round(queue_wait_min, 2),
        "walk_minutes": round(walk_min, 2),
        "delivery_eta_minutes": delivery_eta_min,
        "delivery_eta_timestamp": eta_at.isoformat(),
        "placed_at": placed_at.isoformat(),
        "total_price": total_price,
        "special_instructions": body.special_instructions,
    }

    # ── Publish to Kafka vf_orders ────────────────────────────────────────────
    _publish(ORDERS_TOPIC, f"order:{seat_id}", order_record)
    log.info("Order %s placed — seat=%s stand=%s ETA=%.1f min",
             order_id, seat_id, stand_id, delivery_eta_min)

    # ── Persist to TimescaleDB ────────────────────────────────────────────────
    try:
        await insert_fan_order({
            **order_record,
            "placed_at": placed_at,
            "items": json.dumps(items_json),
        })
    except Exception as exc:
        log.warning("DB insert fan_order failed (order still published): %s", exc)

    return OrderResponse(
        order_id=order_id,
        seat_id=seat_id,
        stand_id=stand_id,
        stand_name=stand_name,
        items=body.items,
        status="placed",
        queue_wait_minutes=round(queue_wait_min, 2),
        walk_minutes=round(walk_min, 2),
        delivery_eta_minutes=delivery_eta_min,
        delivery_eta_timestamp=eta_at.isoformat(),
        placed_at=placed_at.isoformat(),
        total_price=total_price,
        special_instructions=body.special_instructions,
    )


# ── Endpoint: GET /fan/{seat_id}/orders ──────────────────────────────────────

@router.get(
    "/{seat_id}/orders",
    summary="Order history for a fan's seat",
)
async def fan_order_history(
    seat_id: str,
    limit: int = Query(default=20, ge=1, le=100),
    status: Optional[str] = Query(
        default=None,
        description="Filter by status: placed | preparing | delivered | cancelled",
    ),
):
    """
    Returns the order history for a given seat from TimescaleDB.

    Orders are returned newest-first. Use `status` to filter by lifecycle stage.
    """
    try:
        orders = await get_fan_orders(seat_id, limit=limit, status_filter=status)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"DB unavailable: {exc}")

    # Deserialise the items JSON column back to dicts
    for order in orders:
        if isinstance(order.get("items"), str):
            try:
                order["items"] = json.loads(order["items"])
            except Exception:
                order["items"] = []

    return {
        "seat_id": seat_id,
        "total": len(orders),
        "orders": orders,
    }
