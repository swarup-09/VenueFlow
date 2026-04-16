"""
VenueFlow – FastAPI Server (Phase 3)
=====================================
Phase 1: Virtual sensors + Kafka producer
Phase 2: Digital Twin queries, Intelligence Layer endpoints, route planning
Phase 3: WebSocket push — fan rooms, zone broadcast, ops channel

Run:
    cd venueflow
    uvicorn api.main:app --reload --port 8000

Docs: http://localhost:8000/docs
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncGenerator, Optional

from confluent_kafka import Producer, KafkaException
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from api.websocket_manager import manager as ws_manager
from api.kafka_bridge import bridge as kafka_bridge
from api.fan_routes import router as fan_router
from api.ops_routes import router as ops_router

from models.schemas import (
    VenueSnapshot, CrowdSensorReading, QueueSensorReading,
    BLEBeaconReading, CCTVReading,
)
from sensors.orchestrator import SensorOrchestrator
from intelligence.wait_estimator import WaitEstimator
from intelligence.flow_predictor import FlowPredictor
from db.database import (
    get_pool, close_pool,
    get_all_zones, get_zone, get_all_stands, get_stand,
    get_active_alerts, get_crowd_history, get_queue_history,
)

log = logging.getLogger("vf.api")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

orchestrator = SensorOrchestrator()
wait_estimator = WaitEstimator(window=5)
flow_predictor = FlowPredictor()

_kafka_producer: Optional[Producer] = None
_kafka_ready = False


def _get_producer() -> Producer:
    global _kafka_producer, _kafka_ready
    if _kafka_producer is None:
        _kafka_producer = Producer({
            "bootstrap.servers": KAFKA_BROKERS,
            "linger.ms": 10,
            "compression.type": "lz4",
        })
        _kafka_ready = True
    return _kafka_producer


def _publish(topic: str, key: str, payload: dict):
    try:
        p = _get_producer()
        p.produce(topic, key=key.encode(), value=json.dumps(payload, default=str).encode())
        p.poll(0)
    except KafkaException as e:
        log.warning(f"Kafka publish failed: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("VenueFlow API starting – Phase 3")
    try:
        _get_producer()
        log.info("Kafka producer ready")
    except Exception as e:
        log.warning(f"Kafka not available: {e}")
    try:
        await get_pool()
        log.info("TimescaleDB pool ready")
    except Exception as e:
        log.warning(f"DB not available: {e}")

    # Phase 3 – start the Kafka → WebSocket bridge as a background task
    bridge_task = kafka_bridge.start()
    log.info("Kafka→WS bridge started")

    yield

    # Shutdown: stop bridge first, then flush Kafka producer and DB
    await kafka_bridge.stop()
    log.info("Kafka→WS bridge shut down")
    if _kafka_producer:
        _kafka_producer.flush(timeout=5)
    await close_pool()


app = FastAPI(
    title="VenueFlow API — Phase 3",
    description=(
        "**Phase 1**: Virtual sensors + Kafka producer\n\n"
        "**Phase 2**: Digital Twin (TimescaleDB), M/M/c Wait Estimator, "
        "Graph-based Flow Predictor\n\n"
        "**Phase 3 · Module A**: WebSocket push — `ws/fan/{seat_id}`, "
        "`ws/zone/{zone}`, `ws/ops`\n\n"
        "**Phase 3 · Module B**: Fan API — `GET /fan/{seat_id}/nearby`, "
        "`POST /fan/{seat_id}/order`, `GET /fan/{seat_id}/orders`\n\n"
        "**Phase 3 · Module C**: Ops Command — `GET /ops/heatmap`, "
        "`GET /ops/exit-plan`, `POST /ops/alerts/resolve/{alert_id}`"
    ),
    version="3.2.0",
    lifespan=lifespan,
)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Phase 3 · Module B — Fan / Attendee routes
app.include_router(fan_router)

# Phase 3 · Module C — Ops Command routes
app.include_router(ops_router)


@app.get("/", tags=["Info"])
async def root():
    return {"service": "VenueFlow API", "version": "3.1.0", "docs": "/docs"}


@app.get("/health", tags=["Info"])
async def health():
    db_ok = False
    unresolved_critical_alerts: Optional[int] = None
    try:
        pool = await get_pool()
        await pool.fetchval("SELECT 1")
        db_ok = True
        # Count unresolved critical alerts — surfaced on the health dashboard
        # so ops staff immediately see severity-1 issues.
        unresolved_critical_alerts = await pool.fetchval(
            """
            SELECT COUNT(*)
            FROM alerts
            WHERE severity = 'critical'
              AND resolved = FALSE
            """
        )
        unresolved_critical_alerts = int(unresolved_critical_alerts or 0)
    except Exception:
        pass
    ws_stats = await ws_manager.stats()
    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat(),
        "kafka_producer_ready": _kafka_ready,
        "timescaledb_connected": db_ok,
        "unresolved_critical_alerts": unresolved_critical_alerts,
        "version": "venueflow-v3.2.0",
        "websocket": ws_stats,
    }


@app.get("/ws/debug", tags=["Phase 3 – WebSocket"])
async def ws_debug():
    """Live WebSocket room stats — useful during development and ops."""
    return await ws_manager.stats()


# ── Phase 1: Sensors ──────────────────────────────────────────────────────────

@app.get("/sensors/snapshot", response_model=VenueSnapshot, tags=["Phase 1 – Sensors"])
async def get_snapshot():
    snapshot = orchestrator.get_snapshot()
    for cr in snapshot.crowd_sensors:
        _publish("vf_crowd_raw", cr.zone, cr.model_dump(mode="json"))
    for qr in snapshot.queue_sensors:
        _publish("vf_queues_raw", qr.stand_id, qr.model_dump(mode="json"))
    return snapshot


@app.get("/sensors/crowd", response_model=list[CrowdSensorReading], tags=["Phase 1 – Sensors"])
async def get_crowd():
    readings = orchestrator.crowd_array.read_all()
    for r in readings:
        _publish("vf_crowd_raw", r.zone, r.model_dump(mode="json"))
    return readings


@app.get("/sensors/crowd/{zone}", response_model=list[CrowdSensorReading], tags=["Phase 1 – Sensors"])
async def get_crowd_zone(zone: str):
    readings = orchestrator.crowd_array.read_zone(zone)
    if not readings:
        raise HTTPException(404, f"Zone '{zone}' not found")
    return readings


@app.get("/sensors/queues", response_model=list[QueueSensorReading], tags=["Phase 1 – Sensors"])
async def get_queues():
    readings = orchestrator.queue_array.read_all()
    for r in readings:
        _publish("vf_queues_raw", r.stand_id, r.model_dump(mode="json"))
    return readings


@app.get("/sensors/queues/shortest", response_model=list[QueueSensorReading], tags=["Phase 1 – Sensors"])
async def get_shortest_queues(top_n: int = Query(default=3, ge=1, le=8)):
    return orchestrator.queue_array.get_shortest_queues(top_n)


@app.get("/sensors/queues/{stand_id}", response_model=QueueSensorReading, tags=["Phase 1 – Sensors"])
async def get_queue_stand(stand_id: str):
    r = orchestrator.queue_array.read_stand(stand_id.upper())
    if not r:
        raise HTTPException(404, f"Stand '{stand_id}' not found")
    return r


@app.get("/sensors/ble", response_model=list[BLEBeaconReading], tags=["Phase 1 – Sensors"])
async def get_ble():
    return orchestrator.ble_array.read_all()


@app.get("/sensors/cctv", response_model=list[CCTVReading], tags=["Phase 1 – Sensors"])
async def get_cctv():
    return orchestrator.cctv_array.read_all()


@app.get("/sensors/cctv/anomalies", response_model=list[CCTVReading], tags=["Phase 1 – Sensors"])
async def get_cctv_anomalies():
    return orchestrator.cctv_array.get_anomalies()


async def _sse_generator(interval: float) -> AsyncGenerator[str, None]:
    while True:
        snapshot = orchestrator.get_snapshot()
        for cr in snapshot.crowd_sensors:
            _publish("vf_crowd_raw", cr.zone, cr.model_dump(mode="json"))
        for qr in snapshot.queue_sensors:
            _publish("vf_queues_raw", qr.stand_id, qr.model_dump(mode="json"))
        yield f"event: snapshot\ndata: {snapshot.model_dump_json()}\n\n"
        await asyncio.sleep(interval)


@app.get("/sensors/stream", tags=["Phase 1 – Sensors"])
async def stream_sensors(interval: float = Query(default=3.0, ge=1.0, le=30.0)):
    """SSE live stream — pushes VenueSnapshot every `interval` seconds."""
    return StreamingResponse(
        _sse_generator(interval),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── Phase 2: Digital Twin ─────────────────────────────────────────────────────

@app.get("/twin/zones", tags=["Phase 2 – Digital Twin"])
async def twin_zones():
    try:
        return await get_all_zones()
    except Exception as e:
        raise HTTPException(503, f"DB unavailable: {e}")


@app.get("/twin/zones/{zone}", tags=["Phase 2 – Digital Twin"])
async def twin_zone(zone: str):
    try:
        row = await get_zone(zone)
    except Exception as e:
        raise HTTPException(503, f"DB unavailable: {e}")
    if not row:
        raise HTTPException(404, f"Zone '{zone}' not in Digital Twin yet")
    return row


@app.get("/twin/stands", tags=["Phase 2 – Digital Twin"])
async def twin_stands():
    try:
        return await get_all_stands()
    except Exception as e:
        raise HTTPException(503, f"DB unavailable: {e}")


@app.get("/twin/stands/{stand_id}", tags=["Phase 2 – Digital Twin"])
async def twin_stand(stand_id: str):
    """Answer: 'How long is the line at Stand C4?' — from the Digital Twin."""
    try:
        row = await get_stand(stand_id.upper())
    except Exception as e:
        raise HTTPException(503, f"DB unavailable: {e}")
    if not row:
        raise HTTPException(404, f"Stand '{stand_id}' not found")
    return row


@app.get("/twin/alerts", tags=["Phase 2 – Digital Twin"])
async def twin_alerts(limit: int = Query(default=20, ge=1, le=100)):
    try:
        return await get_active_alerts(limit)
    except Exception as e:
        raise HTTPException(503, f"DB unavailable: {e}")


# ── Phase 2: History ──────────────────────────────────────────────────────────

@app.get("/history/crowd/{zone}", tags=["Phase 2 – History"])
async def crowd_history(zone: str, minutes: int = Query(default=30, ge=5, le=1440)):
    try:
        return await get_crowd_history(zone, minutes)
    except Exception as e:
        raise HTTPException(503, f"DB unavailable: {e}")


@app.get("/history/queues/{stand_id}", tags=["Phase 2 – History"])
async def queue_history(stand_id: str, minutes: int = Query(default=30, ge=5, le=1440)):
    try:
        return await get_queue_history(stand_id.upper(), minutes)
    except Exception as e:
        raise HTTPException(503, f"DB unavailable: {e}")


# ── Phase 2: Intelligence (on-demand, no DB needed) ───────────────────────────

@app.get("/intelligence/wait", tags=["Phase 2 – Intelligence"])
async def intelligence_wait():
    """M/M/c Wait Estimator — live queue predictions with halftime spike detection."""
    queue_readings = orchestrator.queue_array.read_all()
    raw = [r.model_dump() for r in queue_readings]
    predictions = wait_estimator.predict_all(raw)
    return [
        {
            "stand_id": p.stand_id,
            "stand_name": p.stand_name,
            "current_wait_min": p.current_wait_min,
            "predicted_wait_5min": p.predicted_wait_5min,
            "predicted_wait_10min": p.predicted_wait_10min,
            "arrival_rate_lambda": p.lambda_,
            "service_rate_mu": p.mu,
            "servers_c": p.c,
            "utilization_rho": p.rho,
            "halftime_spike_expected": p.halftime_spike_expected,
            "recommendation": p.recommendation,
        }
        for p in predictions
    ]


@app.get("/intelligence/flow", tags=["Phase 2 – Intelligence"])
async def intelligence_flow():
    """Flow Predictor — Dijkstra crowd-aware routing, ETS density forecast per zone."""
    crowd_readings = orchestrator.crowd_array.read_all()
    raw = [r.model_dump() for r in crowd_readings]
    predictions = flow_predictor.predict_all(raw)
    return [
        {
            "zone": p.zone,
            "current_density": p.current_density,
            "predicted_density_5min": p.predicted_density,
            "congestion_risk": p.congestion_risk,
            "recommended_route": p.recommended_route,
            "avoid_zones": p.avoid_zones,
        }
        for p in predictions
    ]


@app.get("/intelligence/route", tags=["Phase 2 – Intelligence"])
async def intelligence_route(
    from_zone: str = Query(..., example="Gate-North"),
    to_zone: str   = Query(..., example="Lower-Stands"),
):
    """Optimal walking route between two zones — density-weighted Dijkstra."""
    crowd_readings = orchestrator.crowd_array.read_all()
    raw = [r.model_dump() for r in crowd_readings]
    try:
        result = flow_predictor.best_route(from_zone, to_zone, raw)
    except Exception as e:
        raise HTTPException(400, str(e))
    if not result["reachable"]:
        raise HTTPException(503, f"No passable route from '{from_zone}' to '{to_zone}'")
    return result


@app.get("/alerts", tags=["Phase 2 – Intelligence"])
async def live_alerts():
    """Combined sensor + intelligence alerts."""
    snapshot = orchestrator.get_snapshot()
    queue_raw = [r.model_dump() for r in orchestrator.queue_array.read_all()]
    wait_preds = wait_estimator.predict_all(queue_raw)
    spike_alerts = [
        {"severity": "warning", "category": "intelligence",
         "message": p.recommendation, "stand_id": p.stand_id}
        for p in wait_preds if p.halftime_spike_expected
    ]
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "congestion_level": snapshot.venue_congestion_level,
        "sensor_alerts": snapshot.alerts,
        "intelligence_alerts": spike_alerts,
        "total": len(snapshot.alerts) + len(spike_alerts),
    }


# ── Phase 3: WebSocket endpoints ──────────────────────────────────────────────

@app.websocket("/ws/fan/{seat_id}")
async def ws_fan(websocket: WebSocket, seat_id: str):
    """
    Personalised WebSocket channel for a single fan identified by their seat ID.

    The client will receive:
    • Real-time alerts targeted at this seat (e.g. food-delivery ETA)
    • Any alert escalated from the fan's zone or stand
    """
    await ws_manager.connect_fan(websocket, seat_id)
    log.info("WS /ws/fan/%s opened", seat_id)
    try:
        while True:
            # Keep the connection alive; the bridge pushes from the server side.
            # We echo any client ping payloads back to confirm liveness.
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        log.info("WS /ws/fan/%s closed", seat_id)
    finally:
        await ws_manager.disconnect(websocket)


@app.websocket("/ws/zone/{zone}")
async def ws_zone(websocket: WebSocket, zone: str):
    """
    Zone-wide WebSocket broadcast channel.

    Receives:
    • Flow intelligence updates for this zone (density, congestion risk, routing)
    • Alerts originating in this zone (crowd, queue, CCTV anomaly)
    """
    await ws_manager.connect_zone(websocket, zone)
    log.info("WS /ws/zone/%s opened", zone)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        log.info("WS /ws/zone/%s closed", zone)
    finally:
        await ws_manager.disconnect(websocket)


@app.websocket("/ws/ops")
async def ws_ops(websocket: WebSocket):
    """
    Global operations WebSocket channel for staff dashboards.

    Receives every message the bridge dispatches — flow intelligence,
    wait intelligence, and all alert severities — making this the
    full-fidelity feed for the ops console.
    """
    await ws_manager.connect_ops(websocket)
    log.info("WS /ws/ops opened")
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        log.info("WS /ws/ops closed")
    finally:
        await ws_manager.disconnect(websocket)
