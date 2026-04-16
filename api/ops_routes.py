"""
VenueFlow – Ops Command Layer (Phase 3 · Module C)
===================================================
Provides venue-staff and operations-centre endpoints for a bird's-eye view of
the stadium and automated post-match crowd egress management.

Routes
------
  GET  /ops/heatmap               — Nested thermal view of every zone + stand
  GET  /ops/exit-plan             — Staggered departure windows, auto-rerouting
                                    around hard-blocked (>90 % density) gates
  POST /ops/alerts/resolve/{id}   — Mark an alert UUID as resolved

Database: asyncpg pool (shared via db.database.get_pool)
All queries are async and non-blocking to support the Dell G15-hosted
high-concurrency stack.
"""

import logging
import math
import uuid
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from db.database import get_pool
from intelligence.flow_predictor import BLOCK_THRESHOLD, VENUE_EDGES

log = logging.getLogger("vf.ops")

router = APIRouter(prefix="/ops", tags=["Phase 3 – Ops Command"])

# ── Constants ─────────────────────────────────────────────────────────────────

# Assumed match-end time for the exit planner.
# In production this would come from an event calendar service; here we
# default to "now + 0 minutes" so the planner is always exercisable.
MATCH_END_OFFSET_MINUTES: float = 0.0

# Hard-block threshold (mirrors flow_predictor.BLOCK_THRESHOLD = 0.90)
GATE_BLOCK_DENSITY = BLOCK_THRESHOLD  # 0.90

# Exit planner formula weights
# T_departure = T_match_end + (ZoneDistance × CongestionWeight)
# ZoneDistance is measured in graph hops from the seating zone to the nearest gate.
# CongestionWeight = 1 + density (higher density → push departure later)
EXIT_WINDOW_MINUTES = 5  # each window slot is 5 minutes wide

# Seating zones ordered by natural priority (lower index = closer to exit)
SEATING_ZONES = [
    "VIP-Lounge",
    "Lower-Stands",
    "Upper-Stands",
    "Concourse-A",
    "Concourse-B",
    "Concourse-C",
]

# Gate zones (nodes in VENUE_EDGES that connect to Parking-Px)
GATE_ZONES = {"Gate-North", "Gate-South", "Gate-East", "Gate-West"}

# Nearest gate(s) per seating zone (heuristic, based on VENUE_EDGES topology)
ZONE_NEAREST_GATES: dict[str, list[str]] = {
    "VIP-Lounge":   ["Gate-North"],
    "Lower-Stands": ["Gate-North", "Gate-East"],
    "Upper-Stands": ["Gate-East", "Gate-North"],
    "Concourse-A":  ["Gate-North", "Gate-East"],
    "Concourse-B":  ["Gate-East"],
    "Concourse-C":  ["Gate-South", "Gate-West"],
}

# Fallback distance (hops) when we cannot traverse the graph
FALLBACK_ZONE_DISTANCE: dict[str, int] = {
    "VIP-Lounge":   1,
    "Lower-Stands": 2,
    "Upper-Stands": 3,
    "Concourse-A":  2,
    "Concourse-B":  2,
    "Concourse-C":  2,
}

# All gate zones in priority order (used for rerouting)
ALL_GATES_ORDERED = ["Gate-North", "Gate-East", "Gate-South", "Gate-West"]


# ── Pydantic response models ───────────────────────────────────────────────────

class StandHeatState(BaseModel):
    stand_id: str
    stand_name: str
    zone: str
    queue_length: Optional[int]
    wait_minutes: Optional[float]
    capacity_pct: Optional[float]
    predicted_wait_5min: Optional[float]
    is_open: bool
    halftime_spike_expected: bool


class ZoneHeatState(BaseModel):
    zone: str
    density: Optional[float]
    headcount: Optional[int]
    flow_rate: Optional[float]
    congestion_level: Optional[str]
    anomaly_active: bool
    last_updated: Optional[str]
    stands: list[StandHeatState]


class HeatmapResponse(BaseModel):
    generated_at: str
    total_zones: int
    total_stands: int
    critical_zones: list[str]
    zones: list[ZoneHeatState]


class ExitWindow(BaseModel):
    zone: str
    priority: int                        # 1 = depart first
    recommended_gate: str
    gate_was_rerouted: bool
    departure_offset_minutes: float      # minutes after match_end
    window_start: str                    # ISO timestamp
    window_end: str                      # ISO timestamp (start + 5 min)
    zone_density: Optional[float]
    congestion_weight: float
    note: Optional[str] = None


class ExitPlanResponse(BaseModel):
    generated_at: str
    match_end: str
    total_zones: int
    hard_blocked_gates: list[str]
    exit_windows: list[ExitWindow]


class ResolveAlertResponse(BaseModel):
    alert_id: str
    resolved: bool
    message: str


# ── Internal query helpers ─────────────────────────────────────────────────────

async def _fetch_zones_with_stands() -> list[dict]:
    """
    JOIN digital_twin_zones ← digital_twin_stands to build a
    zone-keyed dict enriched with its stand list.
    """
    pool = await get_pool()

    zones = await pool.fetch(
        """
        SELECT
            zone, last_updated, density, headcount, flow_rate,
            direction, congestion_level, anomaly_active
        FROM digital_twin_zones
        ORDER BY zone
        """
    )
    stands = await pool.fetch(
        """
        SELECT
            stand_id, stand_name, zone, queue_length, wait_minutes,
            capacity_pct, predicted_wait_5min, is_open,
            halftime_spike_expected
        FROM digital_twin_stands
        ORDER BY zone, wait_minutes ASC
        """
    )

    # Index stands by zone
    stands_by_zone: dict[str, list[dict]] = {}
    for s in stands:
        d = dict(s)
        stands_by_zone.setdefault(d["zone"], []).append(d)

    # Merge
    result: list[dict] = []
    for z in zones:
        zd = dict(z)
        zd["stands"] = stands_by_zone.get(zd["zone"], [])
        result.append(zd)

    # Also include zones that appear only in stands (Digital Twin not yet seeded)
    known_zones = {r["zone"] for r in result}
    for zone, stand_list in stands_by_zone.items():
        if zone not in known_zones:
            result.append({
                "zone": zone,
                "last_updated": None,
                "density": None,
                "headcount": None,
                "flow_rate": None,
                "direction": None,
                "congestion_level": None,
                "anomaly_active": False,
                "stands": stand_list,
            })

    return result


async def _fetch_gate_throughput() -> dict[str, dict]:
    """
    Determine the current density for each gate zone by merging two sources:

    1. `crowd_1min` continuous aggregate — live sensor pipeline data (primary).
    2. `digital_twin_zones` — authoritative live state, updated by the consumer
       AND by direct DB writes (e.g. manual ops overrides / test injections).

    For each gate we take the **maximum** density reported by either source so
    that both live sensor data and manual DB updates trigger hard-block detection.
    This ensures the test scenario (direct INSERT/UPDATE on digital_twin_zones)
    works without waiting for the Kafka consumer pipeline to materialise data.
    """
    pool = await get_pool()

    # Source 1 — crowd_1min (continuous aggregate, populated by consumer)
    agg_rows = await pool.fetch(
        """
        SELECT DISTINCT ON (zone)
            zone,
            avg_density,
            avg_flow_rate,
            max_headcount
        FROM crowd_1min
        WHERE zone LIKE 'Gate-%%'
          AND bucket > NOW() - INTERVAL '5 minutes'
        ORDER BY zone, bucket DESC
        """
    )

    throughput: dict[str, dict] = {}
    for r in agg_rows:
        throughput[r["zone"]] = {
            "avg_density":   float(r["avg_density"] or 0.0),
            "avg_flow_rate": float(r["avg_flow_rate"] or 0.0),
            "max_headcount": int(r["max_headcount"] or 0),
        }

    # Source 2 — digital_twin_zones (authoritative; includes manual overrides)
    twin_rows = await pool.fetch(
        """
        SELECT zone, density
        FROM digital_twin_zones
        WHERE zone LIKE 'Gate-%%'
        """
    )

    for r in twin_rows:
        gate = r["zone"]
        twin_density = float(r["density"] or 0.0)
        if gate in throughput:
            # Take the higher density — whichever source is more alarming wins
            if twin_density > throughput[gate]["avg_density"]:
                log.debug(
                    "Gate %s: digital_twin density (%.2f) overrides crowd_1min (%.2f)",
                    gate, twin_density, throughput[gate]["avg_density"],
                )
                throughput[gate]["avg_density"] = twin_density
        else:
            # Gate not yet in crowd_1min — use Digital Twin as sole source
            throughput[gate] = {
                "avg_density":   twin_density,
                "avg_flow_rate": 0.0,
                "max_headcount": 0,
            }

    return throughput


async def _fetch_zone_densities() -> dict[str, float]:
    """
    Return the current density for every zone from digital_twin_zones.
    Used by the exit planner to compute congestion weights.
    """
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT zone, density FROM digital_twin_zones"
    )
    return {r["zone"]: float(r["density"] or 0.0) for r in rows}


async def _resolve_alert_in_db(alert_id_str: str) -> bool:
    """
    Set resolved = TRUE for the given UUID in the alerts table.
    Returns True if exactly one row was updated.
    """
    try:
        aid = uuid.UUID(alert_id_str)
    except ValueError:
        raise HTTPException(status_code=422, detail=f"Invalid UUID: '{alert_id_str}'")

    pool = await get_pool()
    # alerts is a hypertable — primary key is (time, alert_id); we match on alert_id
    result = await pool.execute(
        """
        UPDATE alerts
        SET resolved = TRUE
        WHERE alert_id = $1
          AND resolved = FALSE
        """,
        aid,
    )
    # asyncpg returns 'UPDATE <n>' as a string
    updated = int(result.split()[-1])
    return updated > 0


# ── Exit Planner helpers ───────────────────────────────────────────────────────

def _choose_gate(
    zone: str,
    blocked_gates: set[str],
) -> tuple[str, bool]:
    """
    Return (recommended_gate, was_rerouted).

    Primary gates for the zone are tried first (ZONE_NEAREST_GATES).
    If all primaries are blocked, we fall back through ALL_GATES_ORDERED.
    """
    candidates = ZONE_NEAREST_GATES.get(zone, ALL_GATES_ORDERED[:])
    for gate in candidates:
        if gate not in blocked_gates:
            return gate, False

    # Primary gates all blocked — find the first non-blocked gate globally
    for gate in ALL_GATES_ORDERED:
        if gate not in blocked_gates:
            log.warning(
                "Exit planner: all primary gates for %s blocked; rerouting to %s",
                zone, gate,
            )
            return gate, True

    # Absolute fallback — all gates blocked (highly abnormal; pick nearest anyway)
    fallback = candidates[0] if candidates else "Gate-North"
    log.error("Exit planner: ALL gates blocked — using %s as last-resort", fallback)
    return fallback, True


def _zone_distance_hops(zone: str) -> int:
    """
    Simple BFS hop-count from seating zone to the nearest gate.
    Uses the static VENUE_EDGES graph (unweighted).
    """
    if zone in GATE_ZONES:
        return 0

    # Build unweighted adjacency list from VENUE_EDGES (bidirectional)
    adj: dict[str, set[str]] = {}
    for a, b, _ in VENUE_EDGES:
        adj.setdefault(a, set()).add(b)
        adj.setdefault(b, set()).add(a)

    visited = {zone}
    queue = [(zone, 0)]
    while queue:
        current, hops = queue.pop(0)
        if current in GATE_ZONES:
            return hops
        for neighbour in adj.get(current, []):
            if neighbour not in visited:
                visited.add(neighbour)
                queue.append((neighbour, hops + 1))

    return FALLBACK_ZONE_DISTANCE.get(zone, 3)


# ── Endpoint: GET /ops/heatmap ────────────────────────────────────────────────

@router.get(
    "/heatmap",
    response_model=HeatmapResponse,
    summary="Current thermal state of the entire venue",
)
async def ops_heatmap():
    """
    Returns a **nested JSON thermal map** of the stadium.

    Each zone entry carries:
    - Live density, headcount, flow-rate, congestion level, anomaly flag.
    - A list of concession stands within that zone with queue state and
      M/M/c wait predictions.

    Data source: `digital_twin_zones` ⋈ `digital_twin_stands`.
    Suitable as the primary data-feed for a real-time ops dashboard.
    """
    try:
        merged = await _fetch_zones_with_stands()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"DB unavailable: {exc}")

    zone_states: list[ZoneHeatState] = []
    critical_zones: list[str] = []
    total_stands = 0

    for z in merged:
        # Serialise last_updated to ISO string
        lu = z.get("last_updated")
        last_updated_str = lu.isoformat() if lu and hasattr(lu, "isoformat") else None

        # Convert density to float safely
        density = float(z["density"]) if z["density"] is not None else None

        if density is not None and density >= GATE_BLOCK_DENSITY:
            critical_zones.append(z["zone"])

        stands_out: list[StandHeatState] = []
        for s in z["stands"]:
            stands_out.append(StandHeatState(
                stand_id=s["stand_id"],
                stand_name=s["stand_name"],
                zone=s["zone"],
                queue_length=s.get("queue_length"),
                wait_minutes=round(float(s["wait_minutes"]), 2) if s.get("wait_minutes") is not None else None,
                capacity_pct=round(float(s["capacity_pct"]), 2) if s.get("capacity_pct") is not None else None,
                predicted_wait_5min=round(float(s["predicted_wait_5min"]), 2) if s.get("predicted_wait_5min") is not None else None,
                is_open=bool(s.get("is_open", True)),
                halftime_spike_expected=bool(s.get("halftime_spike_expected", False)),
            ))
        total_stands += len(stands_out)

        zone_states.append(ZoneHeatState(
            zone=z["zone"],
            density=round(density, 4) if density is not None else None,
            headcount=z.get("headcount"),
            flow_rate=round(float(z["flow_rate"]), 2) if z.get("flow_rate") is not None else None,
            congestion_level=z.get("congestion_level"),
            anomaly_active=bool(z.get("anomaly_active", False)),
            last_updated=last_updated_str,
            stands=stands_out,
        ))

    return HeatmapResponse(
        generated_at=datetime.utcnow().isoformat(),
        total_zones=len(zone_states),
        total_stands=total_stands,
        critical_zones=critical_zones,
        zones=zone_states,
    )


# ── Endpoint: GET /ops/exit-plan ──────────────────────────────────────────────

@router.get(
    "/exit-plan",
    response_model=ExitPlanResponse,
    summary="Staggered post-match exit windows with auto-rerouting",
)
async def ops_exit_plan(
    match_end_offset_minutes: float = Query(
        default=MATCH_END_OFFSET_MINUTES,
        ge=0,
        le=120,
        description=(
            "Minutes from NOW to simulate match end "
            "(0 = match just ended, for immediate planning)"
        ),
    ),
):
    """
    **Exit Flow Planner** — generates a prioritised, staggered departure schedule.

    Algorithm
    ---------
    For each seating zone *z*:
    ```
    T_departure = T_match_end + (ZoneDistance × CongestionWeight)
    ```
    - `ZoneDistance` = BFS hop-count from seating zone to nearest gate (in the
      static venue graph).
    - `CongestionWeight` = `1 + current_density` pulled from `digital_twin_zones`.
      Higher density pushes the departure window later, giving under-pressure
      zones more time before the crowd is asked to move.

    **Hard-Block Rule**: Gate zones with density ≥ 90 % (from the `crowd_1min`
    continuous aggregate) are flagged as blocked. The planner automatically
    reroutes those zones to the next nearest non-blocked gate.

    The output is sorted ascending by `departure_offset_minutes` (priority 1 first).
    Each window is a 5-minute slot: `[window_start, window_end)`.
    """
    try:
        zone_densities = await _fetch_zone_densities()
        gate_throughput = await _fetch_gate_throughput()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"DB unavailable: {exc}")

    match_end = datetime.utcnow() + timedelta(minutes=match_end_offset_minutes)

    # Determine hard-blocked gates from crowd_1min throughput data
    blocked_gates: set[str] = set()
    for gate, stats in gate_throughput.items():
        if stats["avg_density"] >= GATE_BLOCK_DENSITY:
            blocked_gates.add(gate)
            log.warning("Exit planner: gate %s is HARD BLOCKED (density=%.2f)", gate, stats["avg_density"])

    windows: list[ExitWindow] = []

    for zone in SEATING_ZONES:
        density = zone_densities.get(zone, 0.0)
        congestion_weight = 1.0 + density            # range [1.0, 2.0]
        distance_hops = _zone_distance_hops(zone)

        # Core formula: T_departure offset (in minutes)
        departure_offset = distance_hops * congestion_weight * EXIT_WINDOW_MINUTES

        gate, was_rerouted = _choose_gate(zone, blocked_gates)

        window_start = match_end + timedelta(minutes=departure_offset)
        window_end   = window_start + timedelta(minutes=EXIT_WINDOW_MINUTES)

        note = None
        if was_rerouted:
            note = (
                f"Primary gate(s) blocked (density ≥ {GATE_BLOCK_DENSITY:.0%}). "
                f"Rerouted to {gate}."
            )

        windows.append(ExitWindow(
            zone=zone,
            priority=0,                 # filled after sort
            recommended_gate=gate,
            gate_was_rerouted=was_rerouted,
            departure_offset_minutes=round(departure_offset, 2),
            window_start=window_start.isoformat(),
            window_end=window_end.isoformat(),
            zone_density=round(density, 4),
            congestion_weight=round(congestion_weight, 4),
            note=note,
        ))

    # Sort ascending by departure offset (earliest departure = priority 1)
    windows.sort(key=lambda w: w.departure_offset_minutes)
    for i, w in enumerate(windows, start=1):
        w.priority = i

    return ExitPlanResponse(
        generated_at=datetime.utcnow().isoformat(),
        match_end=match_end.isoformat(),
        total_zones=len(windows),
        hard_blocked_gates=sorted(blocked_gates),
        exit_windows=windows,
    )


# ── Endpoint: POST /ops/alerts/resolve/{alert_id} ─────────────────────────────

@router.post(
    "/alerts/resolve/{alert_id}",
    response_model=ResolveAlertResponse,
    summary="Mark a specific alert as resolved",
)
async def ops_resolve_alert(alert_id: str):
    """
    Sets `resolved = TRUE` in the **`alerts`** hypertable for the given UUID.

    - Returns **200** if the alert was found and updated.
    - Returns **404** if no unresolved alert with that UUID exists.
    - Returns **422** if the provided `alert_id` is not a valid UUID.

    The lookup is performed across all time partitions so old alerts can
    still be resolved retroactively.
    """
    try:
        updated = await _resolve_alert_in_db(alert_id)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"DB unavailable: {exc}")

    if not updated:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No unresolved alert found with id '{alert_id}'. "
                "It may already be resolved or the UUID is incorrect."
            ),
        )

    log.info("Alert %s resolved by ops staff", alert_id)
    return ResolveAlertResponse(
        alert_id=alert_id,
        resolved=True,
        message=f"Alert '{alert_id}' marked as resolved.",
    )
