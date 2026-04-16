"""
VenueFlow – Pipeline Tests (The 'Golden Path')
==============================================
Phase 3 · Module D » tests/test_pipeline.py

Validates the full data journey across the first two system layers:

  Layer 1 — Sensing
    TestEndToEndSensorFlow
      • Calls  GET /sensors/snapshot  (publishes all readings to Kafka)
      • Polls  GET /twin/zones/{zone} until the Consumer materialises the twin
      • Separately injects a raw message directly to vf_crowd_raw via Kafka
        and verifies the Consumer persists it to crowd_readings within the SLO.

  Layer 2 — Intelligence (Ops)
    TestIntelligenceExitPlanner
      • Upserts Gate-North density = 0.95 directly into digital_twin_zones
      • Calls GET /ops/exit-plan
      • Asserts: Gate-North in hard_blocked_gates
      • Asserts: VIP-Lounge rerouted away from Gate-North (gate_was_rerouted=True)
      • Asserts: Concourse-C (South/West gates) is unaffected
      • Asserts: exit_windows list is sorted by departure_offset_minutes

Latency budget
--------------
  Sensor → Kafka → Consumer → DB : ≤ 8 s  (SENSOR_TO_DB_SLO_S)
  Each poll interval                : 0.5 s (POLL_INTERVAL_S)
"""

import asyncio
import logging
import time
import uuid
from datetime import datetime, timezone

import asyncpg
import httpx
import pytest
import pytest_asyncio

from tests.conftest import kafka_publish

log = logging.getLogger("vf.tests.pipeline")

# ── Timing constants ───────────────────────────────────────────────────────────

# How long to wait for the Consumer to process a Kafka message and update the DB.
# The 8-second SLO covers: Kafka produce → Kafka network → Consumer poll (≤0.5s)
# → asyncpg INSERT → digital twin UPSERT.
SENSOR_TO_DB_SLO_S = 8.0

# Granularity of the polling loop while waiting for the Consumer.
POLL_INTERVAL_S = 0.5

# Test blockage constants
BLOCKED_GATE     = "Gate-North"
BLOCK_DENSITY    = 0.95
MATCH_OFFSET_MIN = 15


# ── Helper: poll the Digital Twin until a zone appears ────────────────────────

async def _await_twin_zone(
    client: httpx.AsyncClient,
    zone: str,
    timeout: float = SENSOR_TO_DB_SLO_S,
) -> dict | None:
    """
    Repeatedly GET /twin/zones/{zone} until 200 OK or timeout.

    Returns the parsed JSON body on success, None on timeout.
    This pattern (poll-with-deadline) is more robust than a fixed asyncio.sleep
    because fast CI machines may resolve in well under the SLO.
    """
    deadline = time.monotonic() + timeout
    attempt  = 0
    while time.monotonic() < deadline:
        attempt += 1
        resp = await client.get(f"/twin/zones/{zone}")
        if resp.status_code == 200:
            log.info(
                "Twin zone '%s' confirmed after attempt %d (%.1f s elapsed)",
                zone, attempt, timeout - (deadline - time.monotonic()),
            )
            return resp.json()
        log.debug("Poll %d: twin/zones/%s → %d", attempt, zone, resp.status_code)
        await asyncio.sleep(POLL_INTERVAL_S)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# TEST CLASS 1 — End-to-End Sensor Flow
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.pipeline
class TestEndToEndSensorFlow:
    """
    Validates the full Sensing → Kafka → Consumer → Digital Twin pipeline.

    Two complementary strategies:
      A. API-triggered: hit /sensors/snapshot → Consumer processes Kafka messages
      B. Direct-inject: bypass the API and produce a raw payload to vf_crowd_raw

    Both paths must land in the Digital Twin within SENSOR_TO_DB_SLO_S.
    """

    # ── Strategy A: API-triggered flow ────────────────────────────────────────

    async def test_snapshot_triggers_pipeline_and_updates_twin(
        self,
        http_client: httpx.AsyncClient,
    ):
        """
        Calling GET /sensors/snapshot must result in at least one zone
        appearing (or updating) in the Digital Twin within the 8-second SLO.

        This exercises the complete critical path:
          /sensors/snapshot
            ↓ confluent_kafka Producer (inside API)
          Kafka vf_crowd_raw
            ↓ Consumer.poll() → _handle_crowd()
          asyncpg INSERT INTO crowd_readings
          asyncpg UPSERT INTO digital_twin_zones
            ↓
          GET /twin/zones/{zone} → 200 OK ✓
        """
        # Step 1 – trigger the snapshot (also publishes all readings to Kafka)
        snap_resp = await http_client.get("/sensors/snapshot")
        assert snap_resp.status_code == 200, (
            f"GET /sensors/snapshot failed: {snap_resp.status_code} — {snap_resp.text}"
        )
        snapshot   = snap_resp.json()
        crowd_list = snapshot.get("crowd_sensors", [])
        assert len(crowd_list) > 0, "snapshot.crowd_sensors is empty — check orchestrator"

        first_zone = crowd_list[0]["zone"]
        log.info("Snapshot published %d crowd readings; tracking zone '%s'", len(crowd_list), first_zone)

        # Step 2 – poll the Digital Twin
        zone_data = await _await_twin_zone(http_client, first_zone)
        assert zone_data is not None, (
            f"Zone '{first_zone}' did NOT appear in the Digital Twin within "
            f"{SENSOR_TO_DB_SLO_S}s.\n"
            "Check that the vf-consumer container is running and consuming vf_crowd_raw."
        )

        # Step 3 – structural assertions on the twin entry
        assert "density"          in zone_data, "Twin entry missing 'density'"
        assert "congestion_level" in zone_data, "Twin entry missing 'congestion_level'"
        assert 0.0 <= float(zone_data["density"]) <= 1.0, (
            f"Density {zone_data['density']} out of [0,1] range"
        )
        log.info(
            "✅ Pipeline test A passed — zone='%s' density=%.3f congestion='%s'",
            first_zone, zone_data["density"], zone_data.get("congestion_level"),
        )

    # ── Strategy B: Direct Kafka injection ────────────────────────────────────

    async def test_kafka_inject_persists_to_crowd_readings(
        self,
        http_client: httpx.AsyncClient,
        db_pool: asyncpg.Pool,
        kafka_producer,
    ):
        """
        Bypass the API entirely: produce a crafted raw sensor payload directly
        to vf_crowd_raw.  The Consumer is expected to:
          1. Consume the message within its poll interval (≤0.5 s).
          2. INSERT the row into crowd_readings.
          3. UPSERT the zone into digital_twin_zones.

        We use a unique sensor_id so the DB query unambiguously identifies
        our injected row, even if other sensors are producing concurrently.
        """
        unique_sensor_id = f"test-inject-{uuid.uuid4().hex[:8]}"
        test_zone        = "Concourse-B"
        test_density     = 0.55

        payload = {
            "timestamp":  datetime.now(timezone.utc).isoformat(),
            "sensor_id":  unique_sensor_id,
            "zone":       test_zone,
            "density":    test_density,
            "headcount":  275,
            "flow_rate":  2.1,
            "direction":  "inbound",
        }

        log.info("Injecting sensor payload → vf_crowd_raw (sensor_id=%s)", unique_sensor_id)
        kafka_publish(kafka_producer, "vf_crowd_raw", test_zone, payload)

        # Poll crowd_readings for our unique sensor_id
        deadline  = time.monotonic() + SENSOR_TO_DB_SLO_S
        persisted = False
        while time.monotonic() < deadline:
            row = await db_pool.fetchrow(
                """
                SELECT sensor_id, zone, density
                FROM crowd_readings
                WHERE sensor_id = $1
                ORDER BY time DESC
                LIMIT 1
                """,
                unique_sensor_id,
            )
            if row:
                persisted = True
                log.info(
                    "✅ Consumer persisted row → sensor_id=%s zone=%s density=%.2f",
                    row["sensor_id"], row["zone"], row["density"],
                )
                assert row["zone"]    == test_zone
                assert abs(float(row["density"]) - test_density) < 0.001
                break
            await asyncio.sleep(POLL_INTERVAL_S)

        assert persisted, (
            f"Injected sensor '{unique_sensor_id}' was NOT found in crowd_readings "
            f"within {SENSOR_TO_DB_SLO_S}s.\n"
            "Verify the vf-consumer container is running and consuming vf_crowd_raw."
        )

    async def test_snapshot_contains_all_sensor_types(
        self, http_client: httpx.AsyncClient
    ):
        """
        Smoke-level structural assertion: /sensors/snapshot must return
        non-empty lists for all four sensor types every time.
        """
        resp = await http_client.get("/sensors/snapshot")
        assert resp.status_code == 200
        snap = resp.json()

        for field in ("crowd_sensors", "queue_sensors", "ble_sensors", "cctv_sensors"):
            assert field in snap,         f"Missing field '{field}' in snapshot"
            assert len(snap[field]) > 0,  f"'{field}' list is empty in snapshot"

        log.info(
            "Snapshot OK — crowd=%d queue=%d ble=%d cctv=%d",
            len(snap["crowd_sensors"]),
            len(snap["queue_sensors"]),
            len(snap["ble_sensors"]),
            len(snap["cctv_sensors"]),
        )


# ══════════════════════════════════════════════════════════════════════════════
# TEST CLASS 2 — Intelligence Layer: Exit Planner
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.pipeline
class TestIntelligenceExitPlanner:
    """
    Validates the Exit Flow Planner's hard-block detection and rerouting.

    Setup: Each test in this class runs after inject_gate_blockage, which
    sets Gate-North to density=0.95 in digital_twin_zones.  The fixture
    also cleans up to density=0.0 after the test so other tests see neutral state.

    The planner reads density from digital_twin_zones (patched in Phase C fix),
    so no Kafka pipeline wait is needed here — the upsert is synchronous.
    """

    @pytest_asyncio.fixture(autouse=True)
    async def inject_gate_blockage(self, db_pool: asyncpg.Pool):
        """Upsert Gate-North = 0.95 before each test; reset to 0.0 after."""
        await db_pool.execute(
            """
            INSERT INTO digital_twin_zones
                (zone, last_updated, density, congestion_level)
            VALUES ($1, NOW(), $2, 'critical')
            ON CONFLICT (zone) DO UPDATE SET
                density          = EXCLUDED.density,
                congestion_level = EXCLUDED.congestion_level,
                last_updated     = NOW()
            """,
            BLOCKED_GATE, BLOCK_DENSITY,
        )
        log.info("Fixture: %s set to density=%.2f", BLOCKED_GATE, BLOCK_DENSITY)
        yield
        await db_pool.execute(
            """
            UPDATE digital_twin_zones
            SET density = 0.0, congestion_level = 'low', last_updated = NOW()
            WHERE zone = $1
            """,
            BLOCKED_GATE,
        )
        log.debug("Fixture teardown: %s reset to density=0.0", BLOCKED_GATE)

    # ── Assertions ─────────────────────────────────────────────────────────────

    async def test_blocked_gate_in_hard_blocked_list(
        self, http_client: httpx.AsyncClient
    ):
        """
        Gate-North at density 0.95 must appear in hard_blocked_gates.
        This validates the dual-source (crowd_1min + digital_twin_zones)
        density check implemented in Phase C.
        """
        resp = await http_client.get(
            f"/ops/exit-plan?match_end_offset_minutes={MATCH_OFFSET_MIN}"
        )
        assert resp.status_code == 200, f"exit-plan failed: {resp.text}"
        body = resp.json()

        log.info("hard_blocked_gates: %s", body["hard_blocked_gates"])
        assert BLOCKED_GATE in body["hard_blocked_gates"], (
            f"Expected '{BLOCKED_GATE}' in hard_blocked_gates; "
            f"got: {body['hard_blocked_gates']}"
        )

    async def test_vip_lounge_rerouted_away_from_gate_north(
        self, http_client: httpx.AsyncClient
    ):
        """
        VIP-Lounge's primary gate is Gate-North.
        With Gate-North blocked, the planner must reroute it
        (gate_was_rerouted=True, recommended_gate != 'Gate-North').
        """
        resp = await http_client.get(
            f"/ops/exit-plan?match_end_offset_minutes={MATCH_OFFSET_MIN}"
        )
        assert resp.status_code == 200

        windows    = resp.json()["exit_windows"]
        vip_window = next((w for w in windows if w["zone"] == "VIP-Lounge"), None)

        assert vip_window is not None, "VIP-Lounge not found in exit_windows"
        log.info(
            "VIP-Lounge → gate=%s rerouted=%s note=%s",
            vip_window["recommended_gate"],
            vip_window["gate_was_rerouted"],
            vip_window.get("note"),
        )
        assert vip_window["gate_was_rerouted"] is True, (
            "VIP-Lounge must have gate_was_rerouted=True when Gate-North is blocked"
        )
        assert vip_window["recommended_gate"] != BLOCKED_GATE, (
            f"Rerouted VIP-Lounge must NOT still point at blocked '{BLOCKED_GATE}'"
        )

    async def test_concourse_C_unaffected_by_gate_north_block(
        self, http_client: httpx.AsyncClient
    ):
        """
        Concourse-C primary gates are Gate-South and Gate-West.
        Neither is blocked, so gate_was_rerouted must be False and the
        recommended gate must be South or West.
        """
        resp = await http_client.get(
            f"/ops/exit-plan?match_end_offset_minutes={MATCH_OFFSET_MIN}"
        )
        assert resp.status_code == 200

        windows   = resp.json()["exit_windows"]
        cc_window = next((w for w in windows if w["zone"] == "Concourse-C"), None)

        assert cc_window is not None, "Concourse-C not found in exit_windows"
        assert cc_window["gate_was_rerouted"] is False, (
            "Concourse-C should NOT be rerouted — its gates are clear"
        )
        assert cc_window["recommended_gate"] in ("Gate-South", "Gate-West"), (
            f"Concourse-C should use Gate-South or Gate-West, "
            f"got: {cc_window['recommended_gate']}"
        )

    async def test_exit_windows_sorted_ascending_by_offset(
        self, http_client: httpx.AsyncClient
    ):
        """
        The exit_windows list must be sorted ascending by departure_offset_minutes
        and the first window must have priority == 1.
        """
        resp = await http_client.get(
            "/ops/exit-plan?match_end_offset_minutes=0"
        )
        assert resp.status_code == 200

        windows = resp.json()["exit_windows"]
        offsets  = [w["departure_offset_minutes"] for w in windows]
        assert offsets == sorted(offsets), (
            f"exit_windows not sorted by departure_offset_minutes: {offsets}"
        )
        assert windows[0]["priority"] == 1, (
            f"First window priority must be 1, got {windows[0]['priority']}"
        )
        log.info("Exit window order: %s", [(w["zone"], w["priority"]) for w in windows])

    async def test_heatmap_marks_critical_zone(
        self, http_client: httpx.AsyncClient
    ):
        """
        GET /ops/heatmap must list Gate-North in critical_zones
        when its density is ≥ 0.90.
        """
        resp = await http_client.get("/ops/heatmap")
        assert resp.status_code == 200

        body = resp.json()
        assert BLOCKED_GATE in body["critical_zones"], (
            f"Expected '{BLOCKED_GATE}' in critical_zones; "
            f"got: {body['critical_zones']}"
        )
        log.info("Heatmap critical_zones: %s", body["critical_zones"])
