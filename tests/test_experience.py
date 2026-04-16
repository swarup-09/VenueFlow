"""
VenueFlow – Experience Layer Tests (The Fan Layer)
==================================================
Phase 3 · Module D » tests/test_experience.py

Validates the three user-facing concerns at the top of the stack:

  TestMobileOrdering
    • POST /fan/{seat_id}/order  → valid ETA, correct total price, UUID order_id
    • DB persistence verified via asyncpg AND the /fan/{seat_id}/orders API
    • Edge cases: invalid stand → 404, empty items → 422

  TestWebSocketIntegrity
    • Connect to /ws/ops
    • Inject a crafted alert directly to vf_alerts via confluent_kafka
    • Assert the bridge delivers the exact payload to the WS client
      within WS_ALERT_TIMEOUT_S  (validates the < 8 s latency claim)
    • Smoke-test /ws/fan/{seat_id} ping→pong liveness check

  TestPerformanceBenchmarks
    • /intelligence/route    — p99 < 200 ms  over BENCHMARK_N = 10 sequential calls
    • /ops/heatmap           — p95 < 500 ms  over BENCHMARK_N calls
    • /ops/exit-plan         — p95 < 300 ms  over BENCHMARK_N calls
    • Concurrent burst: 10 simultaneous /intelligence/route requests, all must 200 OK
"""

import asyncio
import json
import logging
import statistics
import time
import uuid
from datetime import datetime, timezone

import asyncpg
import httpx
import pytest
import pytest_asyncio

from tests.conftest import kafka_publish

log = logging.getLogger("vf.tests.experience")

# ── Constants ──────────────────────────────────────────────────────────────────

TEST_SEAT_ID   = "D-14"           # maps to Lower-Stands via heuristic
TEST_STAND_ID  = "C4"             # Hot Dogs & Snacks, Concourse-A
WS_BASE_URL    = "ws://localhost:8000"

# Latency SLOs
WS_ALERT_TIMEOUT_S   = 3.0    # seconds (well inside the 8 s system SLO)
P99_ROUTE_LIMIT_MS   = 200    # /intelligence/route p99 SLO
P95_HEATMAP_LIMIT_MS = 500    # /ops/heatmap p95 SLO
P95_EXITPLAN_LIMIT_MS= 300    # /ops/exit-plan p95 SLO

BENCHMARK_N = 10              # number of calls for each benchmark

# Standard order body reused across ordering tests
_ORDER_BODY = {
    "stand_id": TEST_STAND_ID,
    "items": [
        {"name": "Nachos", "quantity": 2, "price": 8.50},
        {"name": "Cola",   "quantity": 1, "price": 3.25},
    ],
    "special_instructions": "Extra jalapeños",
}
_EXPECTED_TOTAL = round(2 * 8.50 + 1 * 3.25, 2)   # 20.25


# ── Percentile helper ─────────────────────────────────────────────────────────

def _pct(data: list[float], p: float) -> float:
    """Return the p-th percentile of data (0–100)."""
    s = sorted(data)
    k  = (len(s) - 1) * p / 100
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


# ══════════════════════════════════════════════════════════════════════════════
# TEST CLASS 1 — Mobile Ordering
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.experience
class TestMobileOrdering:
    """
    POST /fan/{seat_id}/order — validates the full order lifecycle:
      1. HTTP 201 with a valid OrderResponse body.
      2. delivery_eta_minutes > 0 (M/M/c queue wait + walk + overhead).
      3. Row persisted in fan_orders TimescaleDB hypertable.
      4. Row visible through GET /fan/{seat_id}/orders.
    """

    async def test_order_returns_201_with_valid_body(
        self, http_client: httpx.AsyncClient
    ):
        """
        The happy-path order must return HTTP 201 and a complete OrderResponse.
        Validates: order_id (UUID), delivery_eta_minutes > 0, status='placed'.
        """
        resp = await http_client.post(f"/fan/{TEST_SEAT_ID}/order", json=_ORDER_BODY)
        assert resp.status_code == 201, (
            f"Expected 201, got {resp.status_code}: {resp.text}"
        )
        body = resp.json()
        log.info(
            "Order placed — id=%s eta=%.1fmin seat=%s stand=%s",
            body.get("order_id"), body.get("delivery_eta_minutes"),
            body.get("seat_id"),  body.get("stand_id"),
        )

        # Required fields
        for field in ("order_id", "delivery_eta_minutes", "status",
                      "queue_wait_minutes", "walk_minutes", "total_price"):
            assert field in body, f"Missing field '{field}' in OrderResponse"

        # Semantic checks
        assert body["status"] == "placed",        "Initial order status must be 'placed'"
        assert body["delivery_eta_minutes"] > 0,  "delivery_eta_minutes must be > 0"
        assert body["queue_wait_minutes"]   >= 0, "queue_wait_minutes must be ≥ 0"
        assert body["walk_minutes"]         >= 0, "walk_minutes must be ≥ 0"
        assert body["seat_id"]  == TEST_SEAT_ID,  "seat_id mismatch"
        assert body["stand_id"] == TEST_STAND_ID, "stand_id mismatch"

        # order_id must be a well-formed UUID
        try:
            uuid.UUID(body["order_id"])
        except ValueError:
            pytest.fail(f"order_id '{body['order_id']}' is not a valid UUID v4")

    async def test_order_delivery_eta_is_additive(
        self, http_client: httpx.AsyncClient
    ):
        """
        delivery_eta_minutes must equal queue_wait_minutes + walk_minutes
        + packaging overhead (≥ 0), within floating-point tolerance.
        """
        resp = await http_client.post(f"/fan/{TEST_SEAT_ID}/order", json=_ORDER_BODY)
        assert resp.status_code == 201
        body = resp.json()

        eta  = body["delivery_eta_minutes"]
        qwm  = body["queue_wait_minutes"]
        wlk  = body["walk_minutes"]

        # eta ≥ queue_wait + walk (overhead is always ≥ 0)
        assert eta >= qwm + wlk - 0.01, (
            f"delivery_eta ({eta:.2f}) < queue_wait ({qwm:.2f}) + walk ({wlk:.2f}) — "
            "overhead component is negative, check fan_routes.py"
        )

    async def test_order_total_price_matches_items(
        self, http_client: httpx.AsyncClient
    ):
        """total_price = Σ(quantity × price) across all items."""
        resp = await http_client.post(f"/fan/{TEST_SEAT_ID}/order", json=_ORDER_BODY)
        assert resp.status_code == 201
        actual = resp.json()["total_price"]
        assert abs(actual - _EXPECTED_TOTAL) < 0.01, (
            f"total_price mismatch: expected {_EXPECTED_TOTAL}, got {actual}"
        )

    async def test_order_persisted_to_timescaledb(
        self,
        http_client: httpx.AsyncClient,
        db_pool:     asyncpg.Pool,
    ):
        """
        The order must be findable in fan_orders via:
          a) GET /fan/{seat_id}/orders  (API history)
          b) Direct asyncpg query        (DB integrity)
        """
        # Place the order
        place_resp = await http_client.post(f"/fan/{TEST_SEAT_ID}/order", json=_ORDER_BODY)
        assert place_resp.status_code == 201
        order_id = place_resp.json()["order_id"]

        # a) API history endpoint
        hist_resp = await http_client.get(f"/fan/{TEST_SEAT_ID}/orders")
        assert hist_resp.status_code == 200
        api_ids = [o["order_id"] for o in hist_resp.json()["orders"]]
        assert order_id in api_ids, (
            f"order_id '{order_id}' not found in GET /fan/{TEST_SEAT_ID}/orders"
        )
        log.info("Order '%s' confirmed via API history", order_id)

        # b) Direct DB verification
        row = await db_pool.fetchrow(
            """
            SELECT order_id, seat_id, stand_id, status, total_price
            FROM fan_orders
            WHERE order_id = $1
            """,
            uuid.UUID(order_id),
        )
        assert row is not None, (
            f"Order '{order_id}' not found in fan_orders table — "
            "insert_fan_order() may have silently failed"
        )
        assert str(row["order_id"]) == order_id
        assert row["seat_id"]       == TEST_SEAT_ID
        assert row["stand_id"]      == TEST_STAND_ID
        assert row["status"]        == "placed"
        assert abs(float(row["total_price"]) - _EXPECTED_TOTAL) < 0.01
        log.info("Order '%s' confirmed in TimescaleDB", order_id)

    async def test_order_invalid_stand_returns_404(
        self, http_client: httpx.AsyncClient
    ):
        """Ordering from a non-existent stand must be rejected with 404."""
        body = {**_ORDER_BODY, "stand_id": "ZZ99"}
        resp = await http_client.post(f"/fan/{TEST_SEAT_ID}/order", json=body)
        assert resp.status_code == 404, (
            f"Expected 404 for unknown stand ZZ99, got {resp.status_code}"
        )

    async def test_order_empty_items_returns_422(
        self, http_client: httpx.AsyncClient
    ):
        """An order with an empty items list must be rejected with 422 (validation error)."""
        body = {**_ORDER_BODY, "items": []}
        resp = await http_client.post(f"/fan/{TEST_SEAT_ID}/order", json=body)
        assert resp.status_code == 422, (
            f"Expected 422 for empty items, got {resp.status_code}"
        )

    async def test_history_endpoint_returns_placed_filter(
        self, http_client: httpx.AsyncClient
    ):
        """
        After placing one order, GET /fan/{seat_id}/orders?status=placed
        must return exactly that order.
        """
        await http_client.post(f"/fan/{TEST_SEAT_ID}/order", json=_ORDER_BODY)

        resp = await http_client.get(
            f"/fan/{TEST_SEAT_ID}/orders", params={"status": "placed"}
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] >= 1, "Expected at least 1 placed order in history"
        statuses = {o["status"] for o in data["orders"]}
        assert statuses == {"placed"}, f"Unexpected statuses with filter: {statuses}"


# ══════════════════════════════════════════════════════════════════════════════
# TEST CLASS 2 — WebSocket Integrity
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.experience
class TestWebSocketIntegrity:
    """
    Validates the Kafka → WebSocket bridge (Phase 3 · Module A).

    Strategy for WS alert test:
      1. Connect to /ws/ops FIRST (so we don't miss the message).
      2. Produce a crafted alert directly to vf_alerts via Kafka.
         The alert has a unique 'alert_id' field for unambiguous identification.
      3. Collect all WS messages for WS_ALERT_TIMEOUT_S seconds.
      4. Assert that the payload matching our alert_id was received.

    The bridge routes alerts with 'zone' present to zone:{zone} + ops,
    so /ws/ops will always receive it regardless of zone value.
    """

    async def test_kafka_alert_delivered_to_ws_ops(self, kafka_producer):
        """
        Alert injected to vf_alerts must arrive on /ws/ops
        within WS_ALERT_TIMEOUT_S ({timeout}s).

        This validates the end-to-end latency of:
          Kafka produce → bridge Consumer.poll() → broadcast_ops() → WS client
        """.format(timeout=WS_ALERT_TIMEOUT_S)
        try:
            import websockets
        except ImportError:
            pytest.skip("websockets package not installed — pip install websockets")

        unique_id  = uuid.uuid4().hex
        test_alert = {
            "severity":  "critical",
            "category":  "crowd",
            "zone":      "Gate-East",
            "message":   f"Integration test alert {unique_id}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            # 'alert_id' is a custom field we inject so we can match our message
            "alert_id":  unique_id,
        }

        ws_uri = f"ws://localhost:8000/ws/ops"
        received_messages: list[dict] = []
        matched = False

        try:
            async with websockets.connect(ws_uri, open_timeout=5) as ws:
                log.info(
                    "WS /ws/ops connected — injecting alert (alert_id=%s) into vf_alerts",
                    unique_id,
                )

                # Produce AFTER connecting so the bridge's next poll will catch it
                kafka_publish(kafka_producer, "vf_alerts", unique_id, test_alert)

                # Drain WS messages until we match or time out
                deadline = time.monotonic() + WS_ALERT_TIMEOUT_S
                while time.monotonic() < deadline:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                        msg = json.loads(raw)
                        received_messages.append(msg)
                        log.debug("WS recv: _type=%s", msg.get("_type"))

                        # Match by our injected alert_id field
                        if msg.get("alert_id") == unique_id:
                            elapsed = WS_ALERT_TIMEOUT_S - remaining
                            log.info(
                                "✅ Alert '%s' received on /ws/ops in %.2fs "
                                "(SLO = %.1fs)",
                                unique_id, elapsed, WS_ALERT_TIMEOUT_S,
                            )
                            matched = True
                            break

                    except asyncio.TimeoutError:
                        break  # outer deadline handles the failure

        except Exception as exc:
            pytest.fail(f"WebSocket connection to /ws/ops failed: {exc}")

        assert matched, (
            f"Alert '{unique_id}' was NOT received on /ws/ops within "
            f"{WS_ALERT_TIMEOUT_S}s.\n"
            f"Messages received: {received_messages}\n"
            "Check that the Kafka→WS bridge is running and the bridge group "
            f"subscribes to 'vf_alerts'."
        )

    async def test_ws_ops_responds_to_ping(self):
        """Smoke test: /ws/ops must accept a connection and echo 'pong'."""
        try:
            import websockets
        except ImportError:
            pytest.skip("websockets not installed")

        try:
            async with websockets.connect("ws://localhost:8000/ws/ops", open_timeout=5) as ws:
                await ws.send("ping")
                pong = await asyncio.wait_for(ws.recv(), timeout=3.0)
                assert pong == "pong", f"Expected 'pong', got '{pong}'"
                log.info("✅ /ws/ops ping→pong confirmed")
        except Exception as exc:
            pytest.fail(f"/ws/ops WebSocket failed: {exc}")

    async def test_ws_fan_channel_responds_to_ping(self):
        """Smoke test: /ws/fan/{seat_id} must accept a connection and echo 'pong'."""
        try:
            import websockets
        except ImportError:
            pytest.skip("websockets not installed")

        try:
            async with websockets.connect(
                f"ws://localhost:8000/ws/fan/{TEST_SEAT_ID}", open_timeout=5
            ) as ws:
                await ws.send("ping")
                pong = await asyncio.wait_for(ws.recv(), timeout=3.0)
                assert pong == "pong", f"Expected 'pong', got '{pong}'"
                log.info("✅ /ws/fan/%s ping→pong confirmed", TEST_SEAT_ID)
        except Exception as exc:
            pytest.fail(f"/ws/fan/{TEST_SEAT_ID} WebSocket failed: {exc}")

    async def test_ws_zone_channel_responds_to_ping(self):
        """Smoke test: /ws/zone/{zone} must accept a connection and echo 'pong'."""
        try:
            import websockets
        except ImportError:
            pytest.skip("websockets not installed")

        try:
            async with websockets.connect(
                "ws://localhost:8000/ws/zone/Concourse-A", open_timeout=5
            ) as ws:
                await ws.send("ping")
                pong = await asyncio.wait_for(ws.recv(), timeout=3.0)
                assert pong == "pong"
                log.info("✅ /ws/zone/Concourse-A ping→pong confirmed")
        except Exception as exc:
            pytest.fail(f"/ws/zone/Concourse-A WebSocket failed: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
# TEST CLASS 3 — Performance Benchmarks
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.experience
class TestPerformanceBenchmarks:
    """
    Latency SLO benchmarks.

    All benchmarks run BENCHMARK_N sequential calls and compute percentiles.
    The first call is always included (no warm-up discards) to surface
    cold-path overhead that real users would experience.

    Endpoints under test:
      /intelligence/route   → Dijkstra on pure Python, no DB — p99 < 200 ms
      /ops/heatmap          → DB JOIN (digital_twin_zones ⋈ stands) — p95 < 500 ms
      /ops/exit-plan        → 2 DB queries + BFS — p95 < 300 ms

    Concurrent burst test:
      10 simultaneous /intelligence/route calls — all must succeed (no race conditions
      in the thread-safe FlowPredictor ETS state).
    """

    # ── Sequential SLO benchmarks ─────────────────────────────────────────────

    async def test_intelligence_route_p99_under_200ms(
        self, http_client: httpx.AsyncClient
    ):
        """
        /intelligence/route p99 must be < 200 ms.
        Pure-Python Dijkstra should complete in < 5 ms; the SLO budget
        accounts for HTTP overhead, asyncio scheduling, and httpx serialisation.
        """
        params      = {"from_zone": "Parking-P1", "to_zone": "Lower-Stands"}
        latencies   = []

        for i in range(BENCHMARK_N):
            t0 = time.perf_counter()
            resp = await http_client.get("/intelligence/route", params=params)
            ms   = (time.perf_counter() - t0) * 1000
            latencies.append(ms)
            assert resp.status_code == 200, (
                f"Route call {i+1}/{BENCHMARK_N} failed: {resp.status_code}"
            )
            log.debug("Route call %d/%d: %.1f ms", i + 1, BENCHMARK_N, ms)

        p50 = statistics.median(latencies)
        p99 = _pct(latencies, 99)
        avg = statistics.mean(latencies)
        log.info(
            "/intelligence/route — avg=%.1f ms  p50=%.1f ms  p99=%.1f ms  (SLO: p99<200ms)",
            avg, p50, p99,
        )

        assert p99 <= P99_ROUTE_LIMIT_MS, (
            f"/intelligence/route p99 {p99:.1f} ms exceeds {P99_ROUTE_LIMIT_MS} ms SLO.\n"
            f"All timings (ms): {[round(l, 1) for l in latencies]}"
        )

    async def test_ops_heatmap_p95_under_500ms(
        self, http_client: httpx.AsyncClient
    ):
        """
        /ops/heatmap (DB JOIN) p95 must be < 500 ms.
        Includes asyncpg query, Python merge, and Pydantic serialisation.
        """
        latencies = []

        for i in range(BENCHMARK_N):
            t0   = time.perf_counter()
            resp = await http_client.get("/ops/heatmap")
            ms   = (time.perf_counter() - t0) * 1000
            latencies.append(ms)
            assert resp.status_code == 200, f"Heatmap call {i+1} failed"

        p95 = _pct(latencies, 95)
        log.info(
            "/ops/heatmap — p95=%.1f ms  max=%.1f ms  (SLO: p95<500ms)",
            p95, max(latencies),
        )
        assert p95 <= P95_HEATMAP_LIMIT_MS, (
            f"/ops/heatmap p95 {p95:.1f} ms exceeds {P95_HEATMAP_LIMIT_MS} ms SLO."
        )

    async def test_ops_exit_plan_p95_under_300ms(
        self, http_client: httpx.AsyncClient
    ):
        """
        /ops/exit-plan (2 DB queries + BFS) p95 must be < 300 ms.
        """
        latencies = []

        for i in range(BENCHMARK_N):
            t0   = time.perf_counter()
            resp = await http_client.get("/ops/exit-plan?match_end_offset_minutes=0")
            ms   = (time.perf_counter() - t0) * 1000
            latencies.append(ms)
            assert resp.status_code == 200, f"Exit-plan call {i+1} failed"

        p95 = _pct(latencies, 95)
        log.info(
            "/ops/exit-plan — p95=%.1f ms  max=%.1f ms  (SLO: p95<300ms)",
            p95, max(latencies),
        )
        assert p95 <= P95_EXITPLAN_LIMIT_MS, (
            f"/ops/exit-plan p95 {p95:.1f} ms exceeds {P95_EXITPLAN_LIMIT_MS} ms SLO."
        )

    # ── Concurrent burst — race condition & thread-safety check ───────────────

    async def test_concurrent_route_requests_all_succeed(
        self, http_client: httpx.AsyncClient
    ):
        """
        Fire BENCHMARK_N /intelligence/route requests concurrently
        (asyncio.gather — all in flight simultaneously).

        All must return 200 OK, verifying that the stateful FlowPredictor
        ETS smoothing dict has no concurrency issues under async load.
        """
        params = {"from_zone": "Gate-East", "to_zone": "VIP-Lounge"}

        async def _call(i: int) -> tuple[int, float]:
            t0 = time.perf_counter()
            r  = await http_client.get("/intelligence/route", params=params)
            return r.status_code, (time.perf_counter() - t0) * 1000

        results   = await asyncio.gather(*[_call(i) for i in range(BENCHMARK_N)])
        statuses  = [r[0] for r in results]
        latencies = [r[1] for r in results]

        failed = [i for i, s in enumerate(statuses) if s != 200]
        log.info(
            "Concurrent burst (%d): statuses=%s  max=%.1f ms",
            BENCHMARK_N, statuses, max(latencies),
        )
        assert not failed, (
            f"Concurrent route calls {failed} returned non-200: "
            f"{[statuses[i] for i in failed]}"
        )

    # ── Smoke: health endpoint latency ────────────────────────────────────────

    async def test_health_endpoint_under_200ms(
        self, http_client: httpx.AsyncClient
    ):
        """
        /health (DB ping + WS stats) must respond within 200 ms.
        Exceeding this threshold would indicate connection pool exhaustion
        or an oversized health check query.
        """
        t0   = time.perf_counter()
        resp = await http_client.get("/health")
        ms   = (time.perf_counter() - t0) * 1000

        assert resp.status_code == 200
        log.info("/health: %.1f ms", ms)
        assert ms <= 200, (
            f"/health took {ms:.1f} ms (SLO 200 ms). "
            "Check the unresolved_critical_alerts COUNT query performance."
        )
