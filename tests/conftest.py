"""
VenueFlow – Test Fixtures (conftest.py)
=======================================
Shared fixtures for all Phase 3 · Module D tests.

Fixtures
--------
  http_client      AsyncClient → live FastAPI server (session-scoped)
  db_pool          asyncpg Pool → TimescaleDB              (session-scoped)
  kafka_producer   confluent_kafka.Producer                (session-scoped)
  clean_tables     DELETE fan_orders + alerts before each test (autouse)

Environment overrides
---------------------
  VENUEFLOW_API_URL          default: http://localhost:8000
  DATABASE_URL               default: postgresql://vf_user:vf_secret@localhost:5432/venueflow
  KAFKA_BOOTSTRAP_SERVERS    default: localhost:9092
"""

import json
import logging
import os
from typing import AsyncGenerator, Optional

import asyncpg
import httpx
import pytest
import pytest_asyncio

log = logging.getLogger("vf.tests")

# ── Service coordinates ────────────────────────────────────────────────────────

API_BASE_URL  = os.getenv("VENUEFLOW_API_URL",         "http://localhost:8000")
DATABASE_URL  = os.getenv("DATABASE_URL",              "postgresql://vf_user:vf_secret@localhost:5432/venueflow")
KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS",   "localhost:9092")


# ── Session-scoped fixtures ────────────────────────────────────────────────────

@pytest_asyncio.fixture(scope="session")
async def http_client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """
    httpx.AsyncClient pointed at the live FastAPI server.

    Performs a /health smoke-check before yielding; skips the entire session
    with a clear message if the server is not reachable (avoids a waterfall of
    confusing failures unrelated to the test logic itself).
    """
    async with httpx.AsyncClient(
        base_url=API_BASE_URL,
        timeout=httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0),
    ) as client:
        try:
            resp = await client.get("/health")
            health = resp.json()
            log.info(
                "API health OK — db=%s kafka=%s version=%s",
                health.get("timescaledb_connected"),
                health.get("kafka_producer_ready"),
                health.get("version"),
            )
        except Exception as exc:
            pytest.skip(
                f"VenueFlow API not reachable at {API_BASE_URL}: {exc}\n"
                "Ensure 'docker compose up' is running before executing the test suite."
            )
        yield client


@pytest_asyncio.fixture(scope="session")
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    """
    asyncpg connection pool to TimescaleDB.

    Skips the entire session if the database is not reachable so tests
    fail with a clear 'DB unavailable' message rather than cryptic errors.
    """
    try:
        pool = await asyncpg.create_pool(
            dsn=DATABASE_URL,
            min_size=2,
            max_size=8,
            command_timeout=10,
        )
    except Exception as exc:
        pytest.skip(
            f"TimescaleDB not reachable at {DATABASE_URL}: {exc}\n"
            "Ensure the vf-timescaledb container is healthy."
        )
        return
    log.info("Test DB pool ready (%s)", DATABASE_URL)
    yield pool
    await pool.close()
    log.info("Test DB pool closed")


@pytest_asyncio.fixture(scope="session")
def kafka_producer():
    """
    confluent_kafka.Producer for direct test message injection.

    linger.ms=0 ensures zero batching delay — messages are sent immediately,
    which is critical for deterministic WS latency measurements.

    Skips the session if Kafka is not reachable.
    """
    try:
        from confluent_kafka import Producer, KafkaException
        p = Producer({
            "bootstrap.servers": KAFKA_BROKERS,
            "linger.ms": 0,
            "delivery.timeout.ms": 5000,
        })
        # Trigger a metadata fetch to verify connectivity
        p.list_topics(timeout=5)
        log.info("Kafka producer connected (%s)", KAFKA_BROKERS)
        return p
    except Exception as exc:
        pytest.skip(
            f"Kafka not reachable at {KAFKA_BROKERS}: {exc}\n"
            "Ensure the vf-kafka container is healthy."
        )


# ── Per-test state reset ───────────────────────────────────────────────────────

@pytest_asyncio.fixture(autouse=True)
async def clean_tables(db_pool: asyncpg.Pool) -> AsyncGenerator[None, None]:
    """
    Wipe fan_orders and alerts before every test to guarantee a clean slate.

    autouse=True means this runs for every test function automatically —
    individual tests never need to call it explicitly.

    Uses DELETE (not TRUNCATE) for compatibility with TimescaleDB hypertable
    chunk boundaries and continuous aggregate refresh policies.
    """
    try:
        await db_pool.execute("DELETE FROM fan_orders")
        await db_pool.execute("DELETE FROM alerts")
        log.debug("Tables cleaned: fan_orders, alerts")
    except Exception as exc:
        log.warning("Table cleanup failed (non-fatal): %s", exc)
    yield
    # Post-test teardown intentionally empty — the next test's setup re-cleans.


# ── Shared helper: Kafka publish ───────────────────────────────────────────────

def kafka_publish(producer, topic: str, key: str, payload: dict) -> None:
    """
    Produce one JSON message to Kafka and flush synchronously.

    Raises pytest.fail() on KafkaException so the calling test fails with
    a descriptive message rather than a bare exception traceback.
    """
    from confluent_kafka import KafkaException

    def _delivery_report(err, msg):
        if err:
            log.error("Kafka delivery failed [%s/%s]: %s", topic, key, err)
        else:
            log.debug(
                "Kafka delivered → %s [partition=%d offset=%d]",
                msg.topic(), msg.partition(), msg.offset(),
            )

    try:
        producer.produce(
            topic,
            key=key.encode("utf-8"),
            value=json.dumps(payload, default=str).encode("utf-8"),
            callback=_delivery_report,
        )
        remaining = producer.flush(timeout=5)
        if remaining:
            pytest.fail(f"Kafka flush: {remaining} messages still in queue for topic={topic}")
    except KafkaException as exc:
        pytest.fail(f"Kafka produce to '{topic}' failed: {exc}")
