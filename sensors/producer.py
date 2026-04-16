"""
VenueFlow – Kafka Async Producer
Singleton wrapper around confluent-kafka's Producer, wired into the
FastAPI lifespan so it starts clean and flushes fully on shutdown.

Optimisation profile
--------------------
* linger.ms  = 5      – batch messages for 5 ms instead of sending one-by-one
* batch.size = 65536  – 64 KB batches (default 16 KB) for throughput spikes
* compression.type = lz4  – fast CPU-light compression for JSON payloads
* acks = 1            – leader ack only keeps end-to-end latency < 100 ms
* queue.buffering.max.ms = 5  – mirrors linger for the internal C queue
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from confluent_kafka import Producer, KafkaException

logger = logging.getLogger("venueflow.kafka")

# ── Topic constants ────────────────────────────────────────────────────────────
TOPIC_CROWD_RAW: str = "vf_crowd_raw"
TOPIC_QUEUES_RAW: str = "vf_queues_raw"


# ── Low-latency / high-throughput producer config ─────────────────────────────
_PRODUCER_CONFIG: dict[str, Any] = {
    "bootstrap.servers": "localhost:9092",
    # Reliability
    "acks": "1",                          # leader ack; avoids full ISR round-trip
    # Batching & latency
    "linger.ms": 5,                       # max 5 ms wait to fill a batch
    "batch.size": 65536,                  # 64 KB — handles halftime spikes
    "queue.buffering.max.ms": 5,          # C-lib internal queue matches linger
    "queue.buffering.max.messages": 100000,
    # Compression – lz4 is fastest for JSON strings
    "compression.type": "lz4",
    # Retries (transient broker restarts)
    "retries": 5,
    "retry.backoff.ms": 200,
    # Delivery report callback logging level
    "log_level": 3,
}


def _delivery_report(err: Any, msg: Any) -> None:
    """Called by confluent-kafka's poll() for every produced message."""
    if err is not None:
        logger.error(
            "Kafka delivery FAILED | topic=%s partition=%s offset=%s | %s",
            msg.topic(), msg.partition(), msg.offset(), err,
        )
    else:
        logger.debug(
            "Delivered | topic=%s partition=%s offset=%s",
            msg.topic(), msg.partition(), msg.offset(),
        )


class VenueFlowProducer:
    """
    Async-friendly singleton Kafka producer.

    Usage
    -----
    # In lifespan:
    await producer.start()
    ...
    await producer.stop()

    # Anywhere:
    await producer.publish(TOPIC_CROWD_RAW, payload_dict)
    """

    _instance: "VenueFlowProducer | None" = None

    def __new__(cls) -> "VenueFlowProducer":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._producer: Producer | None = None
            cls._instance._loop: asyncio.AbstractEventLoop | None = None
            cls._instance._poll_task: asyncio.Task | None = None
        return cls._instance

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Initialise the underlying C producer and start the poll loop."""
        if self._producer is not None:
            logger.warning("VenueFlowProducer already started – ignoring duplicate start()")
            return

        logger.info("Initialising Kafka producer → %s", _PRODUCER_CONFIG["bootstrap.servers"])
        self._producer = Producer(_PRODUCER_CONFIG)
        self._loop = asyncio.get_running_loop()

        # Background poll task: drains the delivery-report queue without blocking
        self._poll_task = asyncio.create_task(
            self._poll_loop(), name="kafka-producer-poll"
        )
        logger.info(
            "✅ Kafka producer ready | topics=[%s, %s]",
            TOPIC_CROWD_RAW, TOPIC_QUEUES_RAW,
        )

    async def stop(self) -> None:
        """Flush all buffered messages, then close the producer cleanly."""
        if self._producer is None:
            return

        logger.info("Flushing Kafka producer (up to 10 s)…")
        # confluent-kafka flush() is blocking — offload to thread pool
        loop = asyncio.get_running_loop()
        remaining = await loop.run_in_executor(None, self._producer.flush, 10)
        if remaining > 0:
            logger.warning("%d message(s) were NOT delivered before shutdown", remaining)
        else:
            logger.info("Kafka producer flushed cleanly.")

        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass

        self._producer = None
        logger.info("Kafka producer closed.")

    # ── Core publish ──────────────────────────────────────────────────────────

    async def publish(self, topic: str, payload: dict) -> None:
        """
        Serialise `payload` to UTF-8 JSON and enqueue it non-blockingly.

        This is fire-and-forget from the caller's perspective; the delivery
        report callback handles logging successes/failures.
        """
        if self._producer is None:
            logger.error("publish() called before start() — message dropped for topic=%s", topic)
            return

        try:
            value: bytes = json.dumps(payload, default=str).encode("utf-8")
            # produce() is non-blocking (enqueues to internal C queue)
            self._producer.produce(
                topic=topic,
                value=value,
                on_delivery=_delivery_report,
            )
        except KafkaException as exc:
            logger.error("KafkaException while producing to %s: %s", topic, exc)
        except BufferError:
            # Internal queue full — trigger a poll to drain delivery reports
            logger.warning("Producer queue full — polling to recover (topic=%s)", topic)
            self._producer.poll(0)

    # ── Internal poll loop ────────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        """
        Periodically call Producer.poll() so that delivery callbacks
        are fired and the internal queue is kept healthy.
        Runs every 50 ms in the asyncio event loop's executor.
        """
        loop = asyncio.get_running_loop()
        while True:
            try:
                await loop.run_in_executor(None, self._producer.poll, 0)
                await asyncio.sleep(0.05)   # 50 ms cadence
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001
                logger.error("Unexpected error in Kafka poll loop: %s", exc)
                await asyncio.sleep(1)


# ── Module-level singleton ─────────────────────────────────────────────────────
kafka_producer = VenueFlowProducer()
