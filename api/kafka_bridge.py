"""
VenueFlow – Kafka → WebSocket Bridge (Phase 3 · Module A)
=========================================================
Background service that consumes `vf_intelligence_out` and `vf_alerts`,
validates each message against the Pydantic schemas, then routes it to
the correct WebSocket room via the ConnectionManager singleton.

Routing table
─────────────
  vf_intelligence_out
    key prefix "wait:{stand_id}"  → ops channel  (no seat_id context yet)
    key prefix "flow:{zone}"      → zone:{zone} + ops
    anything else                 → ops only

  vf_alerts
    has "seat_id" field           → fan:{seat_id} + ops
    has "zone" field              → zone:{zone}  + ops
    has "stand_id" field          → zone of that stand via STAND_ZONE_MAP + ops
    fallback                      → ops only

Performance note
────────────────
The Kafka poll runs in asyncio's default thread-pool executor so it never
blocks the event loop.  Each dispatched message takes < 1 ms (JSON parse +
two or three local sends), keeping us well inside the < 8-second end-to-end
latency budget.

Lifecycle
─────────
  start()  — called from FastAPI lifespan, spawns the loop as an asyncio Task
  stop()   — sets the cancellation flag; the task finishes within one poll
             timeout (0.5 s) then flushes and closes the consumer cleanly.
"""

import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Optional

from confluent_kafka import Consumer, KafkaError, KafkaException

from api.websocket_manager import manager as ws_manager
from models.schemas import (
    CrowdSensorReading,
    QueueSensorReading,
    BLEBeaconReading,
    CCTVReading,
    VenueSnapshot,
)

log = logging.getLogger("vf.bridge")

KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
BRIDGE_TOPICS = ["vf_intelligence_out", "vf_alerts"]

# Consumer group separate from the analytics consumer so we don't steal messages
BRIDGE_GROUP_ID = "vf-ws-bridge"

# Maps stand_id → zone for alert routing (mirrors sensor orchestrator config)
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


# ── Pydantic models for Phase 3 bridge messages ───────────────────────────────
# These are declared here rather than in schemas.py to keep Phase 1/2 models
# stable.  They represent the *output* envelope the bridge sends over WebSocket.

from pydantic import BaseModel, Field
from typing import Any, Literal


class FlowIntelligenceMsg(BaseModel):
    """Validated message shape from vf_intelligence_out (flow prefix)."""
    zone: str
    current_density: float
    predicted_density: float
    congestion_risk: str
    recommended_route: list[str]
    avoid_zones: list[str]
    generated_at: str
    _type: str = "flow_intelligence"

    @classmethod
    def try_parse(cls, d: dict) -> Optional["FlowIntelligenceMsg"]:
        try:
            return cls(**d)
        except Exception:
            return None


class WaitIntelligenceMsg(BaseModel):
    """Validated message shape from vf_intelligence_out (wait prefix)."""
    stand_id: str
    stand_name: str
    predicted_wait_5min: float
    predicted_wait_10min: float
    halftime_spike_expected: bool
    recommendation: str
    generated_at: str
    _type: str = "wait_intelligence"

    @classmethod
    def try_parse(cls, d: dict) -> Optional["WaitIntelligenceMsg"]:
        try:
            return cls(**d)
        except Exception:
            return None


class AlertMsg(BaseModel):
    """Validated message shape from vf_alerts."""
    severity: str
    category: str
    message: str
    timestamp: str
    seat_id: Optional[str] = None
    zone: Optional[str] = None
    stand_id: Optional[str] = None
    _type: str = "alert"

    @classmethod
    def try_parse(cls, d: dict) -> Optional["AlertMsg"]:
        try:
            return cls(**d)
        except Exception:
            return None


# ── Bridge implementation ─────────────────────────────────────────────────────

class KafkaWebSocketBridge:
    """
    Consumes vf_intelligence_out + vf_alerts and fans messages out to the
    correct WebSocket rooms via the shared ConnectionManager.
    """

    def __init__(self) -> None:
        self._consumer: Optional[Consumer] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None

    # ── public API ────────────────────────────────────────────────────────────

    def start(self) -> asyncio.Task:
        """Create and return the background consumer task."""
        self._running = True
        self._task = asyncio.create_task(self._run(), name="kafka-ws-bridge")
        log.info("Kafka→WS bridge task created")
        return self._task

    async def stop(self) -> None:
        """Cancel the background task and wait for it to finish cleanly."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        log.info("Kafka→WS bridge stopped")

    # ── consumer loop ─────────────────────────────────────────────────────────

    def _make_consumer(self) -> Consumer:
        return Consumer(
            {
                "bootstrap.servers": KAFKA_BROKERS,
                "group.id": BRIDGE_GROUP_ID,
                "auto.offset.reset": "latest",        # only new messages for the WS
                "enable.auto.commit": True,
                "auto.commit.interval.ms": 5000,
                "session.timeout.ms": 30_000,
                "heartbeat.interval.ms": 10_000,
            }
        )

    async def _run(self) -> None:
        """Main loop: poll Kafka in executor, dispatch to WebSocket rooms."""
        log.info("Bridge loop starting – topics: %s", BRIDGE_TOPICS)
        try:
            self._consumer = self._make_consumer()
            self._consumer.subscribe(BRIDGE_TOPICS)
        except KafkaException as exc:
            log.error("Bridge could not connect to Kafka: %s", exc)
            return

        loop = asyncio.get_event_loop()

        try:
            while self._running:
                # Non-blocking poll in executor so the event loop stays free
                msg = await loop.run_in_executor(
                    None, lambda: self._consumer.poll(timeout=0.5)
                )

                if msg is None:
                    await asyncio.sleep(0)   # yield to other coroutines
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    log.error("Kafka bridge error: %s", msg.error())
                    continue

                topic = msg.topic()
                key_raw = msg.key()
                key: str = key_raw.decode("utf-8") if key_raw else ""

                try:
                    value: dict = json.loads(msg.value().decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    log.warning("Bridge: bad JSON on %s: %s", topic, exc)
                    continue

                # Dispatch without blocking the consumer loop
                asyncio.create_task(self._dispatch(topic, key, value))

        except asyncio.CancelledError:
            log.info("Bridge task cancelled – flushing consumer")
        except Exception as exc:
            log.error("Bridge loop fatal error: %s", exc, exc_info=True)
        finally:
            if self._consumer:
                self._consumer.close()
                log.info("Bridge Kafka consumer closed")

    # ── routing / dispatch ────────────────────────────────────────────────────

    async def _dispatch(self, topic: str, key: str, value: dict) -> None:
        """Route a validated message to the correct WebSocket room(s)."""
        try:
            if topic == "vf_intelligence_out":
                await self._route_intelligence(key, value)
            elif topic == "vf_alerts":
                await self._route_alert(value)
        except Exception as exc:
            log.warning("Dispatch error (topic=%s key=%s): %s", topic, key, exc)

    async def _route_intelligence(self, key: str, value: dict) -> None:
        """
        Route messages from vf_intelligence_out.

        Key scheme (written by consumer/service.py):
          "flow:{zone}"       → send to zone:{zone} + ops
          "wait:{stand_id}"   → send to ops only (no seat association)
        """
        envelope = {**value, "_routed_at": datetime.utcnow().isoformat()}

        if key.startswith("flow:"):
            zone = key[len("flow:"):]
            msg = FlowIntelligenceMsg.try_parse(value)
            payload = msg.model_dump() if msg else envelope
            payload.setdefault("_type", "flow_intelligence")

            await asyncio.gather(
                ws_manager.broadcast_zone(zone, payload),
                ws_manager.broadcast_ops(payload),
                return_exceptions=True,
            )
            log.debug("Bridge → zone:%s + ops  (flow intelligence)", zone)

        elif key.startswith("wait:"):
            stand_id = key[len("wait:"):]
            msg = WaitIntelligenceMsg.try_parse(value)
            payload = msg.model_dump() if msg else envelope
            payload.setdefault("_type", "wait_intelligence")

            await ws_manager.broadcast_ops(payload)
            log.debug("Bridge → ops  (wait intelligence stand=%s)", stand_id)

        else:
            # Unknown intelligence sub-type; broadcast to ops only
            envelope.setdefault("_type", "intelligence")
            await ws_manager.broadcast_ops(envelope)

    async def _route_alert(self, value: dict) -> None:
        """
        Route messages from vf_alerts.

        Priority:
          1. seat_id present       → fan:{seat_id} + ops
          2. zone present          → zone:{zone} + ops
          3. stand_id present      → zone of that stand (lookup) + ops
          4. fallback              → ops only
        """
        msg = AlertMsg.try_parse(value)
        payload = msg.model_dump() if msg else value
        payload.setdefault("_type", "alert")
        payload.setdefault("_routed_at", datetime.utcnow().isoformat())

        seat_id: Optional[str] = value.get("seat_id")
        zone: Optional[str] = value.get("zone")
        stand_id: Optional[str] = value.get("stand_id")

        if seat_id:
            await asyncio.gather(
                ws_manager.send_to_fan(seat_id, payload),
                ws_manager.broadcast_ops(payload),
                return_exceptions=True,
            )
            log.debug("Bridge → fan:%s + ops  (alert)", seat_id)

        elif zone:
            await asyncio.gather(
                ws_manager.broadcast_zone(zone, payload),
                ws_manager.broadcast_ops(payload),
                return_exceptions=True,
            )
            log.debug("Bridge → zone:%s + ops  (alert)", zone)

        elif stand_id:
            mapped_zone = STAND_ZONE_MAP.get(stand_id.upper())
            tasks = [ws_manager.broadcast_ops(payload)]
            if mapped_zone:
                tasks.append(ws_manager.broadcast_zone(mapped_zone, payload))
                log.debug(
                    "Bridge → zone:%s + ops  (alert via stand=%s)",
                    mapped_zone, stand_id,
                )
            else:
                log.debug("Bridge → ops  (alert stand=%s, no zone map)", stand_id)
            await asyncio.gather(*tasks, return_exceptions=True)

        else:
            await ws_manager.broadcast_ops(payload)
            log.debug("Bridge → ops only  (alert, no routing keys)")


# ── module-level singleton ────────────────────────────────────────────────────

bridge = KafkaWebSocketBridge()
