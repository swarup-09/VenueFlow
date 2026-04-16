"""
VenueFlow – Kafka Consumer + Intelligence Runner
================================================
Reads from vf_crowd_raw and vf_queues_raw, persists to TimescaleDB,
runs the Flow Predictor and Wait Estimator, writes results back to
vf_intelligence_out and vf_alerts topics, and updates the Digital Twin.

Run standalone:
    cd venueflow
    python -m consumer.service
"""

import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

# Add parent to path when running as module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.database import (
    get_pool, close_pool,
    insert_crowd_reading, insert_queue_reading,
    upsert_digital_twin_zone, upsert_digital_twin_stand,
    upsert_wait_prediction, upsert_flow_prediction,
    insert_alert,
)
from intelligence.wait_estimator import WaitEstimator
from intelligence.flow_predictor import FlowPredictor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
log = logging.getLogger("vf.consumer")

KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPICS = ["vf_crowd_raw", "vf_queues_raw"]
INTELLIGENCE_TOPIC = "vf_intelligence_out"
ALERTS_TOPIC = "vf_alerts"

# Run intelligence engines every N crowd batches (avoid every-message overhead)
INTELLIGENCE_EVERY_N = 5


class VenueFlowConsumer:
    def __init__(self):
        self._consumer = Consumer({
            "bootstrap.servers": KAFKA_BROKERS,
            "group.id": "vf-intelligence-consumer",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 10000,
        })
        self._producer = Producer({
            "bootstrap.servers": KAFKA_BROKERS,
            "linger.ms": 50,
            "compression.type": "lz4",
        })
        self._wait_estimator = WaitEstimator(window=5)
        self._flow_predictor = FlowPredictor()
        self._running = False

        # Rolling buffers for intelligence batch processing
        self._crowd_buffer: list[dict] = []
        self._queue_buffer: list[dict] = []
        self._crowd_tick = 0

    def _publish(self, topic: str, key: str, payload: dict):
        try:
            self._producer.produce(
                topic,
                key=key.encode(),
                value=json.dumps(payload, default=str).encode(),
            )
            self._producer.poll(0)
        except KafkaException as e:
            log.warning(f"Publish to {topic} failed: {e}")

    async def _handle_crowd(self, msg_value: dict):
        """Persist crowd reading and buffer for intelligence."""
        ts = datetime.fromisoformat(msg_value["timestamp"].replace("Z", "+00:00"))
        row = {**msg_value, "timestamp": ts}
        try:
            await insert_crowd_reading(row)
        except Exception as e:
            log.warning(f"DB insert crowd failed: {e}")

        # Update digital twin zone
        congestion = (
            "low" if row["density"] < 0.35 else
            "moderate" if row["density"] < 0.60 else
            "high" if row["density"] < 0.80 else
            "critical"
        )
        try:
            await upsert_digital_twin_zone(row["zone"], {**row, "congestion_level": congestion})
        except Exception as e:
            log.warning(f"DB upsert zone failed: {e}")

        self._crowd_buffer.append(msg_value)
        self._crowd_tick += 1

        # Run intelligence engines every N crowd messages
        if self._crowd_tick % INTELLIGENCE_EVERY_N == 0 and self._crowd_buffer:
            await self._run_flow_intelligence()

    async def _handle_queue(self, msg_value: dict):
        """Persist queue reading and run wait estimator."""
        ts = datetime.fromisoformat(msg_value["timestamp"].replace("Z", "+00:00"))
        row = {**msg_value, "timestamp": ts}
        try:
            await insert_queue_reading(row)
        except Exception as e:
            log.warning(f"DB insert queue failed: {e}")

        # Update digital twin stand
        try:
            await upsert_digital_twin_stand(row["stand_id"], row)
        except Exception as e:
            log.warning(f"DB upsert stand failed: {e}")

        self._queue_buffer.append(msg_value)

        # Run wait estimator immediately on each queue message
        await self._run_wait_intelligence(msg_value)

    async def _run_flow_intelligence(self):
        """Flow Predictor: graph-based crowd routing per zone."""
        if not self._crowd_buffer:
            return
        try:
            predictions = self._flow_predictor.predict_all(self._crowd_buffer)
            for p in predictions:
                pred_dict = {
                    "zone": p.zone,
                    "current_density": p.current_density,
                    "predicted_density": p.predicted_density,
                    "congestion_risk": p.congestion_risk,
                    "recommended_route": p.recommended_route,
                    "avoid_zones": p.avoid_zones,
                    "generated_at": datetime.utcnow().isoformat(),
                }
                # Persist
                try:
                    await upsert_flow_prediction(p.zone, pred_dict)
                except Exception as e:
                    log.warning(f"DB flow pred failed: {e}")

                # Publish to intelligence topic
                self._publish(INTELLIGENCE_TOPIC, f"flow:{p.zone}", pred_dict)

                # Alert if critical
                if p.congestion_risk == "critical":
                    msg = (
                        f"CRITICAL congestion in {p.zone} "
                        f"(density={p.current_density:.0%}) — "
                        f"activate crowd management"
                    )
                    self._publish(ALERTS_TOPIC, p.zone, {
                        "severity": "critical", "category": "crowd",
                        "zone": p.zone, "message": msg,
                        "timestamp": datetime.utcnow().isoformat(),
                    })
                    try:
                        await insert_alert("critical", "crowd", msg, zone=p.zone)
                    except Exception:
                        pass
                    log.warning(f"ALERT: {msg}")

            # Keep buffer small — sliding window of last 50 readings
            self._crowd_buffer = self._crowd_buffer[-50:]
            log.info(f"Flow intelligence: {len(predictions)} zones processed")

        except Exception as e:
            log.error(f"Flow predictor error: {e}", exc_info=True)

    async def _run_wait_intelligence(self, queue_msg: dict):
        """Wait Estimator: M/M/c prediction for a single stand."""
        try:
            pred = self._wait_estimator.predict(
                queue_msg["stand_id"],
                queue_msg["stand_name"],
                queue_msg["queue_length"],
                queue_msg["wait_minutes"],
            )
            pred_dict = {
                "stand_id": pred.stand_id,
                "stand_name": pred.stand_name,
                "predicted_wait_5min": pred.predicted_wait_5min,
                "predicted_wait_10min": pred.predicted_wait_10min,
                "lambda_": pred.lambda_,
                "mu": pred.mu,
                "c": pred.c,
                "rho": pred.rho,
                "halftime_spike_expected": pred.halftime_spike_expected,
                "recommendation": pred.recommendation,
                "generated_at": datetime.utcnow().isoformat(),
            }
            try:
                await upsert_wait_prediction(pred.stand_id, pred_dict)
            except Exception as e:
                log.warning(f"DB wait pred failed: {e}")

            self._publish(INTELLIGENCE_TOPIC, f"wait:{pred.stand_id}", pred_dict)

            # Alert on spike detection
            if pred.halftime_spike_expected:
                msg = pred.recommendation
                self._publish(ALERTS_TOPIC, pred.stand_id, {
                    "severity": "warning", "category": "queue",
                    "stand_id": pred.stand_id, "message": msg,
                    "timestamp": datetime.utcnow().isoformat(),
                })
                try:
                    await insert_alert("warning", "queue", msg, stand_id=pred.stand_id)
                except Exception:
                    pass
                log.warning(f"SPIKE ALERT [{pred.stand_id}]: {msg}")

        except Exception as e:
            log.error(f"Wait estimator error: {e}", exc_info=True)

    async def run(self):
        """Main consumer loop."""
        self._running = True
        self._consumer.subscribe(TOPICS)
        log.info(f"Subscribed to topics: {TOPICS}")

        # Ensure DB pool is ready
        await get_pool()
        log.info("DB pool ready")

        loop = asyncio.get_event_loop()

        try:
            while self._running:
                # Poll with short timeout — run in executor to not block event loop
                msg = await loop.run_in_executor(
                    None, lambda: self._consumer.poll(timeout=0.5)
                )

                if msg is None:
                    await asyncio.sleep(0.01)
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    log.error(f"Kafka error: {msg.error()}")
                    continue

                topic = msg.topic()
                try:
                    value = json.loads(msg.value().decode("utf-8"))
                except json.JSONDecodeError as e:
                    log.warning(f"Bad JSON on {topic}: {e}")
                    continue

                if topic == "vf_crowd_raw":
                    await self._handle_crowd(value)
                elif topic == "vf_queues_raw":
                    await self._handle_queue(value)

        except asyncio.CancelledError:
            log.info("Consumer task cancelled — shutting down")
        except Exception as e:
            log.error(f"Consumer loop error: {e}", exc_info=True)
        finally:
            self._consumer.close()
            self._producer.flush(timeout=5)
            await close_pool()
            log.info("Consumer shut down cleanly")

    def stop(self):
        self._running = False


async def main():
    consumer = VenueFlowConsumer()

    def _shutdown(sig, frame):
        log.info(f"Signal {sig} received — stopping consumer")
        consumer.stop()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    await consumer.run()


if __name__ == "__main__":
    asyncio.run(main())
