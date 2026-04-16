"""
VenueFlow – Sensor Orchestrator
Combines all sensor arrays and produces the unified VenueSnapshot.

Kafka integration
-----------------
Every call to `get_snapshot_async()` (the async variant used by FastAPI)
atomically pushes:
  • each CrowdSensorReading  → vf_crowd_raw
  • each QueueSensorReading  → vf_queues_raw

The synchronous `get_snapshot()` is kept for backwards-compatibility with
any non-async callers (e.g., unit tests).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from models.schemas import (
    VenueSnapshot,
    CrowdSensorReading,
    QueueSensorReading,
)
from sensors.crowd_sensors import CrowdSensorArray
from sensors.queue_sensors import QueueSensorArray
from sensors.ble_cctv_sensors import BLEBeaconArray, CCTVCameraArray
from sensors.producer import kafka_producer, TOPIC_CROWD_RAW, TOPIC_QUEUES_RAW

logger = logging.getLogger("venueflow.orchestrator")


class SensorOrchestrator:
    """
    Central coordinator for all virtual sensors.
    Called by the FastAPI layer to produce on-demand snapshots.
    """

    def __init__(self) -> None:
        self.crowd_array = CrowdSensorArray()
        self.queue_array = QueueSensorArray()
        self.ble_array = BLEBeaconArray()
        self.cctv_array = CCTVCameraArray()

    # ── Internal snapshot builder (pure, sync) ────────────────────────────────

    def _build_snapshot(self) -> VenueSnapshot:
        """Read all sensors and assemble a VenueSnapshot. No I/O side-effects."""
        crowd_readings = self.crowd_array.read_all()
        queue_readings = self.queue_array.read_all()
        ble_readings = self.ble_array.read_all()
        cctv_readings = self.cctv_array.read_all()

        # Compute total attendance from BLE active device counts (unique per zone)
        total_attendance = sum(b.active_devices for b in ble_readings) // 2  # rough dedup factor

        # Overall congestion level from crowd density average
        avg_density = sum(c.density for c in crowd_readings) / len(crowd_readings) if crowd_readings else 0
        if avg_density < 0.35:
            congestion = "low"
        elif avg_density < 0.60:
            congestion = "moderate"
        elif avg_density < 0.80:
            congestion = "high"
        else:
            congestion = "critical"

        # Generate alerts
        alerts: list[str] = []
        for q in queue_readings:
            if q.wait_minutes > 15:
                alerts.append(
                    f"Long queue at {q.stand_name} ({q.wait_minutes:.0f} min wait) — consider opening reserve stand"
                )
            elif q.wait_minutes > 10:
                alerts.append(f"Moderate queue at {q.stand_name} ({q.wait_minutes:.0f} min wait)")

        for c in crowd_readings:
            if c.density > 0.85:
                alerts.append(
                    f"High crowd density in {c.zone} ({c.density:.0%}) — activate crowd management"
                )

        for cam in cctv_readings:
            if cam.anomaly_detected:
                alerts.append(
                    f"Anomaly detected by {cam.camera_id} in {cam.zone}: {cam.anomaly_type}"
                )

        return VenueSnapshot(
            timestamp=datetime.utcnow(),
            total_attendance=total_attendance,
            crowd_sensors=crowd_readings,
            queue_sensors=queue_readings,
            ble_beacons=ble_readings,
            cctv_cameras=cctv_readings,
            venue_congestion_level=congestion,
            alerts=alerts,
        )

    # ── Kafka publish helpers ─────────────────────────────────────────────────

    async def _publish_crowd(self, readings: list[CrowdSensorReading]) -> None:
        """Fan-out: publish each crowd reading as a discrete Kafka message."""
        tasks = [
            kafka_producer.publish(
                TOPIC_CROWD_RAW,
                reading.model_dump(mode="json"),   # Pydantic v2 → pure-JSON dict
            )
            for reading in readings
        ]
        await asyncio.gather(*tasks)
        logger.debug("Published %d crowd reading(s) → %s", len(readings), TOPIC_CROWD_RAW)

    async def _publish_queues(self, readings: list[QueueSensorReading]) -> None:
        """Fan-out: publish each queue reading as a discrete Kafka message."""
        tasks = [
            kafka_producer.publish(
                TOPIC_QUEUES_RAW,
                reading.model_dump(mode="json"),
            )
            for reading in readings
        ]
        await asyncio.gather(*tasks)
        logger.debug("Published %d queue reading(s) → %s", len(readings), TOPIC_QUEUES_RAW)

    # ── Public API ────────────────────────────────────────────────────────────

    async def get_snapshot_async(self) -> VenueSnapshot:
        """
        Async entry-point used by all FastAPI route handlers.

        1. Builds the VenueSnapshot synchronously (pure computation).
        2. Concurrently publishes crowd and queue sub-readings to Kafka.
        3. Returns the snapshot immediately — Kafka publish is non-blocking.
        """
        snapshot = self._build_snapshot()

        # Publish to Kafka concurrently; do NOT await before returning —
        # if Kafka is unreachable the API response must still succeed.
        asyncio.ensure_future(
            asyncio.gather(
                self._publish_crowd(snapshot.crowd_sensors),
                self._publish_queues(snapshot.queue_sensors),
                return_exceptions=True,
            )
        )

        return snapshot

    def get_snapshot(self) -> VenueSnapshot:
        """
        Synchronous snapshot (backwards-compatible).
        Does NOT publish to Kafka. Use `get_snapshot_async()` from FastAPI routes.
        """
        return self._build_snapshot()
