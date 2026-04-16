"""
VenueFlow – Pydantic data models shared across sensors and API.
"""

from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
import uuid


class CrowdSensorReading(BaseModel):
    sensor_id: str
    zone: str
    timestamp: datetime
    density: float = Field(..., ge=0.0, le=1.0, description="0=empty, 1=full capacity")
    headcount: int
    flow_rate: float = Field(..., description="People per minute passing through")
    direction: str = Field(..., description="inbound | outbound | mixed")


class QueueSensorReading(BaseModel):
    sensor_id: str
    stand_id: str
    stand_name: str
    zone: str
    timestamp: datetime
    queue_length: int
    wait_minutes: float
    capacity_pct: float = Field(..., ge=0.0, le=1.0)
    is_open: bool = True


class BLEBeaconReading(BaseModel):
    beacon_id: str
    location_label: str
    zone: str
    timestamp: datetime
    active_devices: int
    rssi_avg: float = Field(..., description="Average signal strength (dBm)")
    dwell_time_avg: float = Field(..., description="Avg seconds devices stay in range")


class CCTVReading(BaseModel):
    camera_id: str
    zone: str
    timestamp: datetime
    headcount_estimate: int
    motion_intensity: float = Field(..., ge=0.0, le=1.0)
    anomaly_detected: bool = False
    anomaly_type: Optional[str] = None


class VenueSnapshot(BaseModel):
    """Aggregated real-time venue state across all sensor types."""
    snapshot_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime
    total_attendance: int
    crowd_sensors: list[CrowdSensorReading]
    queue_sensors: list[QueueSensorReading]
    ble_beacons: list[BLEBeaconReading]
    cctv_cameras: list[CCTVReading]
    venue_congestion_level: str = Field(..., description="low | moderate | high | critical")
    alerts: list[str] = []
