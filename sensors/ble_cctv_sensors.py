"""
VenueFlow – Virtual BLE Beacons & CCTV Cameras
Simulates indoor positioning beacons and computer-vision camera feeds.
"""

import random
import math
from datetime import datetime
from models.schemas import BLEBeaconReading, CCTVReading

# ─── BLE BEACONS ────────────────────────────────────────────────────────────

BLE_BEACON_LOCATIONS = [
    ("BLE-001", "Gate North – Entry",      "Gate-North"),
    ("BLE-002", "Gate North – Exit",       "Gate-North"),
    ("BLE-003", "Gate South – Entry",      "Gate-South"),
    ("BLE-004", "Gate East – Entry",       "Gate-East"),
    ("BLE-005", "Concourse A – Midpoint",  "Concourse-A"),
    ("BLE-006", "Concourse A – East end",  "Concourse-A"),
    ("BLE-007", "Concourse B – West",      "Concourse-B"),
    ("BLE-008", "Concourse B – East",      "Concourse-B"),
    ("BLE-009", "Concourse C – Center",    "Concourse-C"),
    ("BLE-010", "Lower Stand – Block D",   "Lower-Stands"),
    ("BLE-011", "Lower Stand – Block A",   "Lower-Stands"),
    ("BLE-012", "Upper Stand – North",     "Upper-Stands"),
    ("BLE-013", "VIP Lounge – Entrance",   "VIP-Lounge"),
    ("BLE-014", "Parking P1 – Entry",      "Parking-P1"),
    ("BLE-015", "Parking P2 – Entry",      "Parking-P2"),
]

ZONE_DEVICE_BASE = {
    "Gate-North": 400, "Gate-South": 280, "Gate-East": 520,
    "Concourse-A": 650, "Concourse-B": 480, "Concourse-C": 320,
    "Lower-Stands": 1200, "Upper-Stands": 900, "VIP-Lounge": 150,
    "Parking-P1": 350, "Parking-P2": 290,
}


class BLEBeacon:
    def __init__(self, beacon_id: str, location_label: str, zone: str):
        self.beacon_id = beacon_id
        self.location_label = location_label
        self.zone = zone
        self._tick = 0
        self._base_devices = ZONE_DEVICE_BASE.get(zone, 300)

    def read(self) -> BLEBeaconReading:
        self._tick += 1
        # Device count fluctuates with crowd movement patterns
        noise = random.gauss(0, self._base_devices * 0.08)
        halftime = 0.0
        phase = (self._tick % 120) / 120.0
        if 0.46 < phase < 0.59:
            halftime = self._base_devices * 0.3 * math.sin((phase - 0.46) / 0.13 * math.pi)
        active_devices = max(0, int(self._base_devices + noise + halftime))

        # RSSI varies with congestion (more bodies = more signal attenuation)
        density_factor = active_devices / self._base_devices
        rssi_avg = round(-55 - (density_factor * 15) + random.gauss(0, 2), 1)

        # Dwell time: higher in stands, lower at gates (people moving through)
        dwell_base = 180 if "Stand" in self.zone or "Lounge" in self.zone else 45
        dwell_time_avg = round(max(5, dwell_base + random.gauss(0, 20)), 1)

        return BLEBeaconReading(
            beacon_id=self.beacon_id,
            location_label=self.location_label,
            zone=self.zone,
            timestamp=datetime.utcnow(),
            active_devices=active_devices,
            rssi_avg=rssi_avg,
            dwell_time_avg=dwell_time_avg,
        )


class BLEBeaconArray:
    def __init__(self):
        self.beacons = [BLEBeacon(*b) for b in BLE_BEACON_LOCATIONS]

    def read_all(self) -> list[BLEBeaconReading]:
        return [b.read() for b in self.beacons]

    def read_zone(self, zone: str) -> list[BLEBeaconReading]:
        return [b.read() for b in self.beacons if b.zone == zone]


# ─── CCTV CAMERAS ───────────────────────────────────────────────────────────

CAMERA_DEPLOYMENTS = [
    ("CAM-001", "Gate-North"),
    ("CAM-002", "Gate-South"),
    ("CAM-003", "Gate-East"),
    ("CAM-004", "Gate-West"),
    ("CAM-005", "Concourse-A"),
    ("CAM-006", "Concourse-A"),
    ("CAM-007", "Concourse-B"),
    ("CAM-008", "Concourse-C"),
    ("CAM-009", "Lower-Stands"),
    ("CAM-010", "Upper-Stands"),
    ("CAM-011", "VIP-Lounge"),
    ("CAM-012", "Parking-P1"),
]

ANOMALY_TYPES = [
    "crowd_surge", "unattended_bag", "medical_emergency",
    "queue_overflow", "restricted_area_breach"
]

ZONE_HEADCOUNT_BASE = {
    "Gate-North": 350, "Gate-South": 220, "Gate-East": 480,
    "Gate-West": 180, "Concourse-A": 600, "Concourse-B": 420,
    "Concourse-C": 280, "Lower-Stands": 2500, "Upper-Stands": 1800,
    "VIP-Lounge": 120, "Parking-P1": 300,
}


class CCTVCamera:
    """
    Simulates a CV-enabled CCTV camera with headcount estimation
    and anomaly detection (very low probability events).
    """

    def __init__(self, camera_id: str, zone: str):
        self.camera_id = camera_id
        self.zone = zone
        self._tick = 0
        self._base_headcount = ZONE_HEADCOUNT_BASE.get(zone, 200)

    def read(self) -> CCTVReading:
        self._tick += 1
        noise = random.gauss(0, self._base_headcount * 0.05)
        headcount = max(0, int(self._base_headcount + noise))

        # Motion intensity correlates with crowd density
        motion = min(1.0, max(0.0, (headcount / self._base_headcount) * 0.7 + random.uniform(0, 0.3)))

        # Anomaly: 0.5% chance per reading (realistic low-frequency events)
        anomaly_detected = random.random() < 0.005
        anomaly_type = random.choice(ANOMALY_TYPES) if anomaly_detected else None

        return CCTVReading(
            camera_id=self.camera_id,
            zone=self.zone,
            timestamp=datetime.utcnow(),
            headcount_estimate=headcount,
            motion_intensity=round(motion, 3),
            anomaly_detected=anomaly_detected,
            anomaly_type=anomaly_type,
        )


class CCTVCameraArray:
    def __init__(self):
        self.cameras = [CCTVCamera(*c) for c in CAMERA_DEPLOYMENTS]

    def read_all(self) -> list[CCTVReading]:
        return [c.read() for c in self.cameras]

    def get_anomalies(self) -> list[CCTVReading]:
        return [r for r in self.read_all() if r.anomaly_detected]
