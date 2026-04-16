"""
VenueFlow – Virtual Crowd Density Sensors
Simulates pressure-plate / infrared footfall counters across venue zones.
"""

import random
import math
from datetime import datetime
from models.schemas import CrowdSensorReading

# Venue zones and their base crowd profiles
ZONE_PROFILES = {
    "Gate-North":    {"base_density": 0.55, "variance": 0.20, "flow_base": 120},
    "Gate-South":    {"base_density": 0.40, "variance": 0.15, "flow_base": 90},
    "Gate-East":     {"base_density": 0.70, "variance": 0.25, "flow_base": 160},
    "Gate-West":     {"base_density": 0.30, "variance": 0.10, "flow_base": 70},
    "Concourse-A":   {"base_density": 0.60, "variance": 0.20, "flow_base": 200},
    "Concourse-B":   {"base_density": 0.45, "variance": 0.15, "flow_base": 140},
    "Concourse-C":   {"base_density": 0.35, "variance": 0.12, "flow_base": 100},
    "Lower-Stands":  {"base_density": 0.80, "variance": 0.10, "flow_base": 50},
    "Upper-Stands":  {"base_density": 0.65, "variance": 0.12, "flow_base": 40},
    "VIP-Lounge":    {"base_density": 0.30, "variance": 0.08, "flow_base": 20},
    "Parking-P1":    {"base_density": 0.50, "variance": 0.20, "flow_base": 80},
    "Parking-P2":    {"base_density": 0.45, "variance": 0.18, "flow_base": 70},
}


class CrowdSensor:
    """Simulates a single footfall / pressure-plate crowd sensor."""

    def __init__(self, sensor_id: str, zone: str):
        self.sensor_id = sensor_id
        self.zone = zone
        self._tick = 0
        profile = ZONE_PROFILES.get(zone, {"base_density": 0.5, "variance": 0.15, "flow_base": 80})
        self._base_density = profile["base_density"]
        self._variance = profile["variance"]
        self._flow_base = profile["flow_base"]

    def _halftime_spike(self) -> float:
        """Simulate density spike during halftime (every ~60 ticks)."""
        phase = (self._tick % 120) / 120
        return 0.25 * math.sin(phase * math.pi) if 55 < (self._tick % 120) < 70 else 0.0

    def read(self) -> CrowdSensorReading:
        self._tick += 1
        noise = random.gauss(0, self._variance * 0.3)
        halftime_boost = self._halftime_spike()
        density = max(0.0, min(1.0, self._base_density + noise + halftime_boost))

        # Derive headcount assuming 1000 person zone capacity
        headcount = int(density * 1000)

        # Flow rate oscillates with a light sinusoidal pattern
        flow_noise = random.gauss(0, 10)
        flow_rate = max(0.0, self._flow_base * (0.8 + 0.4 * density) + flow_noise)

        # Direction heuristic: high density = more outbound (people leaving crowded areas)
        if density > 0.75:
            direction = "outbound"
        elif density < 0.3:
            direction = "inbound"
        else:
            direction = "mixed"

        return CrowdSensorReading(
            sensor_id=self.sensor_id,
            zone=self.zone,
            timestamp=datetime.utcnow(),
            density=round(density, 3),
            headcount=headcount,
            flow_rate=round(flow_rate, 1),
            direction=direction,
        )


class CrowdSensorArray:
    """Manages all crowd sensors deployed across the venue."""

    def __init__(self):
        self.sensors: list[CrowdSensor] = []
        for zone in ZONE_PROFILES:
            # 2 sensors per zone for redundancy
            for i in range(1, 3):
                sid = f"CS-{zone.upper().replace('-', '')}-{i:02d}"
                self.sensors.append(CrowdSensor(sid, zone))

    def read_all(self) -> list[CrowdSensorReading]:
        """Return one reading per sensor (averaged per zone in practice)."""
        return [s.read() for s in self.sensors]

    def read_zone(self, zone: str) -> list[CrowdSensorReading]:
        return [s.read() for s in self.sensors if s.zone == zone]
