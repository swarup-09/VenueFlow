"""
VenueFlow – Virtual Queue Sensors
Simulates IR beam-break / computer-vision queue depth sensors at concession stands.
"""

import random
import math
from datetime import datetime
from models.schemas import QueueSensorReading

CONCESSION_STANDS = {
    "C4": {"name": "Hot Dogs & Snacks",  "zone": "Concourse-A", "capacity": 40},
    "B2": {"name": "Beer & Beverages",   "zone": "Concourse-B", "capacity": 35},
    "A7": {"name": "Pizza Corner",        "zone": "Concourse-A", "capacity": 30},
    "B5": {"name": "Burgers & Fries",    "zone": "Concourse-B", "capacity": 45},
    "C1": {"name": "Coffee & Desserts",  "zone": "Concourse-C", "capacity": 25},
    "A3": {"name": "Nachos & Wraps",     "zone": "Concourse-A", "capacity": 30},
    "D2": {"name": "VIP Bar",            "zone": "VIP-Lounge",  "capacity": 20},
    "B8": {"name": "South Grill",        "zone": "Concourse-B", "capacity": 35},
}

# Service rate: people served per minute per stand
SERVICE_RATES = {
    "C4": 6.0, "B2": 5.0, "A7": 3.5, "B5": 4.5,
    "C1": 5.5, "A3": 4.0, "D2": 7.0, "B8": 4.5,
}


class QueueSensor:
    """
    Simulates a queue depth sensor for a single concession stand.
    Uses a queuing model: arrival_rate vs service_rate determines queue build-up.
    """

    def __init__(self, stand_id: str):
        self.stand_id = stand_id
        info = CONCESSION_STANDS[stand_id]
        self.stand_name = info["name"]
        self.zone = info["zone"]
        self.capacity = info["capacity"]
        self.service_rate = SERVICE_RATES[stand_id]
        self._queue_length = random.randint(2, 15)
        self._tick = 0

    def _arrival_rate(self) -> float:
        """
        Simulate time-varying arrival rates.
        Spikes at halftime (tick 55–70 of every 120-tick cycle).
        """
        phase = (self._tick % 120) / 120.0
        base_rate = self.service_rate * random.uniform(0.7, 1.4)
        halftime_spike = 0.0
        if 0.46 < phase < 0.59:
            halftime_spike = self.service_rate * 1.5 * math.sin((phase - 0.46) / 0.13 * math.pi)
        return max(0, base_rate + halftime_spike + random.gauss(0, 0.5))

    def read(self) -> QueueSensorReading:
        self._tick += 1
        arrival = self._arrival_rate()
        net_change = (arrival - self.service_rate) * random.uniform(0.8, 1.2)
        self._queue_length = max(0, min(self.capacity, self._queue_length + net_change * 0.5))
        queue_length = int(self._queue_length)

        capacity_pct = queue_length / self.capacity
        # Wait time = queue length / service rate (Little's Law approximation)
        wait_minutes = round(queue_length / self.service_rate, 1) if queue_length > 0 else 0.0

        return QueueSensorReading(
            sensor_id=f"QS-{self.stand_id}",
            stand_id=self.stand_id,
            stand_name=self.stand_name,
            zone=self.zone,
            timestamp=datetime.utcnow(),
            queue_length=queue_length,
            wait_minutes=wait_minutes,
            capacity_pct=round(capacity_pct, 3),
            is_open=True,
        )


class QueueSensorArray:
    """Manages all queue sensors across concession stands."""

    def __init__(self):
        self.sensors = {sid: QueueSensor(sid) for sid in CONCESSION_STANDS}

    def read_all(self) -> list[QueueSensorReading]:
        return [s.read() for s in self.sensors.values()]

    def read_stand(self, stand_id: str) -> QueueSensorReading | None:
        if stand_id in self.sensors:
            return self.sensors[stand_id].read()
        return None

    def get_shortest_queues(self, top_n: int = 3) -> list[QueueSensorReading]:
        readings = self.read_all()
        return sorted(readings, key=lambda r: r.wait_minutes)[:top_n]
