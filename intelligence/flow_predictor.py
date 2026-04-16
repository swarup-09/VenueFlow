"""
VenueFlow Intelligence Layer – Flow Predictor
=============================================
Uses a weighted directed graph of the venue to compute optimal fan routing.

Algorithm
---------
  • Venue zones are graph NODES.
  • Concourse corridors are directed EDGES with a base travel-time weight.
  • Edge weight is dynamically inflated by zone density:
        w_effective = w_base × (1 + α × density²)
    where α=4 makes dense zones (>80%) severely penalised.
  • Shortest path is found via Dijkstra's algorithm (stdlib heapq).
  • Zones above the BLOCK_THRESHOLD density are hard-blocked.

The predictor also classifies each zone's congestion risk and forecasts
density 5 minutes ahead using a simple exponential smoothing (ETS):
        D̂_{t+1} = α_ets · D_t + (1 - α_ets) · D̂_t
"""

import heapq
import math
from dataclasses import dataclass, field
from typing import Optional


# ─── Venue graph ──────────────────────────────────────────────────────────────
# Edge: (from_zone, to_zone, base_travel_seconds)
# Bidirectional unless otherwise noted.

VENUE_EDGES: list[tuple[str, str, float]] = [
    # Gates → Concourses
    ("Gate-North",  "Concourse-A",  45),
    ("Gate-South",  "Concourse-C",  40),
    ("Gate-East",   "Concourse-A",  50),
    ("Gate-East",   "Concourse-B",  55),
    ("Gate-West",   "Concourse-C",  45),
    # Concourse ring
    ("Concourse-A", "Concourse-B",  60),
    ("Concourse-B", "Concourse-C",  60),
    ("Concourse-A", "Concourse-C",  90),   # longer cross-cut
    # Concourses → Stands
    ("Concourse-A", "Lower-Stands", 30),
    ("Concourse-B", "Lower-Stands", 35),
    ("Concourse-C", "Lower-Stands", 40),
    ("Concourse-A", "Upper-Stands", 60),
    ("Concourse-B", "Upper-Stands", 65),
    # VIP bypass
    ("Gate-North",  "VIP-Lounge",   25),
    ("VIP-Lounge",  "Lower-Stands", 20),
    # Parking
    ("Parking-P1",  "Gate-North",   120),
    ("Parking-P1",  "Gate-East",    90),
    ("Parking-P2",  "Gate-South",   100),
    ("Parking-P2",  "Gate-West",    110),
]

# Density above this threshold → zone is hard-blocked from routing
BLOCK_THRESHOLD = 0.90
# Density penalty coefficient
ALPHA = 4.0
# Congestion bands
CONGESTION_LEVELS = [
    (0.35, "low"),
    (0.60, "moderate"),
    (0.80, "high"),
    (1.01, "critical"),
]


def _build_graph(
    zone_densities: dict[str, float],
    blocked: set[str],
) -> dict[str, list[tuple[float, str]]]:
    """Return adjacency list with effective edge weights."""
    graph: dict[str, list[tuple[float, str]]] = {}

    def add_edge(a: str, b: str, base: float):
        # Skip edges into blocked zones
        if a in blocked or b in blocked:
            return
        d_b = zone_densities.get(b, 0.0)
        w = base * (1 + ALPHA * d_b ** 2)
        graph.setdefault(a, []).append((w, b))
        # Bidirectional
        d_a = zone_densities.get(a, 0.0)
        w_rev = base * (1 + ALPHA * d_a ** 2)
        graph.setdefault(b, []).append((w_rev, a))

    for a, b, base in VENUE_EDGES:
        add_edge(a, b, base)

    return graph


def _dijkstra(
    graph: dict[str, list[tuple[float, str]]],
    source: str,
    target: str,
) -> tuple[float, list[str]]:
    """Return (cost, path) from source to target. Returns (inf, []) if unreachable."""
    dist: dict[str, float] = {source: 0.0}
    prev: dict[str, Optional[str]] = {source: None}
    pq: list[tuple[float, str]] = [(0.0, source)]

    while pq:
        cost, node = heapq.heappop(pq)
        if cost > dist.get(node, math.inf):
            continue
        if node == target:
            # Reconstruct path
            path, cur = [], node
            while cur is not None:
                path.append(cur)
                cur = prev.get(cur)
            return cost, list(reversed(path))
        for w, neighbour in graph.get(node, []):
            new_cost = cost + w
            if new_cost < dist.get(neighbour, math.inf):
                dist[neighbour] = new_cost
                prev[neighbour] = node
                heapq.heappush(pq, (new_cost, neighbour))

    return math.inf, []


@dataclass
class ZoneFlowPrediction:
    zone: str
    current_density: float
    predicted_density: float        # ETS 5-min forecast
    congestion_risk: str            # low | moderate | high | critical
    avoid_zones: list[str]
    recommended_route: Optional[str] = None


class FlowPredictor:
    """
    Stateful flow predictor — maintains ETS smoothed density per zone.
    Call update() each time a batch of crowd readings arrives.
    """

    _ETS_ALPHA = 0.3   # smoothing factor

    def __init__(self):
        self._smoothed: dict[str, float] = {}

    def _ets_update(self, zone: str, density: float) -> float:
        prev = self._smoothed.get(zone, density)
        smoothed = self._ETS_ALPHA * density + (1 - self._ETS_ALPHA) * prev
        self._smoothed[zone] = smoothed
        return round(smoothed, 4)

    @staticmethod
    def _congestion_level(density: float) -> str:
        for threshold, label in CONGESTION_LEVELS:
            if density < threshold:
                return label
        return "critical"

    def predict_all(
        self, crowd_readings: list[dict]
    ) -> list[ZoneFlowPrediction]:
        """
        Process a batch of crowd readings and return per-zone predictions.
        Readings are first averaged per zone (multiple sensors per zone).
        """
        # Average density per zone
        zone_sum: dict[str, list[float]] = {}
        for r in crowd_readings:
            zone_sum.setdefault(r["zone"], []).append(r["density"])
        zone_densities = {
            z: sum(vals) / len(vals) for z, vals in zone_sum.items()
        }

        # Identify blocked zones
        blocked = {z for z, d in zone_densities.items() if d >= BLOCK_THRESHOLD}

        # Build dynamic graph
        graph = _build_graph(zone_densities, blocked)

        results: list[ZoneFlowPrediction] = []
        for zone, density in zone_densities.items():
            predicted = self._ets_update(zone, density)
            risk = self._congestion_level(density)

            # Compute avoid list — any zone with density > 0.7 near this zone
            avoid = [
                z for z, d in zone_densities.items()
                if d > 0.70 and z != zone
            ]

            # Find recommended alternative route from this zone to Lower-Stands
            # (most common fan destination during halftime)
            _, path = _dijkstra(graph, zone, "Lower-Stands")
            route_str = " → ".join(path) if len(path) > 1 else None

            results.append(ZoneFlowPrediction(
                zone=zone,
                current_density=round(density, 4),
                predicted_density=predicted,
                congestion_risk=risk,
                avoid_zones=avoid,
                recommended_route=route_str,
            ))

        return results

    def best_route(self, from_zone: str, to_zone: str, crowd_readings: list[dict]) -> dict:
        """
        Public helper: find the crowd-optimised walking route between two zones.
        Returns path, estimated travel time (seconds), and avoided zones.
        """
        zone_sum: dict[str, list[float]] = {}
        for r in crowd_readings:
            zone_sum.setdefault(r["zone"], []).append(r["density"])
        zone_densities = {z: sum(v)/len(v) for z, v in zone_sum.items()}
        blocked = {z for z, d in zone_densities.items() if d >= BLOCK_THRESHOLD}
        graph = _build_graph(zone_densities, blocked)
        cost, path = _dijkstra(graph, from_zone, to_zone)
        avoided = [z for z in blocked if z not in (from_zone, to_zone)]
        return {
            "from_zone": from_zone,
            "to_zone": to_zone,
            "path": path,
            "estimated_seconds": round(cost, 1) if cost < math.inf else None,
            "estimated_minutes": round(cost / 60, 1) if cost < math.inf else None,
            "reachable": cost < math.inf,
            "avoided_zones": avoided,
        }
