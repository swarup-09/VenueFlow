"""
VenueFlow Intelligence Layer – Wait Estimator
=============================================
Uses the M/M/c queuing model (Erlang C formula) to:
  1. Compute current utilisation (ρ = λ / c·μ)
  2. Predict wait times 5 and 10 minutes ahead
  3. Detect upcoming halftime spikes before they hit

Theory
------
  λ  = arrival rate  (customers/min)
  μ  = service rate  (customers/min per server)
  c  = number of servers (open tills)
  ρ  = λ / (c · μ)   — utilisation per server  (must be < 1 for stable queue)

Erlang C probability that an arriving customer waits:
  P(wait) = [  (λ/μ)^c / (c! · (1 - ρ))  ]
            / [  Σ_{k=0}^{c-1} (λ/μ)^k/k!  +  (λ/μ)^c / (c! · (1-ρ))  ]

Mean wait in queue (Wq):
  Wq = P(wait) / (c · μ · (1 - ρ))
"""

import math
from dataclasses import dataclass
from typing import Optional


# Per-stand server configuration (open tills during normal operation)
STAND_SERVERS: dict[str, int] = {
    "C4": 3, "B2": 2, "A7": 2, "B5": 3,
    "C1": 2, "A3": 2, "D2": 4, "B8": 2,
}

# Per-stand service rates (customers served per minute per till)
STAND_MU: dict[str, float] = {
    "C4": 2.0, "B2": 1.7, "A7": 1.2, "B5": 1.5,
    "C1": 1.8, "A3": 1.3, "D2": 2.3, "B8": 1.5,
}

# Halftime: a surge arrives at tick ~55–70 of every 120-tick producer cycle.
# We detect an upcoming spike when the 3-reading trend shows rising λ.
HALFTIME_LAMBDA_MULTIPLIER = 2.8   # spike amplitude
SPIKE_LOOKAHEAD_MINUTES = 8        # we warn 8 min before spike


@dataclass
class WaitPrediction:
    stand_id: str
    stand_name: str
    lambda_: float          # current arrival rate (ppl/min)
    mu: float               # service rate per server
    c: int                  # servers
    rho: float              # utilisation
    current_wait_min: float
    predicted_wait_5min: float
    predicted_wait_10min: float
    halftime_spike_expected: bool
    recommendation: str


def _erlang_c(lambda_: float, mu: float, c: int) -> float:
    """Return P(wait) — the Erlang C probability."""
    if lambda_ <= 0 or mu <= 0 or c <= 0:
        return 0.0
    a = lambda_ / mu           # offered traffic (Erlangs)
    rho = a / c                # utilisation per server
    if rho >= 1.0:
        return 1.0             # saturated — everyone waits

    # Numerator: a^c / (c! · (1 - ρ))
    try:
        numerator = (a ** c) / (math.factorial(c) * (1 - rho))
    except OverflowError:
        return 1.0

    # Denominator: sum_{k=0}^{c-1} a^k/k!  +  numerator
    summation = sum((a ** k) / math.factorial(k) for k in range(c))
    denominator = summation + numerator
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _mean_wait_minutes(lambda_: float, mu: float, c: int) -> float:
    """Return mean queue wait in minutes (Wq from M/M/c)."""
    if lambda_ <= 0:
        return 0.0
    pw = _erlang_c(lambda_, mu, c)
    a = lambda_ / mu
    rho = a / c
    if rho >= 1.0:
        # Saturated: approximate wait from queue length
        return min(60.0, lambda_ / (c * mu))
    denom = c * mu * (1 - rho)
    if denom <= 0:
        return 60.0
    wq = pw / denom          # in minutes (λ and μ already in ppl/min)
    return round(min(wq, 60.0), 2)


class WaitEstimator:
    """
    Stateful estimator that tracks a rolling window of queue readings
    per stand and emits M/M/c predictions.
    """

    def __init__(self, window: int = 5):
        # window: number of readings to average for λ estimation
        self._window = window
        # Circular buffer of recent queue lengths per stand_id
        self._history: dict[str, list[float]] = {}

    def _estimate_lambda(self, stand_id: str, queue_length: int) -> float:
        """
        Estimate arrival rate λ from the change in queue length over time.
        Δqueue = λ - c·μ  → λ = Δqueue + c·μ
        Fall back to a sensible default if history is thin.
        """
        buf = self._history.setdefault(stand_id, [])
        buf.append(float(queue_length))
        if len(buf) > self._window:
            buf.pop(0)

        mu = STAND_MU.get(stand_id, 1.5)
        c = STAND_SERVERS.get(stand_id, 2)

        if len(buf) < 2:
            # Insufficient history — assume queue in equilibrium
            return c * mu * 0.9

        delta = buf[-1] - buf[-2]          # queue change in last interval
        lambda_ = max(0.1, c * mu + delta)
        return round(lambda_, 3)

    def _detect_halftime_spike(self, stand_id: str) -> bool:
        """
        Detect upward trend in queue length that signals an imminent halftime rush.
        Flag if 3+ consecutive readings are rising AND growth rate > 20%/reading.
        """
        buf = self._history.get(stand_id, [])
        if len(buf) < 3:
            return False
        recent = buf[-3:]
        increasing = all(recent[i] < recent[i + 1] for i in range(len(recent) - 1))
        if not increasing:
            return False
        growth_rate = (recent[-1] - recent[0]) / max(1, recent[0])
        return growth_rate > 0.20

    def predict(
        self,
        stand_id: str,
        stand_name: str,
        queue_length: int,
        current_wait: float,
    ) -> WaitPrediction:
        mu = STAND_MU.get(stand_id, 1.5)
        c = STAND_SERVERS.get(stand_id, 2)

        lambda_ = self._estimate_lambda(stand_id, queue_length)
        rho = lambda_ / (c * mu)

        # Current wait from M/M/c (should match sensor reading closely)
        mmc_wait = _mean_wait_minutes(lambda_, mu, c)

        # 5-min forecast: assume λ grows 10% if trend is rising
        spike = self._detect_halftime_spike(stand_id)
        lambda_5 = lambda_ * (HALFTIME_LAMBDA_MULTIPLIER * 0.5 if spike else 1.1)
        wait_5 = _mean_wait_minutes(lambda_5, mu, c)

        # 10-min forecast: full spike if detected, else mild growth
        lambda_10 = lambda_ * (HALFTIME_LAMBDA_MULTIPLIER if spike else 1.2)
        wait_10 = _mean_wait_minutes(lambda_10, mu, c)

        # Recommendation
        if spike:
            rec = f"Halftime surge imminent at {stand_name} — open reserve till or push mobile-order nudge now"
        elif rho > 0.85:
            rec = f"{stand_name} near saturation (ρ={rho:.2f}) — consider second server"
        elif mmc_wait > 12:
            rec = f"Long wait at {stand_name} — surface shorter alternatives to nearby fans"
        else:
            rec = f"{stand_name} operating normally (ρ={rho:.2f})"

        return WaitPrediction(
            stand_id=stand_id,
            stand_name=stand_name,
            lambda_=round(lambda_, 3),
            mu=mu,
            c=c,
            rho=round(rho, 3),
            current_wait_min=mmc_wait,
            predicted_wait_5min=round(wait_5, 2),
            predicted_wait_10min=round(wait_10, 2),
            halftime_spike_expected=spike,
            recommendation=rec,
        )

    def predict_all(self, queue_readings: list[dict]) -> list[WaitPrediction]:
        return [
            self.predict(
                r["stand_id"], r["stand_name"],
                r["queue_length"], r["wait_minutes"],
            )
            for r in queue_readings
        ]
