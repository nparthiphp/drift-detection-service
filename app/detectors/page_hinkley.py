"""
Page-Hinkley — sequential change-point detector.

Wraps river.drift.PageHinkley. Where ADWIN tells you "something has shifted
somewhere in this window", Page-Hinkley is tuned to answer "the mean of this
stream has sustained a step-change, and here is roughly when it started" —
which is exactly what an RCA copilot wants: a timestamp to anchor the
"drift started at 14:32" narrative.

It accumulates a cumulative sum of deviations from the running mean; when
that cumulative sum exceeds `threshold`, a change is flagged. It is less
sensitive to noise than ADWIN but slower to reset after a flagged change,
so the two are complementary rather than redundant.
"""
from __future__ import annotations

from dataclasses import dataclass

from river.drift import PageHinkley as _RiverPageHinkley


@dataclass
class PageHinkleyResult:
    drift_detected: bool
    n_updates: int


class PageHinkleyDetector:
    """
    Usage:
        detector = PageHinkleyDetector(threshold=50.0, min_instances=30)
        for value in stream:
            result = detector.update(value)
            if result.drift_detected:
                ... mark change-point timestamp for RCA ...
    """

    def __init__(
        self,
        min_instances: int = 30,
        delta: float = 0.005,
        threshold: float = 50.0,
        alpha: float = 0.9999,
        mode: str = "both",
    ):
        """
        min_instances: warm-up period before drift can be flagged.
        delta: magnitude of allowed deviation before it counts against the sum.
        threshold: cumulative deviation required to flag a change - lower
            values catch smaller shifts faster but raise false-positive risk.
        mode: "up" (increase only), "down" (decrease only), or "both".
              For network latency/error-rate metrics, "up" is often what you
              actually want (you rarely alert on things getting *better*).
        """
        self._ph = _RiverPageHinkley(
            min_instances=min_instances, delta=delta, threshold=threshold, alpha=alpha, mode=mode
        )
        self.n_updates = 0
        self.n_drifts = 0

    def update(self, value: float) -> PageHinkleyResult:
        self._ph.update(value)
        self.n_updates += 1
        drift = bool(self._ph.drift_detected)
        if drift:
            self.n_drifts += 1
        return PageHinkleyResult(drift_detected=drift, n_updates=self.n_updates)

    def reset(self) -> None:
        self._ph = _RiverPageHinkley(
            min_instances=self._ph.min_instances,
            delta=self._ph.delta,
            threshold=self._ph.threshold,
            alpha=self._ph.alpha,
            mode=self._ph.mode,
        )
        self.n_updates = 0
        self.n_drifts = 0
