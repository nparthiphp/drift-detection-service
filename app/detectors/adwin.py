"""
ADWIN (Adaptive Windowing) — streaming concept-drift detector.

Wraps river.drift.ADWIN. Unlike PSI, this is point-by-point streaming: you
feed it one value at a time (e.g. per-tick anomaly score, latency reading)
and it maintains a variable-length window internally, shrinking it whenever
it detects that the older and newer halves of the window differ
statistically. No manual window-size tuning required.

Good fit for: EWMA anomaly-score streams, per-interface error-rate streams —
anything from your Redis tumbling-window pipeline that arrives as a
continuous series rather than discrete batches.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from river.drift import ADWIN as _RiverADWIN


@dataclass
class ADWINResult:
    drift_detected: bool
    warning_detected: bool
    estimation: float  # ADWIN's current running mean estimate
    width: int  # current window size ADWIN is tracking


class ADWINDetector:
    """
    Stateful, per-metric-stream ADWIN wrapper.

    Usage:
        detector = ADWINDetector(delta=0.002)
        for value in stream:
            result = detector.update(value)
            if result.drift_detected:
                ... trigger reasoning stage / alert ...
    """

    def __init__(self, delta: float = 0.002):
        """
        delta: confidence parameter. Smaller delta -> more conservative
        (fewer false positives, slower to detect real drift). River's
        default (0.002) is a reasonable starting point; tune down for
        noisy telemetry, up if drift needs to be caught faster.
        """
        self._adwin = _RiverADWIN(delta=delta)
        self.n_updates = 0
        self.n_drifts = 0

    def update(self, value: float) -> ADWINResult:
        self._adwin.update(value)
        self.n_updates += 1
        drift = bool(self._adwin.drift_detected)
        if drift:
            self.n_drifts += 1
        return ADWINResult(
            drift_detected=drift,
            warning_detected=drift,  # river's ADWIN has no separate warning state
            estimation=float(self._adwin.estimation) if self._adwin.estimation is not None else 0.0,
            width=int(self._adwin.width),
        )

    def reset(self) -> None:
        self._adwin = _RiverADWIN(delta=self._adwin.delta)
        self.n_updates = 0
        self.n_drifts = 0
