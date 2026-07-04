"""
Population Stability Index (PSI) — batch-style data/covariate drift detector.

PSI compares a REFERENCE distribution (e.g. last week's feature values) against
a CURRENT distribution (this window's values) using fixed bins. It answers:
"has the shape of this metric's distribution changed?" — not concept drift,
just distribution drift.

Interpretation (industry-standard thresholds):
    PSI < 0.10            -> no significant shift
    0.10 <= PSI < 0.25     -> moderate shift, worth watching
    PSI >= 0.25            -> significant shift, investigate / retrain

Reference: Population Stability Index formula used widely in credit-risk
scorecards; adapted here for telemetry/feature drift on network metrics
(e.g. interface utilization, latency, error rate distributions).
"""
from __future__ import annotations

import bisect
import math
from dataclasses import dataclass, field
from typing import Sequence

_EPS = 1e-6  # avoids log(0) / div-by-0 when a bin is empty


@dataclass
class PSIResult:
    psi: float
    bin_edges: list[float]
    reference_pct: list[float]
    current_pct: list[float]
    verdict: str  # "stable" | "moderate" | "significant"

    def to_dict(self) -> dict:
        return {
            "psi": round(self.psi, 6),
            "verdict": self.verdict,
            "bin_edges": self.bin_edges,
            "reference_pct": self.reference_pct,
            "current_pct": self.current_pct,
        }


@dataclass
class PSIDetector:
    """
    Stateful PSI detector for a single metric stream.

    Usage:
        detector = PSIDetector(num_bins=10)
        detector.fit_reference(historical_values)      # once, or periodically re-baselined
        result = detector.score(current_window_values)  # call per window
    """

    num_bins: int = 10
    _bin_edges: list[float] = field(default_factory=list, init=False)
    _reference_pct: list[float] = field(default_factory=list, init=False)
    _fitted: bool = field(default=False, init=False)

    def fit_reference(self, values: Sequence[float]) -> None:
        if len(values) < self.num_bins:
            raise ValueError(
                f"Need at least {self.num_bins} reference values, got {len(values)}"
            )
        sorted_vals = sorted(values)
        # Quantile-based binning so each reference bin starts with ~equal mass
        edges = [
            sorted_vals[int(q * (len(sorted_vals) - 1))]
            for q in (i / self.num_bins for i in range(self.num_bins + 1))
        ]
        # Guarantee strictly increasing edges (dedupe flat regions)
        edges = sorted(set(edges))
        if len(edges) < 2:
            edges = [min(sorted_vals) - _EPS, max(sorted_vals) + _EPS]
        self._bin_edges = edges
        self._reference_pct = self._bucket_pct(values, edges)
        self._fitted = True

    def score(self, values: Sequence[float]) -> PSIResult:
        if not self._fitted:
            raise RuntimeError("Call fit_reference() before score()")
        if not values:
            raise ValueError("Cannot score an empty window")

        current_pct = self._bucket_pct(values, self._bin_edges)
        psi = 0.0
        for ref_p, cur_p in zip(self._reference_pct, current_pct):
            ref_p = max(ref_p, _EPS)
            cur_p = max(cur_p, _EPS)
            psi += (cur_p - ref_p) * math.log(cur_p / ref_p)

        verdict = "stable" if psi < 0.10 else "moderate" if psi < 0.25 else "significant"
        return PSIResult(
            psi=psi,
            bin_edges=self._bin_edges,
            reference_pct=self._reference_pct,
            current_pct=current_pct,
            verdict=verdict,
        )

    @staticmethod
    def _bucket_pct(values: Sequence[float], edges: list[float]) -> list[float]:
        """Assign each value to bin i where edges[i] <= v < edges[i+1],
        with the final bin closed on the right (v == max edge included)."""
        n_bins = len(edges) - 1
        counts = [0] * n_bins
        n = len(values)
        interior_edges = edges[1:-1]  # bisect against the interior cut points
        for v in values:
            idx = bisect.bisect_right(interior_edges, v)
            idx = min(idx, n_bins - 1)
            counts[idx] += 1
        return [c / n for c in counts]
