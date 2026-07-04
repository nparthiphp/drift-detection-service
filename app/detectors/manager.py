"""
StreamManager owns one set of detectors per metric stream (stream_id) and
routes incoming values to each registered detector.

Design note on state persistence:
ADWIN and Page-Hinkley are stateful objects (they hold internal windows /
cumulative sums). river's detectors are NOT natively JSON-serializable in a
simple way, so for a single-replica deployment we keep them in-process
(fast, simple). If you need to run multiple replicas or survive restarts
without losing detector state, the clean path is river's built-in
`clone()`/pickle support — see `snapshot()`/`restore()` below, which use
pickle + Redis rather than hand-rolled JSON.
"""
from __future__ import annotations

import pickle
from dataclasses import dataclass, field
from datetime import datetime, timezone

from app.detectors.adwin import ADWINDetector
from app.detectors.page_hinkley import PageHinkleyDetector
from app.detectors.psi import PSIDetector
from app.models import DetectorType

try:
    import redis as _redis
except ImportError:  # redis client is optional at runtime
    _redis = None


@dataclass
class StreamState:
    stream_id: str
    detector_types: list[DetectorType]
    adwin: ADWINDetector | None = None
    page_hinkley: PageHinkleyDetector | None = None
    psi: PSIDetector | None = None


class StreamManager:
    def __init__(self, redis_url: str | None = None, redis_key_prefix: str = "drift:"):
        self._streams: dict[str, StreamState] = {}
        self._redis = None
        self._redis_prefix = redis_key_prefix
        if redis_url and _redis is not None:
            self._redis = _redis.from_url(redis_url)

    def register(
        self,
        stream_id: str,
        detector_types: list[DetectorType],
        psi_reference_values: list[float] | None = None,
        adwin_delta: float = 0.002,
        page_hinkley_threshold: float = 50.0,
        page_hinkley_mode: str = "both",
    ) -> StreamState:
        state = StreamState(stream_id=stream_id, detector_types=detector_types)
        if DetectorType.adwin in detector_types:
            state.adwin = ADWINDetector(delta=adwin_delta)
        if DetectorType.page_hinkley in detector_types:
            state.page_hinkley = PageHinkleyDetector(
                threshold=page_hinkley_threshold, mode=page_hinkley_mode
            )
        if DetectorType.psi in detector_types:
            if not psi_reference_values:
                raise ValueError("psi_reference_values required when registering a 'psi' detector")
            state.psi = PSIDetector()
            state.psi.fit_reference(psi_reference_values)
        self._streams[stream_id] = state
        self._persist(state)
        return state

    def get(self, stream_id: str) -> StreamState:
        if stream_id not in self._streams:
            raise KeyError(f"Unknown stream_id '{stream_id}'. Register it first via /streams/register")
        return self._streams[stream_id]

    def update_point(self, stream_id: str, value: float) -> list[dict]:
        state = self.get(stream_id)
        signals: list[dict] = []
        if state.adwin is not None:
            r = state.adwin.update(value)
            signals.append(
                {
                    "detector": DetectorType.adwin,
                    "drift_detected": r.drift_detected,
                    "details": {"estimation": r.estimation, "width": r.width},
                }
            )
        if state.page_hinkley is not None:
            r = state.page_hinkley.update(value)
            signals.append(
                {
                    "detector": DetectorType.page_hinkley,
                    "drift_detected": r.drift_detected,
                    "details": {"n_updates": r.n_updates},
                }
            )
        self._persist(state)
        return signals

    def update_batch_psi(self, stream_id: str, values: list[float]) -> dict:
        state = self.get(stream_id)
        if state.psi is None:
            raise ValueError(f"Stream '{stream_id}' has no PSI detector registered")
        result = state.psi.score(values)
        return {
            "detector": DetectorType.psi,
            "drift_detected": result.verdict != "stable",
            "details": result.to_dict(),
        }

    def status(self, stream_id: str) -> dict:
        state = self.get(stream_id)
        n_updates, n_drifts = {}, {}
        if state.adwin is not None:
            n_updates["adwin"] = state.adwin.n_updates
            n_drifts["adwin"] = state.adwin.n_drifts
        if state.page_hinkley is not None:
            n_updates["page_hinkley"] = state.page_hinkley.n_updates
            n_drifts["page_hinkley"] = state.page_hinkley.n_drifts
        return {
            "stream_id": stream_id,
            "detectors": state.detector_types,
            "n_updates": n_updates,
            "n_drifts": n_drifts,
        }

    def list_streams(self) -> list[str]:
        return list(self._streams.keys())

    # -- optional Redis persistence (pickle-based snapshot, not the source of truth) --
    def _persist(self, state: StreamState) -> None:
        if self._redis is None:
            return
        key = f"{self._redis_prefix}{state.stream_id}"
        try:
            self._redis.set(key, pickle.dumps(state))
        except Exception:
            # Persistence is best-effort; never let a Redis hiccup break the hot path
            pass

    def restore_all(self) -> None:
        if self._redis is None:
            return
        for key in self._redis.scan_iter(f"{self._redis_prefix}*"):
            raw = self._redis.get(key)
            if raw:
                state: StreamState = pickle.loads(raw)
                self._streams[state.stream_id] = state


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
