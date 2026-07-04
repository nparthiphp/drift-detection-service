from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field


class DetectorType(str, Enum):
    psi = "psi"
    adwin = "adwin"
    page_hinkley = "page_hinkley"


class StreamRegisterRequest(BaseModel):
    stream_id: str = Field(..., description="Unique metric stream key, e.g. 'iface.eth0.util_pct'")
    detectors: list[DetectorType] = Field(
        default=[DetectorType.adwin, DetectorType.page_hinkley],
        description="Which detectors to run for this stream",
    )
    psi_reference_values: list[float] | None = Field(
        default=None, description="Required if 'psi' is in detectors — historical baseline window"
    )
    adwin_delta: float = 0.002
    page_hinkley_threshold: float = 50.0
    page_hinkley_mode: Literal["up", "down", "both"] = "both"


class UpdatePointRequest(BaseModel):
    stream_id: str
    value: float
    timestamp: str | None = Field(default=None, description="ISO8601; server time used if omitted")


class UpdateBatchRequest(BaseModel):
    stream_id: str
    values: list[float]
    timestamp: str | None = None


class DriftSignal(BaseModel):
    detector: DetectorType
    drift_detected: bool
    details: dict


class UpdateResponse(BaseModel):
    stream_id: str
    timestamp: str
    signals: list[DriftSignal]
    any_drift: bool


class StreamStatus(BaseModel):
    stream_id: str
    detectors: list[DetectorType]
    n_updates: dict[str, int]
    n_drifts: dict[str, int]
