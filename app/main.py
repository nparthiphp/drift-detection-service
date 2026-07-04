from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.detectors.manager import StreamManager, now_iso
from app.models import (
    StreamRegisterRequest,
    StreamStatus,
    UpdateBatchRequest,
    UpdatePointRequest,
    UpdateResponse,
)

logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(settings.service_name)

manager = StreamManager(redis_url=settings.redis_url, redis_key_prefix=settings.redis_key_prefix)


@asynccontextmanager
async def lifespan(app: FastAPI):
    manager.restore_all()
    logger.info("drift-detection-service started; restored %d streams", len(manager.list_streams()))
    yield


app = FastAPI(
    title="Drift Detection Service",
    description=(
        "Independent microservice detecting data drift (PSI), concept drift "
        "(ADWIN), and change-points (Page-Hinkley) for streaming AIOps metrics."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    if settings.api_key and x_api_key != settings.api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing API key")


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok", "service": settings.service_name}


@app.get("/readyz")
def readyz() -> dict:
    return {"status": "ready", "streams_registered": len(manager.list_streams())}


@app.post("/streams/register", response_model=StreamStatus, dependencies=[Depends(require_api_key)])
def register_stream(req: StreamRegisterRequest) -> StreamStatus:
    try:
        manager.register(
            stream_id=req.stream_id,
            detector_types=req.detectors,
            psi_reference_values=req.psi_reference_values,
            adwin_delta=req.adwin_delta,
            page_hinkley_threshold=req.page_hinkley_threshold,
            page_hinkley_mode=req.page_hinkley_mode,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return StreamStatus(**manager.status(req.stream_id))


@app.post("/streams/update", response_model=UpdateResponse, dependencies=[Depends(require_api_key)])
def update_point(req: UpdatePointRequest) -> UpdateResponse:
    try:
        signals = manager.update_point(req.stream_id, req.value)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return UpdateResponse(
        stream_id=req.stream_id,
        timestamp=req.timestamp or now_iso(),
        signals=signals,
        any_drift=any(s["drift_detected"] for s in signals),
    )


@app.post("/streams/update-batch-psi", response_model=UpdateResponse, dependencies=[Depends(require_api_key)])
def update_batch_psi(req: UpdateBatchRequest) -> UpdateResponse:
    try:
        signal = manager.update_batch_psi(req.stream_id, req.values)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return UpdateResponse(
        stream_id=req.stream_id,
        timestamp=req.timestamp or now_iso(),
        signals=[signal],
        any_drift=signal["drift_detected"],
    )


@app.get("/streams/{stream_id}/status", response_model=StreamStatus, dependencies=[Depends(require_api_key)])
def stream_status(stream_id: str) -> StreamStatus:
    try:
        return StreamStatus(**manager.status(stream_id))
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/streams", dependencies=[Depends(require_api_key)])
def list_streams() -> dict:
    return {"streams": manager.list_streams()}
