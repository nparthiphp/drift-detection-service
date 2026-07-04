import random

import pytest
from fastapi.testclient import TestClient

from app.main import app, manager

client = TestClient(app)


@pytest.fixture(autouse=True)
def _clear_streams():
    manager._streams.clear()
    yield
    manager._streams.clear()


def test_healthz():
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_register_and_update_adwin_stream():
    r = client.post(
        "/streams/register",
        json={"stream_id": "test.stream", "detectors": ["adwin"]},
    )
    assert r.status_code == 200
    assert r.json()["stream_id"] == "test.stream"

    r = client.post("/streams/update", json={"stream_id": "test.stream", "value": 1.0})
    assert r.status_code == 200
    body = r.json()
    assert body["stream_id"] == "test.stream"
    assert len(body["signals"]) == 1
    assert body["signals"][0]["detector"] == "adwin"


def test_update_on_unregistered_stream_returns_404():
    r = client.post("/streams/update", json={"stream_id": "nope", "value": 1.0})
    assert r.status_code == 404


def test_register_psi_without_reference_returns_400():
    r = client.post(
        "/streams/register",
        json={"stream_id": "psi.stream", "detectors": ["psi"]},
    )
    assert r.status_code == 400


def test_full_psi_flow():
    random.seed(20)
    ref = [random.gauss(0, 1) for _ in range(500)]
    r = client.post(
        "/streams/register",
        json={"stream_id": "psi.stream", "detectors": ["psi"], "psi_reference_values": ref},
    )
    assert r.status_code == 200

    stable = [random.gauss(0, 1) for _ in range(100)]
    r = client.post(
        "/streams/update-batch-psi", json={"stream_id": "psi.stream", "values": stable}
    )
    assert r.status_code == 200
    assert r.json()["any_drift"] is False

    shifted = [random.gauss(3, 1) for _ in range(100)]
    r = client.post(
        "/streams/update-batch-psi", json={"stream_id": "psi.stream", "values": shifted}
    )
    assert r.json()["any_drift"] is True


def test_stream_status_and_listing():
    client.post("/streams/register", json={"stream_id": "s1", "detectors": ["adwin"]})
    client.post("/streams/register", json={"stream_id": "s2", "detectors": ["page_hinkley"]})

    r = client.get("/streams")
    assert set(r.json()["streams"]) == {"s1", "s2"}

    r = client.get("/streams/s1/status")
    assert r.json()["stream_id"] == "s1"


def test_api_key_enforced_when_configured(monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "api_key", "secret123")
    r = client.post("/streams/register", json={"stream_id": "x", "detectors": ["adwin"]})
    assert r.status_code == 401

    r = client.post(
        "/streams/register",
        json={"stream_id": "x", "detectors": ["adwin"]},
        headers={"x-api-key": "secret123"},
    )
    assert r.status_code == 200
    monkeypatch.setattr(settings, "api_key", None)
