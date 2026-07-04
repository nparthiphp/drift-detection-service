# Architecture

![Drift detection service workflow](./docs/workflow-diagram.png)

## Request flow

```
                 Metric stream (anomaly-service, reduction-funnel, CMDB)
                                     │
                                     ▼
┌───────────────────────────────────────────────────────────────────────┐
│  drift-detection-service                                              │
│                                                                       │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │  FastAPI layer                                               │   │
│   │  POST /streams/register                                      │   │
│   │  POST /streams/update              (point-by-point)          │   │
│   │  POST /streams/update-batch-psi    (batch)                   │   │
│   │  GET  /streams/{id}/status                                   │   │
│   └───────────────────────────┬───────────────────────────────────┘   │
│                                │                                       │
│                                ▼                                       │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │  StreamManager  (one instance per stream_id)                 │   │
│   │                                                               │   │
│   │   ┌───────────┐    ┌───────────┐    ┌─────────────────┐     │   │
│   │   │    PSI    │    │   ADWIN   │    │  Page-Hinkley    │     │   │
│   │   │ batch drift│    │concept drift│  │  change-point    │     │   │
│   │   └───────────┘    └───────────┘    └─────────────────┘     │   │
│   └───────────────────────────┬───────────────────────────────────┘   │
│                                │ (best-effort, async)                 │
│                                ▼                                       │
│   ┌─────────────────────────────────────────┐                        │
│   │  Redis persistence (optional)            │                        │
│   │  pickle snapshot per stream, warm cache  │                        │
│   └───────────────────────────────────────────┘                       │
└───────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
                       Drift signal → Reason stage / alerting
```

## Component responsibilities

### FastAPI layer (`app/main.py`)
Thin HTTP boundary. No detection logic lives here — its job is request
validation (Pydantic schemas in `app/models.py`), auth (`x-api-key` header
if `DRIFT_API_KEY` is set), and routing to `StreamManager`. Two write paths
exist because PSI and the streaming detectors have fundamentally different
calling conventions: PSI needs a batch to compare against a reference, ADWIN
and Page-Hinkley take one value at a time.

### StreamManager (`app/detectors/manager.py`)
Owns exactly one `StreamState` per `stream_id`. This is the only place that
knows which detectors are active for a given metric — registering a stream
is where you choose PSI, ADWIN, Page-Hinkley, or any combination. Update
calls are routed here and fanned out to whichever detectors are registered;
each detector's result comes back independently in the `signals` array
rather than being collapsed into a single verdict, so a downstream Reason
stage can see *which* detector fired and weight that accordingly (a
Page-Hinkley fire — a sustained change-point — usually warrants more
confidence than a single noisy ADWIN blip).

### Detectors (`app/detectors/{psi,adwin,page_hinkley}.py`)
Each is a stateful wrapper around either a hand-rolled implementation (PSI)
or `river`'s streaming implementation (ADWIN, Page-Hinkley). They are
intentionally dumb — no I/O, no persistence, no knowledge of HTTP. This
keeps them independently unit-testable (see `tests/test_detectors.py`) and
means the detection logic could be lifted into a batch job or a Flink UDF
without touching the API layer.

### Redis persistence (optional)
State persistence is a deliberate weak point, called out here rather than
buried in code comments: `river`'s ADWIN/Page-Hinkley objects aren't cleanly
JSON-serializable, so persistence uses `pickle` and is best-effort — a
failed write is swallowed rather than raised, because a Redis hiccup should
never break the hot path of scoring a metric. This is fine for a single
replica that occasionally restarts (state is restored via `restore_all()`
on startup) but is **not** a source of truth and **not** safe for multiple
replicas writing the same stream concurrently — last-write-wins, no locking.
If you need multi-replica correctness, that's the next real piece of work
here, not a Redis config flag.

## Why point-by-point and batch coexist

This is the one design decision worth defending explicitly: PSI is
fundamentally a *distributional* comparison and needs a window of values to
even define a distribution, while ADWIN and Page-Hinkley are online
algorithms built to consume one value at a time and maintain their own
internal state. Forcing PSI into a point-by-point interface would mean
buffering internally and hiding that from the caller; forcing ADWIN into a
batch interface would throw away the incremental state that makes it cheap.
Two endpoints, matched to two calling conventions, is simpler than one
endpoint with hidden buffering.

## Where this sits in the ARIES closed loop

`drift-detection-service` is a **Detect**-stage component. It never calls
downstream services itself — it emits an `any_drift` boolean and per-detector
detail, and it's the caller's job (the correlation service, or eventually
the LangGraph orchestrator) to decide what "drift" means for that specific
metric. Keeping this service opinion-free about consequences is what makes
it independently deployable and testable, per the original design goal.
