# Backend service

A single FastAPI process: JSON API under `/api`, one WebSocket for live data, a background worker running
jobs one at a time from a sqlite queue, dependency probes, optional token auth, and the built UI served from
`server/static` with SPA fallback. Template: `templates/backend/server/`; tests: `templates/backend/tests/`.

## Contents
1. Layout and responsibilities
2. App factory with injectable dependencies
3. Lifespan: worker, status ticker, shutdown
4. Job store (sqlite)
5. Worker and the job function contract
6. Job kinds registry and validation
7. WebSocket hub (fan-out from threads)
8. Health / dependency probes
9. Pure state derivation with hysteresis
10. Insights on the backend
11. Auth hook
12. SPA fallback and static build output
13. Tests
14. Container image and compose

## 1. Layout

| Module | Responsibility |
|---|---|
| `app.py` | `create_app(...)`, routes, lifespan, SPA fallback; lazy module-level `app` for `uvicorn server.app:app` |
| `config.py` | name, slug, version, `APP_*` env settings |
| `store.py` | sqlite jobs table, JSON columns, interrupted-on-restart marking |
| `worker.py` | `JobWorker` thread, `JobHandle` (steps, progress, metrics, logs, abort) |
| `tasks.py` | job kinds registry (`TASKS`), parameter validation, `rank_insights`, the demo job |
| `hub.py` | WebSocket fan-out + short history ring for backfill |
| `health.py` | probes, parallel runner with timeouts, `ProbeCache` for the ticker |
| `state.py` | pure `component_states(...)` with sticky degraded state |
| `auth.py` | optional token: middleware, login/logout/me, WebSocket check |
| `__main__.py` | `python -m server --host --port --reload` (uvicorn factory) |

Endpoint groups are listed in the module docstring of `app.py` (the reference kept such a map at the top
of its app module; it doubles as documentation and OpenAPI is at `/api/docs`).

## 2. App factory

```python
def create_app(data_dir=None, run_fn=None, probes=None, background=True, static_dir=None) -> FastAPI:
```

Everything with side effects is a parameter, defaulting to the real thing:
- `data_dir`: where the sqlite file lives (tests: `tmp_path`).
- `run_fn(job, handle) -> result`: the job implementation (tests: a fast fake; default: `tasks.run`).
- `probes`: dependency probes (tests: lambdas; default: store, worker, disk + `APP_DEPENDENCIES`).
- `background=False`: no worker thread, no ticker. Tests call `app.state.worker.execute(job)` synchronously.
- `static_dir`: the built UI (tests: a temp dir with an `index.html`).

The reference factory took the same shape (`runs_dir`, `run_fn`, `verify_fn`, a broker on/off flag,
`background`, `publish_fn`), which is what made its API testable without a message broker, a cache or a real run. Store, hub and
worker are attached to `app.state` so tests can reach them. A module `__getattr__` builds the default app on
first access, so importing the module in tests has no side effects.

Errors: raise `HTTPException(status, "words a human can read")`; a handler returns `{"detail": ...}`.
Validate early: never queue a job that cannot start (bad kind → 404, bad params → 400 with the reason).
Ids from the path are checked against a strict regex before touching storage (`ID_RE`), and path traversal
returns 400/404.

## 3. Lifespan

```python
@contextlib.asynccontextmanager
async def lifespan(app):
    hub.bind_loop(asyncio.get_running_loop())   # threads publish via call_soon_threadsafe
    if background:
        worker.recover(); worker.start()
        task = asyncio.create_task(status_ticker())   # one {"type": "status", ...} per second
    yield
    task.cancel(); await asyncio.to_thread(worker.shutdown, grace_s)
```

- The ticker runs blocking work (`worker.status()`, probes) in `asyncio.to_thread` and swallows errors: it
  must keep ticking.
- Shutdown aborts the running job and waits up to `APP_SHUTDOWN_GRACE_S` for a partial result; the
  container's stop grace period (`stop_grace_period`, Kubernetes `terminationGracePeriodSeconds`) must be
  longer. The reference used 60 s with a 75 s compose grace period.

## 4. Job store

- One sqlite connection (`check_same_thread=False`) behind a `threading.Lock`; `executescript(SCHEMA)` at
  start; rows as dicts; JSON columns (`params`, `result`, `steps`) encoded/decoded in one place.
- Status vocabulary: `queued | running | done | failed | aborted | cancelled | interrupted`.
- On startup, rows still `running` are marked `interrupted` with a reason: the previous process died, and
  pretending they finished would be dishonest. `worker.recover()` logs them (the reference also told any
  surviving process to stop and built a partial report from what was on disk).
- Methods: `enqueue`, `job`, `jobs(limit)`, `queued`, `next_queued`, `update(**fields)`, `counts`, `ping`.
- Schedules (reference): a second table with `next_at`, `every_minutes`, `remaining`, `enabled`, and a
  `Scheduler` thread that enqueues due ones every 5 s, skipping missed slots instead of bursting.

## 5. Worker and the job function contract

`JobWorker` loops: take the oldest queued job, `execute(job)`, repeat; `wake` (an `Event`) is set on enqueue
so a new job starts immediately. One job at a time (the reference also enforced this across processes with
a lease and kept the next job queued with a `blocked_by` holder while another process owned the resource).

```python
def my_job(job: dict, h: JobHandle) -> dict:
    h.budget_ms, h.planned_s = 120, 60                       # shown on the live page
    h.define_steps([("prepare", "Prepare inputs", "store"),   # (id, label, component it keeps busy)
                    ("process", "Process items", "worker")])
    h.step("prepare", "running", detail="validating"); ...; h.step("prepare", "done")
    h.phase = "RUN"; h.step("process", "running")
    while not done:
        ...
        h.metric(throughput=..., p50_ms=..., p99_ms=..., errors=...)   # ~1/s -> hub -> WebSocket
        h.step("process", done=n, total=N, unit="items")             # rate + ETA computed for you
        if h.wait(1.0): break                                         # sleeps, wakes on abort
    h.step("process", "done" if not h.aborted else "warn")
    return {"processed": n, ..., "series": [...], "insights": rank_insights(...)}
```

`execute` records `running` + `started_at`, calls `run_fn`, then stores `done` (or `aborted` if an abort
was requested; a partial result is still saved) or `failed` with the error text, always with the final
steps. A failing job never kills the worker. `abort(id)` returns what happened in words the UI can toast:
`abort-signalled`, `cancelled` (was queued), `already-done`, `not-found`. Per-job logs are kept in memory
(2000 lines) and sent to the hub; the detail endpoint returns the tail.

## 6. Job kinds registry

`TASKS = {kind: Task(kind, label, description, params=[Param(name, label, default, unit, min, max, step,
hint)], fn)}`. `GET /api/kinds` publishes it, and the UI generates its "new job" form from it (labels,
units, bounds, hints) and labels stored parameters with it. `Task.validate(params)` fills defaults and
rejects unknown names, non-numbers and out-of-range values with a message. The reference validated whole
configuration documents (YAML) with Pydantic and returned `{"errors": [...]}` details that the client joins into one line.

## 7. WebSocket hub

```python
hub.subscribe() -> asyncio.Queue(maxsize=5000)     # one per client
hub.publish(msg)                                    # from any thread: loop.call_soon_threadsafe(put_nowait)
hub.add_log(job_id, line); hub.add_metric(sample)   # append to history rings + publish
hub.history(seconds, job_id) -> {"metrics": [...], "logs": [...]}   # for /api/live/history
```

Slow clients drop messages (`QueueFull` ignored) instead of blocking others. The endpoint authorises, sends
a `status` immediately, then forwards its queue; it unsubscribes in `finally`. The reference hub also
consumed message-broker topics in daemon threads (retrying every 5 s when the broker was down, exposing
`connected`/`last_error`), rate-limited a live event feed to ~20 messages/s, and tracked when each source
last reported (`silence()`), which fed the state function ("no metrics from X for 7 s" → down).

## 8. Health probes

- A probe is `Probe(id, name, kind, fn, url)`; `fn()` returns `{"status": "up|degraded|down|starting",
  "version": ..., "details": {...}}` or raises (→ `down` with `details.error`).
- `timed()` measures latency and never raises; `run_probes()` runs all probes in a thread pool with a short
  timeout (`APP_PROBE_TIMEOUT_S`, 2.5 s), so a dead dependency shows `down` in seconds instead of hanging
  the page. Tested: an unreachable URL is down in under 3 s.
- Give reasons in words in `details.reason` ("only 0.6% free (1.5 GB)"): a bare `down` reads as
  "unreachable" (found in validation: the disk tile said "unreachable" when the disk was full).
- Name details with unit suffixes (`size_bytes`, `free_pct`, `latency_ms`); the UI formats them.
- `GET /api/env` → `{checked_at, components: [...]}`; `GET /api/health` stays cheap (no dependency calls)
  and open without a token, for container health checks.
- `ProbeCache` refreshes at most every 5 s for the 1 s status ticker, setting its timestamp before probing
  so concurrent ticks reuse the old value. (The reference ran probes in a background thread, with the
  expensive ones only while a run was active.)

## 9. Pure state derivation

```python
component_states(components, steps, now, sticky) -> {id: {"state", "reason", "progress"}}
```

No I/O, `now` passed in, the only memory (`sticky`: last time each component was degraded) owned by the
caller. Precedence: probe `down` → `down`; probe `starting` → `starting`; a running step that names the
component → `warming` with the step's progress and reason; slow (> `SLOW_MS`) or `degraded` probe →
`degraded` (remembered); within `STICKY_S` of the last bad probe → `degraded` ("recovering …");
otherwise `ready`. The reference's `node_states()` followed the same idea with a richer context (run phase,
startup steps read from disk, probes, seconds since each metrics source last reported) so that a pipeline
that was starting or warming was never painted red, and it mapped nodes to components by kind so new
topologies got states without code changes. Test each precedence rule with plain dicts.

## 10. Insights on the backend

`rank_insights(result, steps)` is pure: it turns numbers into findings with `severity`, `title` (with the
number), `detail` (evidence), `action` (next step), `area`, `anchor` (the report section holding the
evidence), sorted critical → warning → info → good. Keep thresholds in one place (the reference loaded
them, and cost assumptions, from a config file the UI could override inline).

## 11. Auth hook

`APP_TOKEN` unset → open (local use). Set → every `/api/*` except `/api/health` and the auth routes needs
`Authorization: Bearer <token>` or the session cookie; WebSockets also accept `?token=` and are closed with
code 4401 otherwise. Login compares with `secrets.compare_digest` and sets an HttpOnly, SameSite=Strict,
12 h cookie whose value is `HMAC(token, label)`, never the token. It is a gate, not identity: put SSO at the
ingress for anything internet-facing.

## 12. SPA fallback and static build output

Vite builds into `backend/server/static` (dev) or `/ui/dist` (Docker, `VITE_OUT_DIR`). If `static/index.html`
exists, `app.py` mounts `/assets` (hashed, cacheable) and adds a catch-all route: paths under `api/` stay
404; an existing file inside `static` is served (resolved path must stay inside `static`); everything else
returns `index.html` with `Cache-Control: no-cache` so deep links (`/jobs/<id>`) work. Without a build, `/`
explains how to build the UI. The catch-all is registered last, after every API route.

## 13. Tests

`tests/test_api.py` (TestClient, no threads, no network) covers: health, kinds, env with fake probes; job
lifecycle (404 kind, 400 params with the reason, defaults filled, queue, synchronous execute, detail with
logs, light list rows, summary, live history, bad ids); abort queued → cancelled → already-cancelled; a
failing job → `failed` with the error; the real demo job end to end with a 10 ms tick; WebSocket first
`status` then a broadcast from another thread; token auth (401, cookie, Bearer, logout, WebSocket refused);
SPA fallback; an unreachable dependency is `down` quickly. `tests/test_state.py` covers each state rule, the
sticky window, the progress wording shared with the UI, and insight ranking.

Run: `cd backend && python -m pytest -q` (`pythonpath = ["."]` is set in `pyproject.toml`).

## 14. Container image and compose

`templates/Dockerfile` is multi-stage, like the reference console target: a Node stage runs `npm ci` (or
`npm install` without a lockfile) and `VITE_OUT_DIR=/ui/dist npm run build` (which type-checks first); the
Python stage installs the backend (`pip install -e .`), copies `/ui/dist` into `server/static`, exposes
8090, declares a `/data` volume, and health-checks `/api/health` with the standard library. Compose runs it
with `APP_TOKEN`, `APP_DEPENDENCIES`, `APP_SHUTDOWN_GRACE_S` and a longer `stop_grace_period`. The reference
also passed proxy CA certificates into both build stages through a build secret; add that if builds run
behind a TLS-intercepting proxy.
