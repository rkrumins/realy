"""PulseBench operator console — FastAPI backend.

    uvicorn realy.control.app:app --port 8090        (or: pulsebench console)

API (JSON):
  scenarios   GET /api/scenarios · GET|PUT /api/scenarios/{name} · POST /api/scenarios/validate
              POST /api/scenarios/{name}/budget
  runs        POST /api/runs · GET /api/runs · GET /api/runs/{id} · GET /api/runs/{id}/report.html
              POST /api/runs/{id}/abort · POST /api/runs/abort (bulk) · POST /api/stop-all
              GET /api/active · POST /api/queue/clear · GET /api/compare?a=&b=
  live        WS /api/live · GET /api/live/history · GET /api/status
              POST /api/live/rate · POST /api/live/scoring · POST /api/live/model
  schedules   POST /api/schedules · GET /api/schedules · DELETE /api/schedules/{id}
              POST /api/schedules/{id}/pause|resume · POST /api/schedules/pause-all|resume-all
  overview    GET /api/summary · GET /api/health
  auth        GET /api/auth/me · POST /api/auth/login · POST /api/auth/logout   (REALY_CONSOLE_TOKEN)
  env         GET /api/env · GET /api/env/{kafka,flink,redis,clickhouse}
  explorer    GET /api/runs/{id}/search · /decisions/{event_id} · /customers/{cid} · /campaigns[/{cid}]
  topology    GET /api/topology?topology=
  docs        GET /api/docs · GET /api/docs/page?path= · GET /api/docs/raw/{path} · GET /api/reference/scenario-schema
  openapi     GET /api/openapi.json · Swagger UI at /api/swagger
The built UI (ui/ -> realy/control/static) is served at / with SPA deep-link fallback.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import yaml
from fastapi import Body, FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, ValidationError

from ..scenario import SCENARIO_DIR, Scenario, deep_merge
from . import auth as auth_mod
from . import docs as docs_mod
from . import env as env_mod
from . import topology as topology_mod
from .explorer import make_router as explorer_router
from .store import Store
from .telemetry import Hub
from .worker import RunWorker, Scheduler

STATIC = Path(__file__).parent / "static"
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,80}$")


class RunRequest(BaseModel):
    scenario: str
    overrides: dict[str, Any] = Field(default_factory=dict)
    verify: bool = False


class ScheduleRequest(BaseModel):
    scenario: str
    overrides: dict[str, Any] = Field(default_factory=dict)
    verify: bool = False
    at: str | None = None            # ISO-8601; default now
    every_minutes: float | None = None
    count: int | None = None         # number of runs (default 1 for one-shot, unlimited for repeating)


class BulkAbortRequest(BaseModel):
    ids: list[str] = Field(default_factory=list)     # job ids or run ids (this console's or any other's)
    all: bool = False                                 # the running run + (include_queued) the whole queue
    include_queued: bool = True
    reason: str = "aborted by operator"


class StopAllRequest(BaseModel):
    reason: str = "stop all (operator)"
    pause_schedules: bool = True


class RateRequest(BaseModel):
    tps: float = Field(gt=0)


class ScenarioText(BaseModel):
    yaml: str


def _json(p: Path) -> dict[str, Any] | None:
    try:
        return json.loads(p.read_text())
    except (OSError, ValueError):
        return None


def create_app(runs_dir: Path | None = None, scenario_dir: Path | None = None, run_fn: Callable | None = None,
               verify_fn: Callable | None = None, kafka: bool = True, background: bool = True,
               publish_fn: Callable | None = None) -> FastAPI:
    if runs_dir is None:
        from ..runner.run import RUNS_DIR
        runs_dir = RUNS_DIR
    runs_dir = Path(runs_dir)
    scenario_dir = Path(scenario_dir or SCENARIO_DIR)
    runs_dir.mkdir(parents=True, exist_ok=True)
    store = Store(runs_dir / "console.db")
    hub = Hub(kafka=kafka)

    def publish(msgs: list) -> None:
        if publish_fn is not None:
            publish_fn(msgs)
            return
        from ..common import control
        control.publish(msgs)

    def remote_abort(run_id: str, reason: str) -> dict[str, Any]:
        """Abort a run in ANY process: Redis flag (polled by the run's lease heartbeat) and a
        bench.control message (its metrics monitor). Either channel alone is enough."""
        out: dict[str, Any] = {"redis": False, "kafka": False}
        if kafka:   # tests run without infrastructure
            from ..runner.lifecycle import request_abort
            out["redis"] = request_abort(run_id, reason, publish=False)["redis"]
        try:
            from ..common.control import control_message
            publish([control_message(run_id, "*", phase=6)])
            out["kafka"] = True
        except Exception as e:  # noqa: BLE001
            out["kafka_error"] = str(e)[:200]
        return out

    worker = RunWorker(store, runs_dir, scenario_dir, run_fn=run_fn, verify_fn=verify_fn, log_sink=hub.add_log,
                       remote_abort=remote_abort)
    scheduler = Scheduler(store, worker)
    summary_cache: dict[str, tuple[float, dict]] = {}

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        hub.bind_loop(asyncio.get_running_loop())
        task = None
        if background:
            hub.start()
            recovered = worker.recover()
            if recovered:
                hub.add_log("", f"[console] recovered {len(recovered)} run(s) interrupted by a console restart: "
                                + ", ".join(recovered))
            worker.start()
            scheduler.start()

            async def status_ticker() -> None:
                while True:
                    await asyncio.sleep(1.0)
                    lease = await asyncio.to_thread(_lease_holder)
                    hub.publish({"type": "status", **worker.status(), "kafka": hub.connected, "lease": lease})
            task = asyncio.create_task(status_ticker())
        yield
        if task:
            task.cancel()
        hub.shutdown()
        worker.shutdown()
        scheduler.shutdown()

    app = FastAPI(title="PulseBench console", lifespan=lifespan, openapi_url="/api/openapi.json",
                  docs_url="/api/swagger", redoc_url=None,
                  description="Operator console API: launch and steer benchmark runs, live telemetry, reports, "
                              "explorer, environment and docs.")
    app.state.store, app.state.hub, app.state.worker, app.state.scheduler = store, hub, worker, scheduler
    app.add_middleware(auth_mod.AuthMiddleware)
    for r in (auth_mod.router, env_mod.router, topology_mod.router, docs_mod.router, explorer_router(runs_dir)):
        app.include_router(r)

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        return {"ok": True, "kafka": hub.connected}

    # ================================================================== scenarios
    def _scenario_path(name: str) -> Path:
        if not NAME_RE.match(name):
            raise HTTPException(400, "invalid scenario name")
        return scenario_dir / f"{name}.yaml"

    def _validate(text: str, name: str | None = None) -> Scenario:
        try:
            data = yaml.safe_load(text) or {}
        except yaml.YAMLError as e:
            raise HTTPException(400, f"YAML error: {e}") from e
        if not isinstance(data, dict):
            raise HTTPException(400, "scenario must be a YAML mapping")
        if name is not None:
            data.setdefault("name", name)
            if data["name"] != name:
                raise HTTPException(400, f"scenario name '{data['name']}' must match the file name '{name}'")
        try:
            return Scenario.model_validate(data)
        except ValidationError as e:
            raise HTTPException(400, {"errors": json.loads(e.json())}) from e

    @app.get("/api/scenarios")
    def list_scenarios() -> list[dict[str, Any]]:
        out = []
        for p in sorted(scenario_dir.glob("*.yaml")):
            text = p.read_text()
            try:
                d = yaml.safe_load(text) or {}
            except yaml.YAMLError:
                d = {}
            out.append({"name": p.stem, "topology": d.get("topology"), "description": d.get("description", ""),
                        "tps": ((d.get("traffic") or {}).get("profile") or {}).get("tps"),
                        "duration_s": (d.get("traffic") or {}).get("duration_s"), "yaml": text})
        return out

    @app.get("/api/scenarios/{name}")
    def get_scenario(name: str) -> dict[str, Any]:
        p = _scenario_path(name)
        if not p.exists():
            raise HTTPException(404, "no such scenario")
        text = p.read_text()
        return {"name": name, "yaml": text, "scenario": _validate(text).model_dump(mode="json")}

    @app.post("/api/scenarios/validate")
    def validate_scenario(body: ScenarioText) -> dict[str, Any]:
        sc = _validate(body.yaml)
        return {"ok": True, "scenario": sc.model_dump(mode="json"), "fingerprint": sc.fingerprint()}

    @app.put("/api/scenarios/{name}")
    def put_scenario(name: str, body: ScenarioText) -> dict[str, Any]:
        sc = _validate(body.yaml, name)
        _scenario_path(name).write_text(body.yaml if body.yaml.endswith("\n") else body.yaml + "\n")
        return {"ok": True, "name": name, "scenario": sc.model_dump(mode="json")}

    @app.post("/api/scenarios/{name}/budget")
    def budget(name: str, body: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
        from ..budget.check import check
        body = body or {}
        if body.get("yaml"):
            sc = _validate(body["yaml"])
        else:
            p = _scenario_path(name)
            if not p.exists():
                raise HTTPException(404, "no such scenario")
            data = deep_merge(yaml.safe_load(p.read_text()), body.get("overrides") or {})
            sc = _validate(yaml.safe_dump(data))
        return check(sc, n=int(body.get("samples", 100_000)))

    # ================================================================== runs
    def _summary(run_dir: Path) -> dict[str, Any]:
        from ..report.compare import run_summary
        rep_p = run_dir / "report.json"
        key = str(run_dir)
        mt = max((p.stat().st_mtime for p in (rep_p, run_dir / "verify.json", run_dir / "run.json",
                                               run_dir / "manifest.json") if p.exists()), default=0)
        hit = summary_cache.get(key)
        if hit and hit[0] == mt:
            return hit[1]
        out = run_summary(run_dir)
        summary_cache[key] = (mt, out)
        return out

    def _run_dir(run_id: str) -> Path:
        if not NAME_RE.match(run_id):
            raise HTTPException(400, "invalid run id")
        d = runs_dir / run_id
        if not d.is_dir():
            raise HTTPException(404, "no such run")
        return d

    @app.post("/api/runs")
    def start_run(body: RunRequest) -> dict[str, Any]:
        p = _scenario_path(body.scenario)
        if not p.exists():
            raise HTTPException(404, "no such scenario")
        data = deep_merge(yaml.safe_load(p.read_text()), body.overrides)
        _validate(yaml.safe_dump(data))  # fail fast on bad overrides
        job = store.enqueue(body.scenario, body.overrides, body.verify)
        worker.wake.set()
        return job

    @app.get("/api/runs")
    def list_runs(limit: int = 100) -> dict[str, Any]:
        dirs = sorted((d for d in runs_dir.iterdir() if d.is_dir() and (d / "manifest.json").exists()),
                      key=lambda d: d.stat().st_mtime, reverse=True)[:limit]
        history = [_summary(d) for d in dirs]
        jobs = store.jobs(limit)
        by_run = {j["run_id"]: j for j in jobs if j.get("run_id")}
        for h in history:
            j = by_run.get(h["run_id"])
            if j:
                h["job_id"] = j["id"]
                h["status"] = j["status"] if j["status"] in ("running", "aborted", "failed") else h["status"]
                h["abort_reason"] = h["abort_reason"] or j.get("abort_reason")
                h["verify_requested"] = j["verify"]
        st = worker.status()
        cur = st["current"]
        if cur:
            for h in history:
                if h["run_id"] == cur["run_id"]:
                    h["status"] = "running"
                    h["phase"] = cur["phase"]
        return {"queue": store.queued(), "jobs": jobs[:50], "history": history, "current": cur,
                "blocked_by": st["blocked_by"], "lease": _lease_holder()}

    @app.get("/api/summary")
    def summary(limit: int = 12) -> dict[str, Any]:
        dirs = sorted((d for d in runs_dir.iterdir() if d.is_dir() and (d / "manifest.json").exists()),
                      key=lambda d: d.stat().st_mtime, reverse=True)
        rows = [_summary(d) for d in dirs]
        done = [r for r in rows if r.get("verdict")]
        return {"runs_total": len(rows), "runs_pass": sum(r["verdict"] == "PASS" for r in done),
                "runs_fail": sum(r["verdict"] == "FAIL" for r in done),
                "events_total": sum(r.get("emitted") or 0 for r in done),
                "decisions_total": sum(r.get("decided") or 0 for r in done),
                "last_runs": [{**r, "started": r.get("started_at"), "achieved_emit_tps": r.get("achieved_tps")}
                              for r in rows[:limit]],
                "current": worker.status()["current"]}

    @app.get("/api/runs/{run_id}")
    def get_run(run_id: str) -> dict[str, Any]:
        d = _run_dir(run_id)
        rep = _json(d / "report.json")
        if rep:
            rep.pop("text", None)
        return {"run_id": run_id, "summary": _summary(d), "manifest": _json(d / "manifest.json"), "report": rep,
                "verify": _json(d / "verify.json"), "run": _json(d / "run.json"), "scenario": _json(d / "scenario.json"),
                "logs": list(worker.logs.get(run_id, []))[-500:]}

    @app.get("/api/runs/{run_id}/report.html")
    def get_report_html(run_id: str):
        p = _run_dir(run_id) / "report.html"
        if not p.exists():
            raise HTTPException(404, "report not built yet")
        return FileResponse(p, media_type="text/html")

    lease_cache: dict[str, Any] = {"at": 0.0, "value": None}

    def _lease_holder() -> dict[str, Any] | None:
        """Cached for 2 s: /api/status and /api/runs poll every second and must stay fast when Redis is slow."""
        if not kafka:
            return None
        now = time.time()
        if now - lease_cache["at"] < 2.0:
            return lease_cache["value"]
        from ..runner.lifecycle import holder
        lease_cache["at"] = now          # set first: concurrent requests during a slow lookup reuse the old value
        lease_cache["value"] = holder()
        return lease_cache["value"]

    def _abort_one(ident: str, reason: str) -> dict[str, Any]:
        result = worker.abort(ident, reason)
        out: dict[str, Any] = {"result": result}
        if result == "not-running-here":
            # not this console's: a run in another process (CLI, bench Job, another console)
            job = store.job(ident)
            run_id = (job or {}).get("run_id") or ident
            ch = remote_abort(run_id, reason)
            out["channels"] = ch
            if not (ch.get("redis") or ch.get("kafka")):
                out["result"] = "unreachable"
        return out

    @app.post("/api/runs/abort")
    def abort_many(body: BulkAbortRequest) -> dict[str, Any]:
        """Abort several runs at once: explicit ids, and/or `all` (running + queued). With `all`, a run
        another process holds the pipeline lease for is aborted too."""
        if not body.ids and not body.all:
            raise HTTPException(400, "give ids or all=true")
        res = {k: {"result": v} for k, v in worker.abort_many([], body.all, body.include_queued, body.reason).items()}
        for ident in body.ids:
            if ident not in res:
                res[ident] = _abort_one(ident, body.reason)
        if body.all:
            h = _lease_holder()
            if h and h.get("run_id") and h["run_id"] not in res:
                res[h["run_id"]] = {"result": "remote-abort", "channels": remote_abort(h["run_id"], body.reason)}
        return {"results": res, "count": len(res)}

    @app.post("/api/stop-all")
    def stop_all(body: StopAllRequest | None = None) -> dict[str, Any]:
        """Everything stops: schedules paused (so nothing new is enqueued), queue cancelled, the running
        run aborted, and a wildcard abort on bench.control reaches any run in any other process."""
        body = body or StopAllRequest()
        paused = store.set_schedules_enabled(False) if body.pause_schedules else []
        res = {k: {"result": v} for k, v in worker.abort_many([], True, True, body.reason).items()}
        h = _lease_holder()
        if h and h.get("run_id") and h["run_id"] not in res:
            res[h["run_id"]] = {"result": "remote-abort", "channels": remote_abort(h["run_id"], body.reason)}
        try:
            from ..common.control import control_message
            publish([control_message("", "*", phase=6)])
            wildcard = True
        except Exception:  # noqa: BLE001
            wildcard = False
        hub.add_log(_current_run_id(), f"[console] STOP ALL: {len(res)} run(s)/job(s), {len(paused)} schedule(s) paused")
        return {"results": res, "schedules_paused": paused, "wildcard_published": wildcard}

    @app.post("/api/queue/clear")
    def clear_queue(reason: str = "queue cleared by operator") -> dict[str, Any]:
        out = {j["id"]: worker.abort(j["id"], reason) for j in store.queued()}
        return {"cancelled": [k for k, v in out.items() if v == "cancelled"]}

    @app.get("/api/active")
    def active_runs() -> dict[str, Any]:
        """Every run in flight that this console can see: its own, the pipeline lease holder (any
        process), and run ids with fresh live metrics on bench.metrics."""
        st = worker.status()
        seen = hub.active_runs() if hasattr(hub, "active_runs") else {}
        return {"local": st["current"], "queue": st["queue"], "blocked_by": st["blocked_by"],
                "lease": _lease_holder(), "live": seen}

    @app.post("/api/runs/{ident}/abort")
    def abort_run(ident: str, reason: str = "aborted by operator") -> dict[str, Any]:
        return _abort_one(ident, reason)

    @app.get("/api/compare")
    def compare(a: str, b: str) -> dict[str, Any]:
        from ..report.compare import compare_reports
        ra, rb = _json(_run_dir(a) / "report.json") or {}, _json(_run_dir(b) / "report.json") or {}
        return {"a": _summary(_run_dir(a)), "b": _summary(_run_dir(b)), "stages": compare_reports(ra, rb),
                "levels": {"a": ra.get("levels", []), "b": rb.get("levels", [])},
                "detection": {"a": ra.get("detection", {}), "b": rb.get("detection", {})}}

    # ================================================================== live control
    def _current_run_id() -> str:
        cur = worker.status()["current"]
        return (cur or {}).get("run_id") or ""

    def _admin_put(base_env: str, default_base: str, body: dict) -> str:
        """Immediate effect on a single instance (bench.control is the scalable path)."""
        import httpx
        url = os.environ.get(base_env, default_base).rstrip("/") + "/admin/config"
        try:
            r = httpx.put(url, content=json.dumps(body), timeout=2.0)
            return "applied" if r.status_code < 300 else f"rejected ({r.status_code}: {r.text[:200]})"
        except Exception as e:  # noqa: BLE001
            return f"unreachable ({e.__class__.__name__})"

    @app.post("/api/live/rate")
    def live_rate(body: RateRequest) -> dict[str, Any]:
        from ..common.control import control_message
        run_id = _current_run_id()
        publish([control_message(run_id, "emitter", rate={"tps": body.tps})])
        hub.add_log(run_id, f"[console] live rate change requested: {body.tps:.0f} TPS")
        return {"published": True, "run_id": run_id, "tps": body.tps}

    @app.post("/api/live/scoring")
    def live_scoring(body: dict[str, Any] = Body(...)) -> dict[str, Any]:
        from ..common.control import control_message
        run_id = _current_run_id()
        try:
            msg = control_message(run_id, "*", scoring=body)
        except Exception as e:  # noqa: BLE001
            raise HTTPException(400, f"invalid ScoringConfig: {e}") from e
        publish([msg])
        admin = _admin_put("GATEWAY_ADMIN", "http://localhost:8080", body)
        hub.add_log(run_id, f"[console] live scoring change {json.dumps(body)} (gateway admin: {admin})")
        return {"published": True, "run_id": run_id, "gateway_admin": admin}

    @app.post("/api/live/model")
    def live_model(body: dict[str, Any] = Body(...)) -> dict[str, Any]:
        from ..common.control import control_message
        run_id = _current_run_id()
        try:
            msg = control_message(run_id, "model", model_server=body)
        except Exception as e:  # noqa: BLE001
            raise HTTPException(400, f"invalid ModelServerConfig: {e}") from e
        publish([msg])
        admin = _admin_put("MODEL_ADMIN", "http://localhost:8002", body)
        hub.add_log(run_id, f"[console] live model change {json.dumps(body)} (model admin: {admin})")
        return {"published": True, "run_id": run_id, "model_admin": admin}

    @app.get("/api/status")
    def status() -> dict[str, Any]:
        return {**worker.status(), "kafka": hub.connected, "kafka_error": hub.last_error, "lease": _lease_holder()}

    @app.get("/api/live/history")
    def live_history(run_id: str | None = None, seconds: int = 300) -> dict[str, Any]:
        return {**hub.history(run_id, seconds), **worker.status()}

    @app.websocket("/api/live")
    async def live_ws(ws: WebSocket) -> None:
        await ws.accept()
        if not await auth_mod.websocket_authorized(ws):
            return
        q = hub.subscribe()
        try:
            await ws.send_json({"type": "status", **worker.status(), "kafka": hub.connected})
            while True:
                msg = await q.get()
                await ws.send_json(msg)
        except (WebSocketDisconnect, RuntimeError):
            pass
        finally:
            hub.unsubscribe(q)

    # ================================================================== schedules
    @app.post("/api/schedules")
    def add_schedule(body: ScheduleRequest) -> dict[str, Any]:
        p = _scenario_path(body.scenario)
        if not p.exists():
            raise HTTPException(404, "no such scenario")
        if body.every_minutes is not None and body.every_minutes <= 0:
            raise HTTPException(400, "every_minutes must be > 0")
        try:
            at = datetime.fromisoformat(body.at.replace("Z", "+00:00")).timestamp() if body.at else time.time()
        except ValueError as e:
            raise HTTPException(400, f"bad 'at': {e}") from e
        remaining = body.count if body.count is not None else (None if body.every_minutes else 1)
        s = store.add_schedule(body.scenario, body.overrides, body.verify, at, body.every_minutes, remaining)
        return s

    @app.get("/api/schedules")
    def list_schedules() -> list[dict[str, Any]]:
        return store.schedules()

    @app.post("/api/schedules/pause-all")
    def pause_all() -> dict[str, Any]:
        return {"paused": store.set_schedules_enabled(False)}

    @app.post("/api/schedules/resume-all")
    def resume_all() -> dict[str, Any]:
        return {"resumed": store.set_schedules_enabled(True)}

    @app.post("/api/schedules/{sid}/pause")
    def pause_schedule(sid: str) -> dict[str, Any]:
        if store.schedule(sid) is None:
            raise HTTPException(404, "no such schedule")
        return {"paused": store.set_schedules_enabled(False, sid)}

    @app.post("/api/schedules/{sid}/resume")
    def resume_schedule(sid: str) -> dict[str, Any]:
        if store.schedule(sid) is None:
            raise HTTPException(404, "no such schedule")
        return {"resumed": store.set_schedules_enabled(True, sid)}

    @app.delete("/api/schedules/{sid}")
    def delete_schedule(sid: str) -> dict[str, Any]:
        if not store.delete_schedule(sid):
            raise HTTPException(404, "no such schedule")
        return {"deleted": sid}

    @app.exception_handler(HTTPException)
    async def http_exc(_, exc: HTTPException):
        return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)

    # ================================================================== UI
    if STATIC.is_dir() and (STATIC / "index.html").exists():
        if (STATIC / "assets").is_dir():
            app.mount("/assets", StaticFiles(directory=STATIC / "assets"), name="assets")
        index_html = STATIC / "index.html"

        @app.get("/{full_path:path}", include_in_schema=False)
        def spa(full_path: str):
            # Deep links (/runs/<id>, /explorer, ...) are client-side routes: serve index.html.
            if full_path.startswith("api/"):
                raise HTTPException(404, "not found")
            f = (STATIC / full_path).resolve()
            if full_path and STATIC.resolve() in f.parents and f.is_file():
                return FileResponse(f)
            return FileResponse(index_html, media_type="text/html", headers={"Cache-Control": "no-cache"})
    else:
        @app.get("/")
        def no_ui() -> dict[str, str]:
            return {"detail": "UI not built: cd ui && npm install && npm run build"}

    return app


def __getattr__(name: str):  # `uvicorn realy.control.app:app` builds the app on first access
    if name == "app":
        a = create_app()
        globals()["app"] = a
        return a
    raise AttributeError(name)
