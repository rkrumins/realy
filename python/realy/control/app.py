"""realy operator console — FastAPI backend.

    uvicorn realy.control.app:app --port 8090        (or: realy console)

API (JSON):
  scenarios   GET /api/scenarios · GET|PUT /api/scenarios/{name} · POST /api/scenarios/validate
              POST /api/scenarios/{name}/budget
  runs        POST /api/runs · GET /api/runs · GET /api/runs/{id} · GET /api/runs/{id}/report.html
              POST /api/runs/{id}/abort · GET /api/compare?a=&b=
  live        WS /api/live · GET /api/live/history · GET /api/status
              POST /api/live/rate · POST /api/live/scoring · POST /api/live/model
  schedules   POST /api/schedules · GET /api/schedules · DELETE /api/schedules/{id}
The built UI (ui/ -> realy/control/static) is served at /.
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
    worker = RunWorker(store, runs_dir, scenario_dir, run_fn=run_fn, verify_fn=verify_fn, log_sink=hub.add_log)
    scheduler = Scheduler(store, worker)
    summary_cache: dict[str, tuple[float, dict]] = {}

    def publish(msgs: list) -> None:
        if publish_fn is not None:
            publish_fn(msgs)
            return
        from ..common import control
        control.publish(msgs)

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        hub.bind_loop(asyncio.get_running_loop())
        task = None
        if background:
            hub.start()
            worker.start()
            scheduler.start()

            async def status_ticker() -> None:
                while True:
                    await asyncio.sleep(1.0)
                    hub.publish({"type": "status", **worker.status(), "kafka": hub.connected})
            task = asyncio.create_task(status_ticker())
        yield
        if task:
            task.cancel()
        hub.shutdown()
        worker.shutdown()
        scheduler.shutdown()

    app = FastAPI(title="realy console", lifespan=lifespan)
    app.state.store, app.state.hub, app.state.worker, app.state.scheduler = store, hub, worker, scheduler

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
        rep_p = run_dir / "report.json"
        key = str(run_dir)
        mt = max((p.stat().st_mtime for p in (rep_p, run_dir / "verify.json", run_dir / "run.json",
                                               run_dir / "manifest.json") if p.exists()), default=0)
        hit = summary_cache.get(key)
        if hit and hit[0] == mt:
            return hit[1]
        man = _json(run_dir / "manifest.json") or {}
        rep = _json(rep_p) or {}
        ver = _json(run_dir / "verify.json")
        run = _json(run_dir / "run.json") or {}
        s = rep.get("summary") or {}
        out = {"run_id": run_dir.name, "scenario": man.get("scenario") or rep.get("scenario"),
               "topology": man.get("topology") or rep.get("topology"),
               "started_at": (man.get("t_start_us") or 0) / 1e6 or run_dir.stat().st_mtime,
               "duration_s": man.get("duration_s"), "status": "done" if rep else ("running" if man else "incomplete"),
               "verdict": s.get("verdict"), "reasons": s.get("reasons", []),
               "achieved_tps": s.get("achieved_emit_tps"), "e2e_p50_ms": s.get("e2e_p50_ms"),
               "e2e_p99_ms": s.get("e2e_p99_ms"), "hot_path_p99_ms": s.get("hot_path_p99_ms"),
               "within_budget": s.get("within_budget"), "emitted": s.get("emitted"), "decided": s.get("decided"),
               "missing": s.get("missing"), "duplicates": s.get("duplicates"),
               "abort_reason": run.get("aborted"), "verify_passed": None if ver is None else ver.get("passed"),
               "has_html": (run_dir / "report.html").exists()}
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
        cur = worker.status()["current"]
        if cur:
            for h in history:
                if h["run_id"] == cur["run_id"]:
                    h["status"] = "running"
                    h["phase"] = cur["phase"]
        return {"queue": store.queued(), "jobs": jobs[:50], "history": history, "current": cur}

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

    @app.post("/api/runs/{ident}/abort")
    def abort_run(ident: str) -> dict[str, Any]:
        result = worker.abort(ident)
        if result == "not-running-here" or result == "abort-signalled":
            job = store.job(ident)
            run_id = (job or {}).get("run_id") or ident
            try:
                from ..common.control import control_message
                publish([control_message(run_id, "*", phase=6)])
            except Exception as e:  # noqa: BLE001
                if result == "not-running-here":
                    raise HTTPException(502, f"could not publish abort: {e}") from e
        return {"result": result}

    @app.get("/api/compare")
    def compare(a: str, b: str) -> dict[str, Any]:
        ra, rb = _json(_run_dir(a) / "report.json") or {}, _json(_run_dir(b) / "report.json") or {}
        stages: dict[str, dict[str, Any]] = {}
        for name in sorted(set(ra.get("stages", {})) | set(rb.get("stages", {}))):
            sa, sb = ra.get("stages", {}).get(name, {}), rb.get("stages", {}).get(name, {})
            row: dict[str, Any] = {}
            for q in ("p50", "p99", "p999"):
                va, vb = sa.get(q), sb.get(q)
                row[f"a_{q}"], row[f"b_{q}"] = va, vb
                row[f"d_{q}"] = (vb - va) if va is not None and vb is not None else None
            stages[name] = row
        return {"a": _summary(_run_dir(a)), "b": _summary(_run_dir(b)), "stages": stages,
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
        return {**worker.status(), "kafka": hub.connected, "kafka_error": hub.last_error}

    @app.get("/api/live/history")
    def live_history(run_id: str | None = None, seconds: int = 300) -> dict[str, Any]:
        return {**hub.history(run_id, seconds), **worker.status()}

    @app.websocket("/api/live")
    async def live_ws(ws: WebSocket) -> None:
        await ws.accept()
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
        app.mount("/", StaticFiles(directory=STATIC, html=True), name="ui")
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
