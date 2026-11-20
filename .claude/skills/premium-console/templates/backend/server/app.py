"""Operator console backend (FastAPI).

    uvicorn server.app:app --port 8090         (or: python -m server)

API (JSON, everything under /api so the SPA owns every other path):
  meta      GET /api/health · GET /api/env · GET /api/kinds
  jobs      POST /api/jobs · GET /api/jobs · GET /api/jobs/{id} · POST /api/jobs/{id}/abort
  overview  GET /api/summary · GET /api/status
  live      WS /api/live (status every 1 s, metrics, log) · GET /api/live/history?seconds=
  auth      GET /api/auth/me · POST /api/auth/login · POST /api/auth/logout      (APP_TOKEN)
  openapi   GET /api/openapi.json · Swagger UI at /api/docs
The built UI (frontend/ -> server/static) is served at / with SPA deep-link fallback.
"""
from __future__ import annotations

import asyncio
import contextlib
import re
import time
from pathlib import Path
from typing import Any, Callable

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import auth as auth_mod
from . import config, health, tasks
from .hub import Hub
from .state import component_states
from .store import Store
from .worker import JobHandle, JobWorker

STATIC = Path(__file__).parent / "static"
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,80}$")
RunFn = Callable[[dict[str, Any], JobHandle], "dict[str, Any] | None"]


class JobRequest(BaseModel):
    kind: str
    params: dict[str, Any] = Field(default_factory=dict)


def create_app(data_dir: Path | None = None, run_fn: RunFn | None = None, probes: list[health.Probe] | None = None,
               background: bool = True, static_dir: Path | None = None) -> FastAPI:
    """App factory. Everything with side effects is injectable, so tests build an app with a temp data dir,
    a fake `run_fn`, fake probes and `background=False` (no threads: call `app.state.worker.execute(job)`)."""
    data_dir = Path(data_dir or config.data_dir())
    data_dir.mkdir(parents=True, exist_ok=True)
    store = Store(data_dir / "jobs.db")
    hub = Hub()
    worker = JobWorker(store, run_fn or tasks.run, log_sink=hub.add_log, metric_sink=hub.add_metric)
    probe_list = probes if probes is not None else health.default_probes(store, worker, data_dir)
    probe_cache = health.ProbeCache(probe_list)
    sticky: dict[str, float] = {}   # hysteresis memory for component_states

    def status() -> dict[str, Any]:
        st = worker.status()
        steps = (st["current"] or {}).get("steps") or []
        comps = component_states(probe_cache.get(), steps, time.time(), sticky)
        return {**st, "components": comps, "server_time": time.time()}

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        hub.bind_loop(asyncio.get_running_loop())
        task = None
        if background:
            for jid in worker.recover():
                hub.add_log(jid, f"[console] job {jid} was interrupted by a restart")
            worker.start()

            async def status_ticker() -> None:
                # one status message per second to every client: the live page's heartbeat
                while True:
                    await asyncio.sleep(1.0)
                    try:
                        msg = await asyncio.to_thread(status)
                    except Exception:  # noqa: BLE001 - the ticker must keep ticking
                        continue
                    hub.publish({"type": "status", **msg})
            task = asyncio.create_task(status_ticker())
        yield
        if task:
            task.cancel()
        await asyncio.to_thread(worker.shutdown, config.shutdown_grace_s())

    app = FastAPI(title=config.APP_NAME, version=config.VERSION, lifespan=lifespan,
                  openapi_url="/api/openapi.json", docs_url="/api/docs", redoc_url=None)
    app.state.store, app.state.hub, app.state.worker = store, hub, worker
    app.add_middleware(auth_mod.AuthMiddleware)
    app.include_router(auth_mod.router)

    @app.exception_handler(HTTPException)
    async def http_exc(_, exc: HTTPException):
        return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)

    # ================================================================== meta
    @app.get("/api/health")
    def get_health() -> dict[str, Any]:
        """Liveness for container health checks: cheap, no dependency calls, open without a token."""
        return {"ok": True, "version": config.VERSION}

    @app.get("/api/env")
    def get_env() -> dict[str, Any]:
        return health.environment(probe_list)

    @app.get("/api/kinds")
    def get_kinds() -> list[dict[str, Any]]:
        return [t.public() for t in tasks.TASKS.values()]

    # ================================================================== jobs
    def _job_or_404(jid: str) -> dict[str, Any]:
        if not ID_RE.match(jid):
            raise HTTPException(400, "invalid job id")
        job = store.job(jid)
        if job is None:
            raise HTTPException(404, "no such job")
        return job

    @app.post("/api/jobs")
    def create_job(body: JobRequest) -> dict[str, Any]:
        task = tasks.TASKS.get(body.kind)
        if task is None:
            raise HTTPException(404, f"no such job kind: {body.kind}")
        try:
            params = task.validate(body.params)   # fail fast: never queue a job that cannot start
        except ValueError as e:
            raise HTTPException(400, str(e)) from e
        job = store.enqueue(body.kind, params)
        worker.wake.set()
        return job

    @app.get("/api/jobs")
    def list_jobs(limit: int = 200) -> dict[str, Any]:
        jobs = store.jobs(min(max(limit, 1), 2000))
        for j in jobs:
            if j["result"]:
                j["result"] = {k: v for k, v in j["result"].items() if k != "series"}   # lists stay light
        return {"jobs": jobs, "queue": store.queued(), "current": worker.status()["current"]}

    @app.get("/api/jobs/{jid}")
    def get_job(jid: str) -> dict[str, Any]:
        job = _job_or_404(jid)
        cur = worker.status()["current"]
        if cur and cur["id"] == jid:
            job["steps"] = cur["steps"]           # live steps while it runs
        return {**job, "logs": list(worker.logs.get(jid, []))[-500:]}

    @app.post("/api/jobs/{jid}/abort")
    def abort_job(jid: str, reason: str = "aborted by operator") -> dict[str, Any]:
        _job_or_404(jid)
        return {"result": worker.abort(jid, reason)}

    # ================================================================== overview + live
    @app.get("/api/summary")
    def summary(limit: int = 12) -> dict[str, Any]:
        jobs = store.jobs(500)
        counts = store.counts()
        done = [j for j in jobs if j["status"] in ("done", "aborted") and j["result"]]
        for j in jobs:
            if j["result"]:
                j["result"] = {k: v for k, v in j["result"].items() if k != "series"}
        return {"jobs_total": sum(counts.values()), "jobs_done": counts.get("done", 0),
                "jobs_failed": counts.get("failed", 0), "items_total": sum(j["result"].get("processed") or 0 for j in done),
                "last_jobs": jobs[:limit], "current": worker.status()["current"]}

    @app.get("/api/status")
    def get_status() -> dict[str, Any]:
        return status()

    @app.get("/api/live/history")
    def live_history(seconds: int = 600, job_id: str | None = None) -> dict[str, Any]:
        return {**hub.history(min(max(seconds, 1), 3600), job_id), **worker.status()}

    @app.websocket("/api/live")
    async def live_ws(ws: WebSocket) -> None:
        await ws.accept()
        if not await auth_mod.websocket_authorized(ws):
            return
        q = hub.subscribe()
        try:
            await ws.send_json({"type": "status", **worker.status()})   # no waiting for the first tick
            while True:
                await ws.send_json(await q.get())
        except (WebSocketDisconnect, RuntimeError):
            pass
        finally:
            hub.unsubscribe(q)

    # ================================================================== UI (SPA fallback)
    static = Path(static_dir or STATIC)
    if static.is_dir() and (static / "index.html").exists():
        if (static / "assets").is_dir():
            app.mount("/assets", StaticFiles(directory=static / "assets"), name="assets")  # hashed, cacheable
        index_html = static / "index.html"

        @app.get("/{full_path:path}", include_in_schema=False)
        def spa(full_path: str):
            # deep links (/jobs/<id>, /live, ...) are client-side routes: serve index.html for them
            if full_path.startswith("api/"):
                raise HTTPException(404, "not found")
            f = (static / full_path).resolve()
            if full_path and static.resolve() in f.parents and f.is_file():
                return FileResponse(f)
            return FileResponse(index_html, media_type="text/html", headers={"Cache-Control": "no-cache"})
    else:
        @app.get("/", include_in_schema=False)
        def no_ui() -> dict[str, str]:
            return {"detail": "UI not built: cd frontend && npm install && npm run build"}

    return app


def __getattr__(name: str):  # `uvicorn server.app:app` builds the app on first access
    if name == "app":
        a = create_app()
        globals()["app"] = a
        return a
    raise AttributeError(name)
