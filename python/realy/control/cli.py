"""`realy console` — wire into the main CLI with: app.command()(console)."""
from __future__ import annotations

import typer


def console(host: str = typer.Option("0.0.0.0", help="bind address"),
            port: int = typer.Option(8090, help="HTTP port")) -> None:
    """Operator console: configure, launch, watch and compare benchmark runs (http://HOST:PORT)."""
    import uvicorn

    from .app import create_app
    # timeout_graceful_shutdown: open WebSockets must not hold up the lifespan shutdown, which aborts the
    # running run and waits REALY_SHUTDOWN_GRACE_S for its drain + partial report
    uvicorn.run(create_app(), host=host, port=port, log_level="warning", ws_ping_interval=20, timeout_graceful_shutdown=5)
