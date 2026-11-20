"""python -m server [--host 0.0.0.0] [--port 8090] [--reload]"""
from __future__ import annotations

import argparse

import uvicorn


def main() -> None:
    ap = argparse.ArgumentParser(prog="server")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8090)
    ap.add_argument("--reload", action="store_true", help="restart on code changes (development)")
    a = ap.parse_args()
    uvicorn.run("server.app:create_app", factory=True, host=a.host, port=a.port, reload=a.reload)


if __name__ == "__main__":
    main()
