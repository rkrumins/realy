"""Service identity and settings. Everything configurable comes from APP_* environment variables."""
from __future__ import annotations

import os
from pathlib import Path

APP_NAME = "Operator Console"
SLUG = "operator-console"          # cookie names, user agents; scaffold.py rewrites it with the name
VERSION = "0.1.0"


def env(name: str, default: str = "") -> str:
    return os.environ.get(f"APP_{name}", default)


def data_dir() -> Path:
    return Path(env("DATA_DIR", "./data"))


def shutdown_grace_s() -> float:
    """How long a running job gets to stop cleanly on shutdown (keep below the orchestrator's grace period)."""
    return float(env("SHUTDOWN_GRACE_S", "20"))
