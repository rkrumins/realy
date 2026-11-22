"""Python twins of extension plugins (docs/dev/extending.md): the modules that register plugin aggregations
(realy.features.aggregations) and plugin derived functions (realy.features.derived_fns), next to the Java
jars the gateway and Flink load from PLUGINS_DIR.

Loaded once, on first use of either registry, from:
  - REALY_PLUGINS: comma-separated module names (e.g. `realy.plugin_example`); PLUGINS_DIR/python is on
    the import path, so a twin can sit next to its jar;
  - the `pulsebench.plugins` entry point group of installed packages.
"""
from __future__ import annotations

import importlib
import os
import sys

_LOADED: list[str] | None = None


def modules_from_env() -> list[str]:
    spec = os.environ.get("REALY_PLUGINS", "")
    return [m.strip() for m in spec.split(",") if m.strip()]


def _path() -> None:
    pdir = os.environ.get("PLUGINS_DIR", "")
    py = os.path.join(pdir, "python") if pdir else ""
    if py and os.path.isdir(py) and py not in sys.path:
        sys.path.append(py)


def load(modules: list[str] | None = None) -> list[str]:
    """Import twin modules (each registers what it provides). `modules` given: exactly those; otherwise the
    environment and the entry points, once per process. Returns the modules imported by this call."""
    global _LOADED
    _path()
    if modules is not None:
        for m in modules:
            importlib.import_module(m)
        return list(modules)
    if _LOADED is not None:
        return []
    _LOADED = []
    for m in modules_from_env():
        importlib.import_module(m)
        _LOADED.append(m)
    try:
        from importlib.metadata import entry_points
        for ep in entry_points(group="pulsebench.plugins"):
            ep.load()
            _LOADED.append(ep.value)
    except Exception as e:  # noqa: BLE001 - a broken plugin must say so, not vanish
        raise RuntimeError(f"a plugin failed to load: {e}") from e
    return list(_LOADED)


def reset() -> None:
    """Forget what was loaded (tests)."""
    global _LOADED
    _LOADED = None
