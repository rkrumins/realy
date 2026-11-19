"""docsgen: every reference page renders, is deterministic (CI drift check relies on it) and covers its source."""
from __future__ import annotations

import yaml

from realy import docsgen
from realy.scenario import REPO_ROOT


def test_pages_render_deterministically():
    for name, fn in docsgen.PAGES.items():
        a, b = fn(), fn()
        assert a == b, f"{name} is not deterministic"
        assert a.startswith("<!-- GENERATED"), name
        assert "\n# " in a, name


def test_pages_cover_their_sources():
    feats = yaml.safe_load((REPO_ROOT / "contracts" / "features" / "feature_schema.yaml").read_text())["features"]
    page = docsgen.features()
    assert all(f"`{f['name']}`" in page for f in feats)
    cfg = docsgen.config()
    for field in ("budget_ms", "feature_deadline_ms", "service_time", "max_batch", "error_rate", "timeout_rate"):
        assert f"`{field}`" in cfg, field
    cli = docsgen.cli()
    for cmd in ("pulsebench run", "pulsebench verify", "pulsebench budget", "pulsebench console"):
        assert f"## `{cmd}`" in cli, cmd
    api = docsgen.http_api()
    for path in ("/api/runs", "/api/env", "/api/topology", "/api/auth/login"):
        assert f"`{path}`" in api, path
    scen = docsgen.scenarios()
    assert all(f"`{p.stem}`" in scen for p in (REPO_ROOT / "scenarios").glob("*.yaml"))


def test_committed_reference_is_current():
    for name, fn in docsgen.PAGES.items():
        assert (docsgen.OUT / name).read_text() == fn(), f"docs/reference/{name} is stale: run python -m realy.docsgen"
