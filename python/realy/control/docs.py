"""In-console documentation: serves the repository's docs/*.md (the same files GitHub renders) with a
navigation index, raw assets (images), and machine-generated references (scenario JSON schema)."""
from __future__ import annotations

import mimetypes
import re
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from ..scenario import REPO_ROOT, Scenario

router = APIRouter(tags=["docs"])

# Navigation order; files not listed are appended under "More". Missing files are skipped.
NAV: list[tuple[str, list[str]]] = [
    ("Start here", ["getting-started.md", "console-guide.md", "scenarios.md"]),
    ("Concepts", ["architecture.md", "feature-store-and-inference.md", "fraud-detection.md", "contracts.md",
                  "synthetic-data.md", "latency-budget.md"]),
    ("Run & deploy", ["running-benchmarks.md", "deployment-docker.md", "deployment-kubernetes.md", "k8s-l4.md",
                      "production.md", "capacity-planning.md", "operations.md", "security.md"]),
    ("Develop", ["developer-guide.md", "dev/flink-job.md", "dev/messages.md"]),
    ("Reference", ["reference/messages.md", "reference/scenario-schema.md", "reference/config.md", "reference/cli.md", "reference/http-api.md",
                   "reference/features.md", "reference/rules.md", "reference/topics.md", "reference/scenarios.md"]),
]


def docs_dir() -> Path:
    return REPO_ROOT / "docs"


def _title(p: Path) -> str:
    try:
        for line in p.read_text().splitlines():
            m = re.match(r"^#\s+(.*)", line)
            if m:
                return m.group(1).strip()
    except OSError:
        pass
    return p.stem.replace("-", " ").capitalize()


def _safe(rel: str) -> Path:
    base = docs_dir().resolve()
    p = (base / rel).resolve()
    if base not in p.parents and p != base:
        raise HTTPException(400, "path outside docs/")
    if not p.is_file():
        raise HTTPException(404, "no such document")
    return p


@router.get("/api/docs")
def index() -> dict[str, Any]:
    base = docs_dir()
    seen: set[str] = set()
    sections = []
    for title, files in NAV:
        items = []
        for f in files:
            p = base / f
            if p.is_file():
                items.append({"path": f, "title": _title(p)})
                seen.add(f)
        if items:
            sections.append({"title": title, "items": items})
    more = [{"path": str(p.relative_to(base)), "title": _title(p)} for p in sorted(base.rglob("*.md"))
            if str(p.relative_to(base)) not in seen]
    if more:
        sections.append({"title": "More", "items": more})
    readme = REPO_ROOT / "README.md"
    if readme.is_file():
        sections.insert(0, {"title": "Overview", "items": [{"path": "../README.md", "title": "realy"}]})
    return {"sections": sections}


@router.get("/api/docs/page")
def page(path: str) -> dict[str, Any]:
    if path == "../README.md":
        p = REPO_ROOT / "README.md"
        text = p.read_text().replace("](docs/", "](")  # README links are relative to the repo root
        return {"path": path, "title": "realy", "markdown": text}
    p = _safe(path)
    return {"path": path, "title": _title(p), "markdown": p.read_text()}


@router.get("/api/docs/raw/{rel:path}")
def raw(rel: str):
    p = _safe(rel)
    return FileResponse(p, media_type=mimetypes.guess_type(p.name)[0] or "application/octet-stream")


@router.get("/api/reference/scenario-schema")
def scenario_schema() -> dict[str, Any]:
    return Scenario.model_json_schema()
