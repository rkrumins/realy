#!/usr/bin/env python3
"""Copy the console templates into a new project and give it a name.

    python scaffold.py ../my-console --name "Fleet Console" [--tagline "fleet ops"] [--only frontend|backend] [--force]

Creates <target>/frontend (React + Vite + Tailwind), <target>/backend (FastAPI), plus Dockerfile,
docker-compose.yml and Makefile at the root (unless --only). Replaces the template's default name
("Operator Console"), slug ("operator-console") and sidebar tagline everywhere they appear.
"""
from __future__ import annotations

import argparse
import re
import shutil
import sys
from pathlib import Path

TEMPLATES = Path(__file__).resolve().parent.parent / "templates"
DEFAULT_NAME, DEFAULT_SLUG = "Operator Console", "operator-console"
DEFAULT_TAGLINE = 'tagline: "operations"'
SKIP = {"node_modules", "dist", "__pycache__", ".pytest_cache", "data"}
TEXT_SUFFIXES = {".ts", ".tsx", ".json", ".html", ".css", ".py", ".toml", ".mjs", ".yml", ".yaml", ".md", ".txt"}
TEXT_NAMES = {"Dockerfile", "Makefile", ".gitignore", ".npmrc"}


def slugify(name: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    if not s:
        raise SystemExit("the name must contain letters or digits")
    return s


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("target", type=Path)
    ap.add_argument("--name", required=True, help='display name, e.g. "Fleet Console"')
    ap.add_argument("--slug", help="kebab-case id (default: from the name)")
    ap.add_argument("--tagline", default="operations", help="small caps line under the name in the sidebar")
    ap.add_argument("--only", choices=["frontend", "backend"], help="copy one half only")
    ap.add_argument("--force", action="store_true", help="write into a non-empty directory")
    a = ap.parse_args()

    slug = a.slug or slugify(a.name)
    target: Path = a.target.resolve()
    if target.exists() and any(target.iterdir()) and not a.force:
        print(f"{target} is not empty (use --force to write into it)", file=sys.stderr)
        return 1

    written = 0
    for src in sorted(TEMPLATES.rglob("*")):
        rel = src.relative_to(TEMPLATES)
        if any(part in SKIP for part in rel.parts) or src.is_dir():
            continue
        if a.only and rel.parts[0] != a.only:
            continue
        dst = target / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        if src.suffix in TEXT_SUFFIXES or src.name in TEXT_NAMES:
            text = src.read_text(encoding="utf-8")
            text = text.replace(DEFAULT_NAME, a.name).replace(DEFAULT_SLUG, slug)
            if src.name == "app.config.ts":
                text = text.replace(DEFAULT_TAGLINE, f'tagline: "{a.tagline}"')
            dst.write_text(text, encoding="utf-8")
        else:
            shutil.copy2(src, dst)
        written += 1

    print(f"{written} files written to {target} ({a.name}, slug {slug})\n")
    print("next steps:")
    if a.only != "backend":
        print(f"  cd {target / 'frontend'} && npm install && npx tsc --noEmit && npx vitest run")
    if a.only != "frontend":
        print(f"  cd {target / 'backend'} && python -m pip install -e '.[dev]' && python -m pytest -q")
    if not a.only:
        print(f"  cd {target} && make build && make run       # UI + API on http://127.0.0.1:8090")
        print(f"  # or, for hot reload: make dev-api   and, in another shell, make dev-ui (http://127.0.0.1:5173)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
