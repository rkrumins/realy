"""Keep the writable scenario directory (a volume: scenarios saved from the visual builder survive
restarts) in step with the scenarios bundled in the image.

A bundled scenario nobody edited follows the image: when an upgrade changes it, the copy is updated. One
that was edited (in the console, or by hand) is kept, and flagged when the bundled version moved on, so
the console can offer to reset it. The record is `<dir>/.bundled.json`: per file, the hash of the bundled
version last copied and whether the copy was edited. A directory seeded before that record existed has no
way to tell an edit from an old bundled version, so a differing copy is moved to `<dir>/.backup/` (outside
the `*.yaml` glob, nothing is lost) and replaced by the bundled one.

    python -m realy.control.scenario_seed <bundled dir> <scenario dir>
"""
from __future__ import annotations

import hashlib
import json
import shutil
import sys
import time
from pathlib import Path
from typing import Any, Callable

RECORD = ".bundled.json"


def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def read_record(dest: Path) -> dict[str, Any]:
    try:
        return json.loads((dest / RECORD).read_text()).get("files", {})
    except (OSError, ValueError):
        return {}


def _write(dest: Path, files: dict[str, Any]) -> None:
    tmp = dest / (RECORD + ".tmp")
    tmp.write_text(json.dumps({"version": 1, "files": files}, indent=1, sort_keys=True))
    tmp.replace(dest / RECORD)


def seed(bundled: Path, dest: Path, log: Callable[[str], None] = print) -> dict[str, list[str]]:
    """Bring `dest` up to date with `bundled`. Returns what happened, by outcome."""
    dest.mkdir(parents=True, exist_ok=True)
    files = read_record(dest)
    legacy = not (dest / RECORD).exists()
    out: dict[str, list[str]] = {"added": [], "updated": [], "kept_edited": [], "backed_up": [], "unchanged": []}
    for f in sorted(bundled.glob("*.yaml")):
        name, d, b_sha = f.name, dest / f.name, _sha(f)
        e = files.get(name)
        if not d.exists():
            shutil.copyfile(f, d)
            files[name] = {"sha": b_sha, "edited": False}
            out["added"].append(name)
            continue
        d_sha = _sha(d)
        if e is None:
            if d_sha == b_sha:
                files[name] = {"sha": b_sha, "edited": False}
                out["unchanged"].append(name)
            elif legacy:
                bk = dest / ".backup"
                bk.mkdir(exist_ok=True)
                shutil.copyfile(d, bk / f"{d.stem}.{time.strftime('%Y%m%d-%H%M%S')}.yaml")
                shutil.copyfile(f, d)
                files[name] = {"sha": b_sha, "edited": False}
                out["backed_up"].append(name)
            else:                                   # a user scenario that now shares a bundled name
                files[name] = {"sha": b_sha, "edited": True}
                out["kept_edited"].append(name)
            continue
        if not e.get("edited") and d_sha != e.get("sha"):
            e["edited"] = True                      # edited by hand since it was copied
        if e.get("edited"):
            out["kept_edited"].append(name)
            e["bundled_sha"] = b_sha                # what the image has now (an update is available if it differs)
            continue
        if b_sha != e.get("sha"):
            shutil.copyfile(f, d)
            files[name] = {"sha": b_sha, "edited": False}
            out["updated"].append(name)
        else:
            out["unchanged"].append(name)
    _write(dest, files)
    for k, label in (("added", "added"), ("updated", "updated to the bundled version"),
                     ("backed_up", "replaced by the bundled version (the old copy is in .backup/)"),
                     ("kept_edited", "kept as edited")):
        if out[k]:
            log(f"scenarios {label}: {', '.join(out[k])}")
    return out


def mark_edited(dest: Path, name: str) -> None:
    """A bundled scenario saved from the console: never overwritten by an upgrade from now on."""
    files = read_record(dest)
    fn = name if name.endswith(".yaml") else f"{name}.yaml"
    if fn in files:
        files[fn]["edited"] = True
        _write(dest, files)


def status(bundled: Path, dest: Path) -> dict[str, dict[str, Any]]:
    """Per scenario name: bundled (ships with the image), edited (the copy differs from what was copied),
    update_available (the image's bundled version is newer than the edited copy's base)."""
    files = read_record(dest)
    out = {}
    for f in bundled.glob("*.yaml"):
        e = files.get(f.name) or {}
        d = dest / f.name
        edited = bool(e.get("edited")) or (d.exists() and e.get("sha") is not None and _sha(d) != e["sha"])
        out[f.stem] = {"bundled": True, "edited": edited,
                       "update_available": edited and _sha(f) != e.get("sha")}
    return out


def reset(bundled: Path, dest: Path, name: str) -> bool:
    """Restore a bundled scenario (the edited copy goes to .backup/)."""
    f = bundled / f"{name}.yaml"
    if not f.exists():
        return False
    d = dest / f.name
    if d.exists():
        bk = dest / ".backup"
        bk.mkdir(exist_ok=True)
        shutil.copyfile(d, bk / f"{name}.{time.strftime('%Y%m%d-%H%M%S')}.yaml")
    shutil.copyfile(f, d)
    files = read_record(dest)
    files[f.name] = {"sha": _sha(f), "edited": False}
    _write(dest, files)
    return True


if __name__ == "__main__":
    seed(Path(sys.argv[1]), Path(sys.argv[2]))
