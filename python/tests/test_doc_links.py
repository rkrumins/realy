"""Every relative link and #anchor in docs/ and README.md resolves (GitHub's heading slugs)."""
from __future__ import annotations

import re
from pathlib import Path

from realy.scenario import REPO_ROOT

LINK = re.compile(r"(?<!!)\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")


def slugs(md: str) -> set[str]:
    """GitHub's anchors: lowercase, punctuation dropped, spaces to hyphens, -1/-2 for repeats."""
    out: dict[str, int] = {}
    found = set()
    in_code = False
    for line in md.splitlines():
        if line.strip().startswith("```"):
            in_code = not in_code
            continue
        if in_code:
            continue
        m = re.match(r"^(#{1,6})\s+(.*?)\s*#*\s*$", line)
        if not m:
            continue
        text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", m.group(2))     # links keep their text
        s = re.sub(r"[^\w\- ]", "", text.strip().lower()).replace(" ", "-")
        n = out.get(s, 0)
        found.add(s if n == 0 else f"{s}-{n}")
        out[s] = n + 1
    return found


def links(md: str):
    in_code = False
    for n, line in enumerate(md.splitlines(), 1):
        if line.strip().startswith("```"):
            in_code = not in_code
            continue
        if in_code:
            continue
        for m in LINK.finditer(re.sub(r"`[^`]*`", "", line)):
            yield n, m.group(1)


def test_relative_links_and_anchors_resolve():
    files = sorted((REPO_ROOT / "docs").rglob("*.md")) + [REPO_ROOT / "README.md"]
    cache: dict[Path, set[str]] = {}
    broken = []
    for f in files:
        text = f.read_text()
        for n, target in links(text):
            if re.match(r"^[a-z]+:", target) or target.startswith("/"):
                continue                              # external, or a console route
            path, _, anchor = target.partition("#")
            dest = (f.parent / path).resolve() if path else f
            if path and not dest.exists():
                broken.append(f"{f.relative_to(REPO_ROOT)}:{n}: {target} (no such file)")
                continue
            if anchor and dest.suffix == ".md":
                if dest not in cache:
                    cache[dest] = slugs(dest.read_text())
                if anchor not in cache[dest]:
                    broken.append(f"{f.relative_to(REPO_ROOT)}:{n}: {target} (no heading #{anchor})")
    assert not broken, "broken links:\n" + "\n".join(broken)


def test_slug_rules():
    s = slugs("# Lost / stolen card\n## What's new?\n## What's new?\n```\n# not a heading\n```\n## `code` and [link](x.md)")
    assert s == {"lost--stolen-card", "whats-new", "whats-new-1", "code-and-link"}
