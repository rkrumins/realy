"""The committed ClickHouse DDL (contracts/storage/clickhouse) is exactly what codegen produces from the
contracts: a contract change without `make codegen` fails here, and a refactor of codegen that changes
the DDL for today's contracts shows up as a diff (its golden text is the committed file)."""
from __future__ import annotations

from realy import codegen


def test_committed_ddl_is_current():
    stale = []
    for name, sql in codegen.generate().items():
        want = sql.lstrip("\n") if sql.startswith("\n") else sql
        p = codegen.OUT / name
        if not p.exists() or p.read_text() != want:
            stale.append(name)
    assert not stale, f"regenerate with `make codegen` (python -m realy.codegen): {stale}"
