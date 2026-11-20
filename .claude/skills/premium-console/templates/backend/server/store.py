"""SQLite persistence for jobs (queued / running / done ...). One connection, guarded by a lock."""
from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  params TEXT NOT NULL DEFAULT '{}',
  status TEXT NOT NULL,              -- queued | running | done | failed | aborted | cancelled | interrupted
  created_at REAL NOT NULL,
  started_at REAL,
  finished_at REAL,
  error TEXT,
  abort_reason TEXT,
  result TEXT,                       -- JSON (summary + insights + per-second series)
  steps TEXT NOT NULL DEFAULT '[]'   -- JSON (final step list with durations)
);
CREATE INDEX IF NOT EXISTS jobs_status ON jobs (status, created_at);
"""
JSON_COLS = ("params", "result", "steps")
FINAL = ("done", "failed", "aborted", "cancelled", "interrupted")


class Store:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._lock = threading.Lock()
        self._db = sqlite3.connect(str(path), check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        with self._lock:
            self._db.executescript(SCHEMA)
            # a restart cannot resume a job that was in flight in the previous process: say so honestly
            cur = self._db.execute("SELECT id FROM jobs WHERE status = 'running'")
            self.interrupted = [r["id"] for r in cur.fetchall()]
            self._db.execute("UPDATE jobs SET status='interrupted', abort_reason='service restarted during the job', "
                             "finished_at=? WHERE status='running'", (time.time(),))
            self._db.commit()

    def _exec(self, sql: str, args: tuple = ()) -> list[dict[str, Any]]:
        with self._lock:
            cur = self._db.execute(sql, args)
            rows = [dict(r) for r in cur.fetchall()]
            self._db.commit()
            return rows

    @staticmethod
    def _row(r: dict[str, Any]) -> dict[str, Any]:
        r = dict(r)
        for c in JSON_COLS:
            r[c] = json.loads(r[c]) if r.get(c) else ({} if c == "params" else [] if c == "steps" else None)
        return r

    # ------------------------------------------------------------------ jobs
    def enqueue(self, kind: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        jid = uuid.uuid4().hex[:12]
        self._exec("INSERT INTO jobs (id, kind, params, status, created_at) VALUES (?, ?, ?, 'queued', ?)",
                   (jid, kind, json.dumps(params or {}), time.time()))
        return self.job(jid)  # type: ignore[return-value]

    def job(self, jid: str) -> dict[str, Any] | None:
        rows = self._exec("SELECT * FROM jobs WHERE id = ?", (jid,))
        return self._row(rows[0]) if rows else None

    def jobs(self, limit: int = 200) -> list[dict[str, Any]]:
        return [self._row(r) for r in self._exec("SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,))]

    def queued(self) -> list[dict[str, Any]]:
        return [self._row(r) for r in self._exec("SELECT * FROM jobs WHERE status = 'queued' ORDER BY created_at")]

    def next_queued(self) -> dict[str, Any] | None:
        rows = self._exec("SELECT * FROM jobs WHERE status = 'queued' ORDER BY created_at LIMIT 1")
        return self._row(rows[0]) if rows else None

    def update(self, jid: str, **fields: Any) -> None:
        if not fields:
            return
        vals = [json.dumps(v) if k in JSON_COLS and v is not None else v for k, v in fields.items()]
        cols = ", ".join(f"{k} = ?" for k in fields)
        self._exec(f"UPDATE jobs SET {cols} WHERE id = ?", (*vals, jid))

    def counts(self) -> dict[str, int]:
        return {r["status"]: r["n"] for r in self._exec("SELECT status, count(*) AS n FROM jobs GROUP BY status")}

    def ping(self) -> int:
        return self._exec("SELECT count(*) AS n FROM jobs")[0]["n"]
