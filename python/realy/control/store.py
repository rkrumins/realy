"""SQLite persistence for the console: the run queue (queued/running/done...) and schedules."""
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
  run_id TEXT,
  scenario TEXT NOT NULL,
  overrides TEXT NOT NULL DEFAULT '{}',
  verify INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL,              -- queued | running | done | failed | aborted | cancelled | interrupted
  created_at REAL NOT NULL,
  started_at REAL,
  finished_at REAL,
  verdict TEXT,
  verify_passed INTEGER,
  abort_reason TEXT,
  error TEXT,
  schedule_id TEXT
);
CREATE TABLE IF NOT EXISTS schedules (
  id TEXT PRIMARY KEY,
  scenario TEXT NOT NULL,
  overrides TEXT NOT NULL DEFAULT '{}',
  verify INTEGER NOT NULL DEFAULT 0,
  next_at REAL NOT NULL,
  every_minutes REAL,
  remaining INTEGER,                 -- NULL = repeat forever
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at REAL NOT NULL,
  last_job TEXT
);
"""


class Store:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._db = sqlite3.connect(str(path), check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        with self._lock:
            self._db.executescript(SCHEMA)
            # a console restart cannot resume a run that was in flight in the previous process: mark it
            # interrupted (the worker's recover() stops any survivor and finishes its run directory)
            cur = self._db.execute("SELECT * FROM jobs WHERE status = 'running'")
            self.interrupted = [self._job(dict(r)) for r in cur.fetchall()]
            self._db.execute("UPDATE jobs SET status='interrupted', abort_reason='console restarted during run', "
                             "finished_at=? WHERE status='running'", (time.time(),))
            self._db.commit()

    def _exec(self, sql: str, args: tuple = ()) -> list[dict[str, Any]]:
        with self._lock:
            cur = self._db.execute(sql, args)
            rows = [dict(r) for r in cur.fetchall()]
            self._db.commit()
            return rows

    @staticmethod
    def _job(r: dict[str, Any]) -> dict[str, Any]:
        r = dict(r)
        r["overrides"] = json.loads(r.get("overrides") or "{}")
        r["verify"] = bool(r.get("verify"))
        return r

    # ------------------------------------------------------------------ jobs
    def enqueue(self, scenario: str, overrides: dict | None = None, verify: bool = False,
                schedule_id: str | None = None) -> dict[str, Any]:
        jid = uuid.uuid4().hex[:12]
        self._exec("INSERT INTO jobs (id, scenario, overrides, verify, status, created_at, schedule_id) "
                   "VALUES (?, ?, ?, ?, 'queued', ?, ?)",
                   (jid, scenario, json.dumps(overrides or {}), int(verify), time.time(), schedule_id))
        return self.job(jid)

    def job(self, jid: str) -> dict[str, Any] | None:
        rows = self._exec("SELECT * FROM jobs WHERE id = ? OR run_id = ?", (jid, jid))
        return self._job(rows[0]) if rows else None

    def jobs(self, limit: int = 200) -> list[dict[str, Any]]:
        return [self._job(r) for r in self._exec("SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,))]

    def next_queued(self) -> dict[str, Any] | None:
        rows = self._exec("SELECT * FROM jobs WHERE status = 'queued' ORDER BY created_at LIMIT 1")
        return self._job(rows[0]) if rows else None

    def queued(self) -> list[dict[str, Any]]:
        return [self._job(r) for r in self._exec("SELECT * FROM jobs WHERE status = 'queued' ORDER BY created_at")]

    def update(self, jid: str, **fields: Any) -> None:
        if not fields:
            return
        cols = ", ".join(f"{k} = ?" for k in fields)
        self._exec(f"UPDATE jobs SET {cols} WHERE id = ?", (*fields.values(), jid))

    # ------------------------------------------------------------------ schedules
    def add_schedule(self, scenario: str, overrides: dict | None, verify: bool, next_at: float,
                     every_minutes: float | None, remaining: int | None) -> dict[str, Any]:
        sid = uuid.uuid4().hex[:10]
        self._exec("INSERT INTO schedules (id, scenario, overrides, verify, next_at, every_minutes, remaining, "
                   "enabled, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)",
                   (sid, scenario, json.dumps(overrides or {}), int(verify), next_at, every_minutes, remaining,
                    time.time()))
        return self.schedule(sid)

    def schedule(self, sid: str) -> dict[str, Any] | None:
        rows = self._exec("SELECT * FROM schedules WHERE id = ?", (sid,))
        return self._job(rows[0]) if rows else None

    def schedules(self) -> list[dict[str, Any]]:
        return [self._job(r) for r in self._exec("SELECT * FROM schedules ORDER BY next_at")]

    def delete_schedule(self, sid: str) -> bool:
        before = len(self._exec("SELECT id FROM schedules WHERE id = ?", (sid,)))
        self._exec("DELETE FROM schedules WHERE id = ?", (sid,))
        return before > 0

    def due_schedules(self, now: float) -> list[dict[str, Any]]:
        return [self._job(r) for r in self._exec(
            "SELECT * FROM schedules WHERE enabled = 1 AND next_at <= ? ORDER BY next_at", (now,))]

    def set_schedules_enabled(self, enabled: bool, sid: str | None = None) -> list[str]:
        cond = " AND (remaining IS NULL OR remaining > 0)" if enabled else ""   # never revive a finished one-shot
        rows = self._exec("SELECT id FROM schedules WHERE enabled = ?" + cond + (" AND id = ?" if sid else ""),
                          (int(not enabled), sid) if sid else (int(not enabled),))
        ids = [r["id"] for r in rows]
        for i in ids:
            self._exec("UPDATE schedules SET enabled = ? WHERE id = ?", (int(enabled), i))
        return ids

    def update_schedule(self, sid: str, **fields: Any) -> None:
        cols = ", ".join(f"{k} = ?" for k in fields)
        self._exec(f"UPDATE schedules SET {cols} WHERE id = ?", (*fields.values(), sid))
