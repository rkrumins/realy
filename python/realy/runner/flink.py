"""Flink job management over the REST API: keep exactly one realy job running in the mode the
topology needs, switching modes via stop-with-savepoint so keyed feature state (RocksDB) stays warm
(operator uids `features-<entity>` are identical across modes).

  gateway-* topologies -> `features` job (writes entity blobs to the online store)
  flink-inline         -> `both` job (inline scoring + online-store writes)
  echo-kafka           -> no change (gateway echoes)
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Callable

import httpx

from ..scenario import REPO_ROOT, Scenario

JAR = Path(os.environ.get("FLINK_JOB_JAR", REPO_ROOT / "java" / "flink-job" / "build" / "libs" / "realy-flink-job.jar"))
ENTRY = "io.realy.flink.FraudJob"
SAVEPOINT_DIR = os.environ.get("FLINK_SAVEPOINT_DIR", "file:///checkpoints/savepoints")


def _running(c: httpx.Client) -> list[dict]:
    jobs = c.get("/jobs/overview").json().get("jobs", [])
    return [j for j in jobs if j["state"] in ("RUNNING", "CREATED", "INITIALIZING", "RESTARTING")
            and j["name"].startswith("realy-")]


def _jar_id(c: httpx.Client) -> tuple[str, bool]:
    """Upload the job jar unless an identical-or-newer copy is already on the cluster.
    Returns (jar id, freshly_uploaded)."""
    local_ms = JAR.stat().st_mtime * 1000
    for f in c.get("/jars").json().get("files", []):
        if f["name"] == JAR.name:
            if f.get("uploaded", 0) >= local_ms:
                return f["id"], False
            c.delete(f"/jars/{f['id']}")
    with JAR.open("rb") as fh:
        r = c.post("/jars/upload", files={"jarfile": (JAR.name, fh, "application/x-java-archive")}, timeout=120)
    r.raise_for_status()
    return r.json()["filename"].split("/")[-1], True


def _wait_terminal(c: httpx.Client, jid: str, timeout_s: float = 60) -> None:
    end = time.time() + timeout_s
    while time.time() < end:
        if c.get(f"/jobs/{jid}").json().get("state") in ("FINISHED", "CANCELED", "FAILED"):
            return
        time.sleep(0.5)


def _cancel(c: httpx.Client, jid: str) -> None:
    c.patch(f"/jobs/{jid}", params={"mode": "cancel"})
    _wait_terminal(c, jid)


def _stop_with_savepoint(c: httpx.Client, jid: str, log) -> str | None:
    """Stop a job keeping its state; if that is impossible (e.g. job restarting), cancel it.
    Either way the job is terminal when this returns, so its slots are free."""
    location = None
    r = c.post(f"/jobs/{jid}/stop", json={"targetDirectory": SAVEPOINT_DIR, "drain": False})
    if r.status_code < 300:
        trig = r.json()["request-id"]
        for _ in range(240):
            st = c.get(f"/jobs/{jid}/savepoints/{trig}").json()
            if st.get("status", {}).get("id") == "COMPLETED":
                op = st.get("operation", {})
                location = op.get("location")
                if not location:
                    log(f"savepoint failed ({op.get('failure-cause', {}).get('class', '?')}); cancelling (state starts cold)")
                break
            time.sleep(0.5)
    if location is None:
        _cancel(c, jid)
    else:
        _wait_terminal(c, jid)
    return location


def desired_mode(sc: Scenario) -> str | None:
    if sc.topology == "flink-inline":
        return "both"
    if sc.topology in ("gateway-kafka", "gateway-grpc"):
        return "features"
    return None


def ensure_jobs(sc: Scenario, rest: str | None, log: Callable[[str], None] = print, parallelism: int | None = None,
                force: bool = False, run_id: str | None = None) -> str | None:
    mode = desired_mode(sc)
    if not rest or mode is None:
        return None
    if not JAR.exists():
        raise FileNotFoundError(f"{JAR} not built (cd java && ./gradlew :flink-job:shadowJar)")
    par = parallelism or int(os.environ.get("FLINK_PARALLELISM", "2"))
    scoring = json.dumps(sc.scoring_config(), separators=(",", ":"))
    with httpx.Client(base_url=rest, timeout=30) as c:
        running = _running(c)
        want = f"realy-{mode}"
        jar, new_code = _jar_id(c)
        fresh = sc.flink_state == "fresh"
        if (any(j["name"] == want for j in running) and mode == "features" and not new_code and not force and not fresh
                and not sc.exactly_once):
            log(f"flink: {want} already running")
            return [j["jid"] for j in running if j["name"] == want][0]
        savepoint = None
        for j in running:
            if fresh:
                log(f"flink: cancelling {j['name']} (fresh state requested)")
                _cancel(c, j["jid"])
                continue
            log(f"flink: stopping {j['name']} with savepoint{' (new job jar)' if new_code else ''}")
            savepoint = _stop_with_savepoint(c, j["jid"], log) or savepoint
        args = ["--mode", mode, "--parallelism", str(par), "--bootstrap", os.environ.get("FLINK_KAFKA_BOOTSTRAP", "kafka:29092"),
                "--group", f"realy-flink-{run_id}" if fresh and run_id else "realy-flink", "--scoring-config", scoring,
                "--checkpoint-ms", str(int(os.environ.get("FLINK_CHECKPOINT_MS", "10000"))),
                "--exactly-once", "true" if sc.exactly_once else "false"]
        # online-store blobs carrying aggregator state (exact gateway features); "none" = values only
        if os.environ.get("FLINK_STORE_STATE"):
            args += ["--store-state", os.environ["FLINK_STORE_STATE"]]
        body = {"entryClass": ENTRY, "programArgsList": args, "parallelism": par}
        if savepoint:
            body.update({"savepointPath": savepoint, "allowNonRestoredState": True})
        r = c.post(f"/jars/{jar}/run", json=body, timeout=120)
        if r.status_code >= 300:
            raise RuntimeError(f"flink submit failed: {r.status_code} {r.text[:500]}")
        jid = r.json()["jobid"]
        for _ in range(120):
            st = c.get(f"/jobs/{jid}").json()
            verts = st.get("vertices", [])
            if st.get("state") == "RUNNING" and verts and all(v.get("status") == "RUNNING" for v in verts):
                break
            if st.get("state") in ("FAILED", "CANCELED"):
                raise RuntimeError(f"flink job {want} {st.get('state')}")
            time.sleep(0.5)
        log(f"flink: {want} running (job {jid[:8]}, parallelism {par}{', restored from savepoint' if savepoint else ''})")
        return jid
