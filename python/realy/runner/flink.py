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
# Retained checkpoints (execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION):
# FLINK_CHECKPOINTS_URI is the directory as Flink sees it, FLINK_CHECKPOINTS_LOCAL the same directory as
# this process sees it (compose mounts the volume read-only into the console). A job lost with its
# JobManager (no HA) is restored from its newest retained checkpoint instead of starting empty.
CHECKPOINTS_URI = os.environ.get("FLINK_CHECKPOINTS_URI", "file:///checkpoints")
CHECKPOINTS_LOCAL = os.environ.get("FLINK_CHECKPOINTS_LOCAL", "")


def latest_retained_checkpoint(jid: str, local: str | None = None, uri: str | None = None) -> str | None:
    """The newest complete retained checkpoint of job `jid` (a `chk-N` directory holding `_metadata`), as
    a path Flink can restore from, or None."""
    root = Path(local if local is not None else CHECKPOINTS_LOCAL)
    if not str(root) or not jid or not (root / jid).is_dir():
        return None
    best = None
    for d in (root / jid).glob("chk-*"):
        try:
            n = int(d.name.split("-", 1)[1])
        except (IndexError, ValueError):
            continue
        if (d / "_metadata").exists() and (best is None or n > best[0]):
            best = (n, d.name)
    if best is None:
        return None
    return f"{(uri or CHECKPOINTS_URI).rstrip('/')}/{jid}/{best[1]}"


# Provenance of the running job's keyed state: which world it was built from and which consumer
# group the job commits to. Bound to the job id, so a stale record (job restarted by hand, Redis
# flushed, the JobManager replaced) reads as "unknown" and never as "the same world".
JOB_KEY = "realy:flink-job:{pipeline}"


def _job_key() -> str:
    from .lifecycle import pipeline
    return JOB_KEY.format(pipeline=pipeline())


LINEAGE_KEEP = 50   # run ids kept on the record (the count is exact beyond that)


def stateful_uids(mode: str | None = None) -> list[str]:
    """Operator uids holding keyed state in the job FraudJob builds from the feature schema (one keyed
    stage per entity with stream features, `features-<entity>`)."""
    from ..features import load_schema
    return [f"features-{e}" for e in load_schema().stream_entities()]


def record_job(jid: str, group: str, world: str | None, r=None, lineage: dict | None = None,
               uids: list[str] | None = None) -> None:
    """`lineage` is which runs fed events into this state since it last started fresh:
    {"runs": [run ids, oldest first, at most LINEAGE_KEEP], "count": n}. A fresh job starts it empty;
    a job restored from a savepoint carries the previous job's on. `uids` are its stateful operators:
    a later job restored from its savepoint must have every one of them, or their state is dropped."""
    from .seed import redis_client
    lin = lineage or {"runs": [], "count": 0}
    (r or redis_client()).set(_job_key(), json.dumps({"jid": jid, "group": group, "world": world, "at": time.time(),
                                                      "lineage": lin, "uids": uids if uids is not None else stateful_uids()}))


def dropped_state(prev: dict | None, uids: list[str]) -> list[str]:
    """Stateful operators of the recorded job that the new job graph would not have: restoring its
    savepoint with allowNonRestoredState would silently drop their state."""
    return sorted(set((prev or {}).get("uids") or []) - set(uids))


def note_run(run_id: str, r=None) -> dict | None:
    """Add a run to the running job's state lineage, before it sends any event, and return the lineage
    as it was BEFORE (the runs whose events this run's features also see). None without a record."""
    from .seed import redis_client
    r = r or redis_client()
    rec = job_record(r)
    if not rec:
        return None
    before = rec.get("lineage") or {"runs": [], "count": 0, "unknown": True}
    runs = [x for x in before.get("runs", []) if x != run_id] + [run_id]
    rec["lineage"] = {"runs": runs[-LINEAGE_KEEP:], "count": int(before.get("count", 0)) + 1,
                      **({"unknown": True} if before.get("unknown") else {})}
    r.set(_job_key(), json.dumps(rec))
    return before


def job_record(r=None) -> dict | None:
    from .seed import redis_client
    raw = (r or redis_client()).get(_job_key())
    if not raw:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return None


def state_provenance(rest: str | None, r=None) -> dict:
    """What the pipeline's feature state is: {"running": jid or None, "world": hash or None (unknown),
    "group": consumer group or None, "why": text}. "world" is known only when the job running now is
    the one the record was written for."""
    out: dict = {"running": None, "world": None, "group": None, "why": ""}
    if not rest:
        out["why"] = "no Flink REST endpoint"
        return out
    with httpx.Client(base_url=rest, timeout=10) as c:
        running = _running(c)
    if not running:
        # the job is gone (JobManager restarted or crashed, cancelled by hand): its state is still known
        # if it left a retained checkpoint and the online store still holds the blobs it wrote
        rec = job_record(r)
        ckpt = latest_retained_checkpoint(rec.get("jid")) if rec and rec.get("jid") else None
        if ckpt and _store_has_stream_blobs(r):
            out.update(world=rec.get("world"), group=rec.get("group"), recoverable=ckpt,
                       lineage=rec.get("lineage") or {"runs": [], "count": 0, "unknown": True},
                       why=f"job {rec['jid'][:8]} is gone; its retained checkpoint is restorable")
            return out
        out["why"] = "no feature job running" + (" (its retained checkpoint exists but the online store lost its "
                                                 "blobs)" if ckpt else "")
        return out
    out["running"] = running[0]["jid"]
    rec = job_record(r)
    if not rec:
        out["why"] = f"job {out['running'][:8]} has no provenance record"
    elif rec.get("jid") != out["running"]:
        out["why"] = f"the provenance record is for job {str(rec.get('jid'))[:8]}, job {out['running'][:8]} is running"
    else:
        out["world"], out["group"] = rec.get("world"), rec.get("group")
        # records written before lineage existed: which runs built the state is unknown
        out["lineage"] = rec.get("lineage") or {"runs": [], "count": 0, "unknown": True}
        out["why"] = "recorded"
    return out


def _store_has_stream_blobs(r=None) -> bool:
    """Whether the online store still holds Flink-written stream blobs (a restarted Redis without
    persistence holds none: restoring Flink state alone would leave the gateway reading nothing)."""
    try:
        from .seed import redis_client
        r = r or redis_client()
        cursor, keys = r.scan(0, match="fs:*", count=20000)
        return bool(keys) or bool(cursor and r.scan(cursor, match="fs:*", count=20000)[1])
    except Exception:  # noqa: BLE001
        return False


def job_progress(rest: str | None, jid: str | None = None) -> dict | None:
    """The feature job's own view of its progress, from the Flink REST API: state, start time and the
    records that have entered its LAST keyed stage (`read-records` of the last `features:*` vertex),
    i.e. events that went through every earlier stage. Unlike committed Kafka offsets (written only
    when a checkpoint completes, so they trail by a checkpoint or far more under backpressure), this
    moves continuously. None when no job runs or Flink is unreachable."""
    if not rest:
        return None
    try:
        with httpx.Client(base_url=rest, timeout=10) as c:
            running = _running(c)
            if not running:
                return None
            j = next((x for x in running if x["jid"] == jid), running[0]) if jid else running[0]
            d = c.get(f"/jobs/{j['jid']}").json()
    except (httpx.HTTPError, ValueError, KeyError):
        return None
    verts = d.get("vertices") or []
    feats = [v for v in verts if str(v.get("name", "")).startswith("features:")]
    last = feats[-1] if feats else (verts[-1] if verts else {})
    return {"jid": j["jid"], "name": j.get("name"), "state": d.get("state"), "start_ms": d.get("start-time"),
            "processed": int((last.get("metrics") or {}).get("read-records") or 0), "stage": last.get("name"),
            "vertices_running": sum(1 for v in verts if v.get("status") == "RUNNING"), "vertices": len(verts)}


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
                force: bool = False, run_id: str | None = None, world: str | None = None) -> str | None:
    """Make the job the topology needs run. A job it starts is recorded (job id, consumer group,
    `world`) once RUNNING, so the next run can tell whose state it holds; a job kept as is keeps its
    record."""
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
        try:
            prev = job_record()
        except Exception:  # noqa: BLE001
            prev = None
        prev_ids = {j["jid"] for j in running}
        # state kept (savepoint or surviving blobs) keeps the runs that built it; a fresh job starts with none
        carried = ((prev or {}).get("lineage") or {"runs": [], "count": 0, "unknown": True}) \
            if prev and prev.get("jid") in prev_ids else {"runs": [], "count": 0, "unknown": True}
        for j in running:
            if fresh:
                log(f"flink: cancelling {j['name']} (fresh state requested)")
                _cancel(c, j["jid"])
                continue
            log(f"flink: stopping {j['name']} with savepoint{' (new job jar)' if new_code else ''}")
            savepoint = _stop_with_savepoint(c, j["jid"], log) or savepoint
        recovered = None
        if not running and not fresh and prev and prev.get("jid"):
            # the job is gone without a savepoint (JobManager restarted, job cancelled by hand): restore
            # its newest retained checkpoint, so the keyed state and the runs that built it carry over
            recovered = latest_retained_checkpoint(prev["jid"])
            if recovered:
                savepoint = recovered
                carried = (prev.get("lineage") or {"runs": [], "count": 0, "unknown": True})
                log(f"flink: no job running; restoring job {prev['jid'][:8]} from its retained checkpoint {recovered}")
        lost = dropped_state(prev, stateful_uids()) if savepoint else []
        if lost:
            # allowNonRestoredState would drop these operators' state without a word: start fresh instead,
            # so the lineage and the provenance say what really happened
            log(f"flink: the new job graph has no {', '.join(lost)}: their state cannot be restored, starting fresh")
            savepoint, recovered, fresh = None, None, True
            carried = {"runs": [], "count": 0}
            from .seed import clear_stream_blobs
            log(f"flink: cleared {clear_stream_blobs():,} online-store blobs of the old state")
        group = (f"realy-flink-{run_id}" if fresh and run_id
                 else (prev.get("group") if recovered and prev.get("group") else "realy-flink"))
        args = ["--mode", mode, "--parallelism", str(par), "--bootstrap", os.environ.get("FLINK_KAFKA_BOOTSTRAP", "kafka:29092"),
                "--group", group, "--scoring-config", scoring,
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
        how = (", restored from its retained checkpoint" if recovered else ", restored from savepoint") if savepoint else ""
        log(f"flink: {want} running (job {jid[:8]}, parallelism {par}{how})")
        try:
            # fresh: Flink state cancelled and the runner cleared the stream blobs. Otherwise (savepoint
            # restore, or a failed savepoint whose blobs survive in the store) the earlier runs remain.
            record_job(jid, group, world, lineage={"runs": [], "count": 0} if fresh else carried)
        except Exception as e:  # noqa: BLE001 - without a record the next run starts fresh: safe
            log(f"warn: could not record the job's state provenance ({e.__class__.__name__})")
        return jid
