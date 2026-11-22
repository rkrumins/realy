"""PulseBench command line: `pulsebench --help` (`realy` is a legacy alias)."""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Optional

import typer
import yaml

from .cli_world import world_app
from .control.cli import console

app = typer.Typer(no_args_is_help=True, add_completion=False,
                  help="Deterministic benchmarking & correctness instrument for real-time fraud pipelines.")
app.add_typer(world_app, name="world", help="Synthetic payments world: build, profile, compile datasets.")
infra_app = typer.Typer(no_args_is_help=True, help="The infrastructure a run uses: preflight checks, real or mock.")
app.add_typer(infra_app, name="infra")


@infra_app.command("check")
def infra_check(scenario: Optional[str] = typer.Argument(None, help="scenario (its topology and simulated settings)"),
                topology: Optional[str] = typer.Option(None, help="check for this topology instead of a scenario's"),
                json_: bool = typer.Option(False, "--json", help="machine-readable output")) -> None:
    """Probe every component the topology uses, through the settings the services read; exit 1 on a
    failed check (the run would measure or verify the wrong thing)."""
    import json as _json

    from .infra import check, render
    from .scenario import load_scenario
    sc = load_scenario(scenario) if scenario else None
    res = check(sc, topology)
    typer.echo(_json.dumps(res, indent=1, default=str) if json_ else render(res))
    raise typer.Exit(1 if res["status"] == "fail" else 0)

app.command(help="Operator console (FastAPI + React UI) for configuring and running sustained tests.")(console)


def _overrides(sets: list[str]) -> dict:
    out: dict = {}
    for s in sets or []:
        k, _, v = s.partition("=")
        cur = out
        parts = k.split(".")
        for p in parts[:-1]:
            cur = cur.setdefault(p, {})
        cur[parts[-1]] = yaml.safe_load(v)
    return out


@app.command()
def run(scenario: str = typer.Argument(..., help="scenario name (scenarios/<name>.yaml) or path"),
        set_: list[str] = typer.Option([], "--set", help="override, e.g. --set traffic.profile.tps=2000"),
        collectors: Optional[int] = typer.Option(None, help="collector processes (default: 1 per 15k TPS)"),
        run_id: Optional[str] = typer.Option(None),
        wait_lease: float = typer.Option(0.0, help="seconds to wait when another run holds the pipeline lease"),
        verify_: bool = typer.Option(False, "--verify", help="verify exactly afterwards (starts from fresh "
                                     "feature state when the warm state holds other runs' events)")):
    """Run a scenario end to end and write runs/<run_id>/report.{json,md,html}.

    Ctrl-C / SIGTERM abort gracefully: emission stops, decisions in flight drain, and a partial report
    (verdict ABORTED) is still written. A second Ctrl-C exits at once. Exit codes: 0 PASS (or the
    scenario's expected verdict), 1 FAIL, 2 ABORTED, 3 pipeline busy (another run holds the lease)."""
    import signal

    from .runner.lifecycle import PipelineBusy
    from .runner.run import RUNS_DIR, RunHandle, new_run_id, run_scenario
    from .scenario import load_scenario
    sc = load_scenario(scenario, _overrides(set_))
    rid = run_id or new_run_id(sc)
    handle = RunHandle(rid, RUNS_DIR / rid)

    def on_signal(signum, _frame):
        name = signal.Signals(signum).name
        if handle.abort_reason:
            typer.echo(f"\n{name} again: exiting without waiting for the drain", err=True)
            raise SystemExit(130)
        typer.echo(f"\n{name}: aborting {rid} (draining, writing a partial report; {name} again to force)", err=True)
        handle.request_abort(f"{name} received", "signal")
    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)
    try:
        res = run_scenario(sc, collectors=collectors, run_id=rid, handle=handle, wait_lease_s=wait_lease,
                           verifiable=verify_)
    except PipelineBusy as e:
        typer.echo(f"error: {e}. Wait (--wait-lease SECONDS), or abort it: realy abort {e.holder.get('run_id')}", err=True)
        raise typer.Exit(3)
    verdict = res.get("report", {}).get("verdict")
    if verify_ and verdict != "ABORTED":
        from .verify.verify import render, verify_run
        v = verify_run(Path(res["run_dir"]))
        typer.echo(render(v))
        if not v.get("passed"):
            typer.echo(f"report: {res['run_dir']}/report.html")
            raise typer.Exit(1)
    exp = sc.expect.get("verdict")
    typer.echo(f"report: {res['run_dir']}/report.html")
    if verdict == "ABORTED":
        raise typer.Exit(2)
    if exp:
        raise typer.Exit(0 if exp == verdict else 1)
    raise typer.Exit(0 if verdict == "PASS" else 1)


@app.command()
def abort(run_ids: list[str] = typer.Argument(None, help="run ids to abort (any process: console, CLI, bench Job)"),
          all_: bool = typer.Option(False, "--all", help="abort the pipeline's lease holder and every run listening on bench.control"),
          console: Optional[str] = typer.Option(None, help="console URL: also cancel its queue and pause its schedules (with --all)"),
          reason: str = typer.Option("aborted by operator (cli)")):
    """Abort runs wherever they execute. Two channels: a Redis flag (seen within ~1 s by the run's lease
    heartbeat) and a bench.control message; either alone is enough."""
    from .runner.lifecycle import holder, request_abort
    targets = list(run_ids or [])
    if all_:
        h = holder()
        if h and h.get("run_id") not in targets:
            targets.append(h["run_id"])
    if not targets and not all_:
        typer.echo("nothing to abort: give run ids or --all", err=True)
        raise typer.Exit(1)
    for rid in targets:
        ch = request_abort(rid, reason, log=lambda m: typer.echo(m, err=True))
        typer.echo(f"{rid}: abort requested via {', '.join(k for k, v in ch.items() if v) or 'NO channel (unreachable)'}")
    if all_:
        ch = request_abort("", reason)   # wildcard on bench.control: every run listening
        typer.echo(f"wildcard abort on bench.control: {'published' if ch['kafka'] else 'FAILED'}")
        if console:
            import httpx
            r = httpx.post(console.rstrip("/") + "/api/stop-all", json={"reason": reason}, timeout=10)
            typer.echo(f"console stop-all: HTTP {r.status_code} {r.text[:300]}")


@app.command()
def status(console: Optional[str] = typer.Option(None, help="console URL, to include its queue"),
           as_json: bool = typer.Option(False, "--json")):
    """Who holds the pipeline right now (lease), and the console's current run and queue."""
    from .runner.lifecycle import holder, pipeline
    out: dict = {"pipeline": pipeline(), "lease": holder()}
    if console:
        import httpx
        try:
            out["console"] = httpx.get(console.rstrip("/") + "/api/active", timeout=5).json()
        except Exception as e:  # noqa: BLE001
            out["console"] = {"error": str(e)}
    if as_json:
        typer.echo(json.dumps(out, indent=1, default=str))
        return
    h = out["lease"]
    typer.echo(f"pipeline '{out['pipeline']}': " + (f"BUSY - run {h['run_id']} ({h.get('scenario')}, phase "
               f"{h.get('phase')}, owner {h.get('owner')}, lease expires in {h.get('expires_in_s')} s)" if h else "free"))
    c = out.get("console") or {}
    if c and "error" not in c:
        cur = c.get("local")
        typer.echo(f"console: {'running ' + cur['run_id'] + ' (' + cur['phase'] + ')' if cur else 'idle'}; "
                   f"{len(c.get('queue') or [])} queued")


@app.command()
def report(run_dir: Path):
    """(Re)build the report for a finished run directory."""
    from .report.report import build_report
    rep = build_report(run_dir)
    typer.echo(rep["text"])


@app.command()
def verify(run_dir: Path, clickhouse: Optional[str] = typer.Option(None, help="ClickHouse HTTP URL"),
           max_events: int = typer.Option(1_000_000, help="above this, verify a customer sample")):
    """Independent correctness checks: features, CEL rule parity, score/outcome parity, ClickHouse audit."""
    from .verify.verify import render, verify_run
    v = verify_run(run_dir, clickhouse=clickhouse, max_events=max_events)
    typer.echo(render(v))
    from .report.history import store
    store(run_dir, clickhouse, log=lambda m: typer.echo(m, err=True))
    raise typer.Exit(0 if v.get("passed") else 1)


@app.command()
def budget(scenario: str, set_: list[str] = typer.Option([], "--set"), samples: int = 200_000,
           as_json: bool = typer.Option(False, "--json")):
    """Monte Carlo feasibility of the scenario's latency budget (before running anything)."""
    from .budget.check import check, render
    from .scenario import load_scenario
    r = check(load_scenario(scenario, _overrides(set_)), n=samples)
    typer.echo(json.dumps(r, indent=1) if as_json else render(r))
    raise typer.Exit(0 if r["verdict"] != "INFEASIBLE" else 2)


def _run_dir(ident: str) -> Path:
    p = Path(ident)
    if p.is_dir():
        return p
    from .runner.run import RUNS_DIR
    if (RUNS_DIR / ident).is_dir():
        return RUNS_DIR / ident
    raise typer.BadParameter(f"no run directory {ident!r} (path or run id under {RUNS_DIR})")


@app.command()
def size(tps: float = typer.Option(10_000, help="steady-state events/s"),
         peak: float = typer.Option(2.0, help="peak factor: the plan is sized for tps x peak"),
         headroom: float = typer.Option(0.55, help="target utilisation of CPU-bound components"),
         from_run: str = typer.Option("", "--from", help="run dir / id whose measured costs replace the assumptions"),
         zones: int = typer.Option(3, help="zones (N+1 and rack-aware Kafka placement)"),
         customers: int = typer.Option(10_000_000, help="active customers (state + online-store memory)"),
         retention_days: int = typer.Option(30, help="ClickHouse audit retention"),
         set_: list[str] = typer.Option([], "--set", help="override a cost, e.g. --set flink_cpu_ms=0.8"),
         as_json: bool = typer.Option(False, "--json"),
         write_overlay: Path = typer.Option(None, help="write a kustomize overlay (on top of overlays/gke-l4) here")):
    """Capacity plan on GKE (+L4) for a target load, from measured per-event costs or documented assumptions."""
    from .sizing import Inputs, plan, render
    from .sizing import write_overlay as write
    overrides = {k: float(v) for k, v in (kv.split("=", 1) for kv in set_)}
    p = plan(Inputs(tps=tps, peak=peak, headroom=headroom, zones=zones, customers=customers,
                    retention_days=retention_days, run=_run_dir(from_run) if from_run else None, overrides=overrides))
    typer.echo(json.dumps(p, indent=1) if as_json else render(p))
    if write_overlay:
        typer.echo(f"overlay: {write(p, write_overlay)}", err=as_json)
    raise typer.Exit(0 if p["latency"].get("verdict") != "INFEASIBLE" else 2)


@app.command()
def gate(run: str = typer.Argument(..., help="candidate run dir / id"),
         baseline: str = typer.Option("", help="baseline run dir / id, or a baseline JSON (baselines/<scenario>.json)"),
         save_baseline: Path = typer.Option(None, help="write RUN as a baseline file here and exit"),
         p99_regress: float = typer.Option(0.10, help="max relative p99 increase"),
         min_delta_ms: float = typer.Option(5.0, help="...and only beyond this many ms (noise floor)"),
         within_drop: float = typer.Option(0.01, help="max drop of the share of events within budget"),
         knee_drop: float = typer.Option(0.10, help="max relative drop of the knee TPS"),
         recall_drop: float = typer.Option(0.02), precision_drop: float = typer.Option(0.05),
         fpr_rise: float = typer.Option(0.01), cpu_regress: float = typer.Option(0.15, help="max CPU/event increase"),
         max_duplicates: int = typer.Option(0), require_verify: bool = typer.Option(False),
         as_json: bool = typer.Option(False, "--json")):
    """Fail a run that regressed: verdict, loss, verify, and p99 / knee / detection / CPU-per-event vs a baseline."""
    from .report.gate import Thresholds, gate as do_gate, load_baseline, render, save_baseline as save
    run_dir = _run_dir(run)
    if save_baseline:
        typer.echo(f"baseline written: {save(run_dir, save_baseline)}")
        raise typer.Exit(0)
    base = None
    if baseline:
        bp = Path(baseline)
        base = load_baseline(bp if bp.exists() else _run_dir(baseline))
    g = do_gate(run_dir, base, Thresholds(p99_regress=p99_regress, min_delta_ms=min_delta_ms, within_drop=within_drop,
                                         knee_drop=knee_drop, recall_drop=recall_drop, precision_drop=precision_drop,
                                         fpr_rise=fpr_rise, cpu_regress=cpu_regress, max_duplicates=max_duplicates,
                                         require_verify=require_verify))
    typer.echo(json.dumps(g, indent=1) if as_json else render(g))
    raise typer.Exit(0 if g["passed"] else 1)


@app.command()
def train(run: list[Path] = typer.Option(..., "--run", help="finished run dir(s) to train on (served features + labels)"),
          hidden: str = typer.Option("64,32", help="MLP hidden layer sizes"),
          trees: int = 300, depth: int = 6,
          input_dim: int = typer.Option(0, help="pad the tensor to the production model width (0 = schema size)"),
          labels: str = typer.Option("observed", help="observed: labels as known at training time (delayed, "
                                     "unreported fraud missing, friendly fraud included) | ground-truth: the generator's truth"),
          label_maturity_days: float = typer.Option(30.0, help="observed labels: train this many days after the last event"),
          deploy: bool = typer.Option(False, help="hot-deploy the new MLP to the mock model server afterwards")):
    """Train structural proxy models (MLP->ONNX/Triton + weights JSON, XGBoost->Triton FIL) on served features.

    Evaluate the result on a run of ANOTHER world (different seed): reports flag runs scored by a model
    trained on their own world as in-sample."""
    from .train.proxy import train as do_train
    # progress on stderr: stdout is the JSON result only (scripts parse it)
    meta = do_train(run, hidden=tuple(int(x) for x in hidden.split(",") if x), trees=trees, depth=depth,
                    input_dim=input_dim or None, labels=labels, maturity_days=label_maturity_days,
                    log=lambda m: typer.echo(m, err=True))
    typer.echo(json.dumps({k: meta.get(k) for k in ("version", "rows", "fraud_rate", "label_stats", "input_dim", "metrics",
                                                    "parity", "weights_path")}, indent=1))
    if deploy:
        _deploy_model(meta["weights_path"])


def _deploy_model(weights_path: str) -> None:
    import httpx

    from .common import control as ctl
    body = {"weights_path": weights_path}
    url = os.environ.get("MODEL_ADMIN", "http://localhost:8002").rstrip("/") + "/admin/config"
    r = httpx.get(url, timeout=5)
    r.raise_for_status()
    cfg = {**r.json(), **body}
    r = httpx.put(url, content=json.dumps(cfg), timeout=10)
    if r.status_code >= 300:
        typer.echo(f"model server rejected {weights_path}: HTTP {r.status_code} {r.text[:300]}", err=True)
        raise typer.Exit(1)
    ctl.publish([ctl.control_message("", "model", model_server=cfg)])     # every other replica
    typer.echo(f"deployed {weights_path}: model server now serves {r.json().get('model_version')}")


@app.command("deploy-model")
def deploy_model(weights: Path = typer.Argument(..., help="proxy weights JSON (e.g. /models/proxy/versions/<version>.weights.json)")):
    """Hot-swap the mock model server's weights (no restart): retrain -> redeploy -> measure."""
    _deploy_model(str(weights))


@app.command()
def topics(replication: int = typer.Option(1)):
    """Create the contract topics (contracts/kafka/topics.yaml)."""
    from .common.kafka import ensure_topics
    typer.echo(f"created: {ensure_topics(replication=replication)}")


@app.command()
def seed(scenario: str = "smoke", force: bool = False):
    """Seed online-store profile blobs for the scenario's world."""
    from .runner.seed import seed_profiles
    from .scenario import load_scenario
    from .world import load_or_build
    sc = load_scenario(scenario)
    w = load_or_build(sc.world, sc.seed, sc.world_dir)
    typer.echo(f"seeded {seed_profiles(w, force=force):,} profiles")


@app.command()
def control(target: str = typer.Argument(..., help="gateway | model | emitter | flink | *"),
            run_id: str = typer.Option("", help="run id the change applies to"),
            tps: Optional[float] = typer.Option(None, help="emitter: new absolute TPS"),
            scoring: Optional[str] = typer.Option(None, help="ScoringConfig proto3 JSON / YAML"),
            model_server: Optional[str] = typer.Option(None, help="ModelServerConfig proto3 JSON / YAML"),
            kafka_ingress: Optional[bool] = typer.Option(None, help="gateway: enable/disable Kafka ingress"),
            abort: bool = typer.Option(False, help="abort the run")):
    """Publish a live control message on bench.control."""
    from .common import control as ctl
    msgs = []
    if tps is not None:
        msgs.append(ctl.control_message(run_id, target, rate={"tps": tps}))
    if scoring:
        msgs.append(ctl.control_message(run_id, target, scoring=yaml.safe_load(scoring)))
    if model_server:
        msgs.append(ctl.control_message(run_id, target, model_server=yaml.safe_load(model_server)))
    if kafka_ingress is not None:
        msgs.append(ctl.control_message(run_id, target, ingress={"kafka_enabled": kafka_ingress}))
    if abort:
        msgs.append(ctl.control_message(run_id, target, phase=6))
    ctl.publish(msgs)
    typer.echo(f"published {len(msgs)} control message(s)")


store_app = typer.Typer(help="Inspect the online feature store (Redis).")
app.add_typer(store_app, name="store")


@store_app.command("get")
def store_get(entity: str = typer.Argument(..., help="customer | card | device | ip | merchant | terminal | payee"),
              entity_id: int = typer.Argument(...), as_json: bool = typer.Option(False, "--json")):
    """Decode fs:<entity>:<id> (written by Flink) and pf:<entity>:<id> (profile) into named features."""
    from .runner.store_inspect import get, render
    res = get(entity, entity_id)
    typer.echo(json.dumps(res, indent=1) if as_json else render(res))


@app.command("rules-whatif")
def rules_whatif(run_dir: Path = typer.Argument(..., help="finished run (served features + scores + ground truth)"),
                 rules: list[Path] = typer.Option([], "--rules", help="candidate rule set YAML (repeatable)"),
                 review: Optional[float] = typer.Option(None, help="override the review threshold for every variant"),
                 decline: Optional[float] = typer.Option(None, help="override the decline threshold for every variant"),
                 challenge: Optional[float] = typer.Option(None, help="override the step-up (CHALLENGE) threshold for every variant"),
                 sample_mod: int = typer.Option(1, help="evaluate events with id % N == 0 only (speed)"),
                 model: list[Path] = typer.Option([], "--model", help="also re-score the served vectors with these "
                                                  "proxy weights (repeatable): model comparison on identical traffic"),
                 as_json: bool = typer.Option(False, "--json")):
    """Replay rule sets / thresholds over a finished run offline: precision, recall, FPR, step-ups and their
    outcomes, $ approved per variant and per rule, against the run's own policy. Tune on a training run,
    confirm on the evaluation run."""
    from .report.whatif import evaluate, load, render
    from .scenario import Scenario
    sc = Scenario.model_validate(json.loads((run_dir / "scenario.json").read_text()))
    data = load(run_dir, sample_mod)
    from .scenario import run_rules
    as_run = run_rules(run_dir, sc)[0]
    rows = [(f"as run ({sc.rules})", evaluate(data, as_run, review, decline, challenge))]
    for r in rules:
        rows.append((r.stem, evaluate(data, r.read_text(), review, decline, challenge)))
    if model:
        from .report.whatif import rescore
        for w in model:
            d2 = rescore(data, w)
            rows.append((f"model {d2['model_version']}", evaluate(d2, as_run, review, decline, challenge)))
    typer.echo(json.dumps(dict(rows), indent=1) if as_json else render(rows))


@app.command("live-test")
def live_test(action: str = typer.Argument("run", help="run | fault-start | fault-check"),
              args: list[str] = typer.Argument(None, help="fault-check: JOB_ID RUN_ID"),
              console: str = typer.Option(os.environ.get("REALY_CONSOLE_URL", "http://localhost:8090")),
              case: list[str] = typer.Option([], help="run only these cases (repeatable); default: all"),
              expect: str = typer.Option("zero-loss", help="fault-check: zero-loss | aborted | interrupted"),
              duration: float = typer.Option(90, help="fault-start: run duration (s)"),
              out: Optional[Path] = typer.Option(None, help="write results JSON here")):
    """Live acceptance suite against a running stack, through the console API: launch, steer, report
    accuracy (vs the ClickHouse audit), abort in every phase and from every process, bulk control,
    guardrails, dead instrument processes, signals and recovery. Exit 0 only if every check passes."""
    from .acceptance.live import Suite
    s = Suite(console=console)
    if action == "fault-start":
        typer.echo(s.fault_start(duration))
        return
    if action == "fault-check":
        ok = s.fault_check(args[0], args[1], expect)
    else:
        ok = s.run_cases(case or None)
    typer.echo(s.render())
    if out or action == "run":
        s.save(out or s.runs_dir / f"live-test-{time.strftime('%Y%m%d-%H%M%S')}.json")
    raise typer.Exit(0 if ok else 1)


@app.command()
def scenarios():
    """List available scenarios."""
    from .scenario import SCENARIO_DIR
    for p in sorted(SCENARIO_DIR.glob("*.yaml")):
        d = yaml.safe_load(p.read_text())
        typer.echo(f"{p.stem:26s} {d.get('topology', ''):14s} {d.get('description', '')[:80]}")


if __name__ == "__main__":
    app()
