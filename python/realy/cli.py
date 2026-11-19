"""realy command line: `realy --help`."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer
import yaml

from .cli_world import world_app
from .control.cli import console

app = typer.Typer(no_args_is_help=True, add_completion=False,
                  help="Deterministic benchmarking & correctness instrument for real-time fraud pipelines.")
app.add_typer(world_app, name="world", help="Synthetic payments world: build, profile, compile datasets.")
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
        run_id: Optional[str] = typer.Option(None)):
    """Run a scenario end to end and write runs/<run_id>/report.{json,md,html}."""
    from .runner.run import run_scenario
    from .scenario import load_scenario
    sc = load_scenario(scenario, _overrides(set_))
    res = run_scenario(sc, collectors=collectors, run_id=run_id)
    verdict = res.get("report", {}).get("verdict")
    exp = sc.expect.get("verdict")
    typer.echo(f"report: {res['run_dir']}/report.html")
    if exp:
        raise typer.Exit(0 if exp == verdict else 1)
    raise typer.Exit(0 if verdict == "PASS" else 1)


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
          input_dim: int = typer.Option(0, help="pad the tensor to the production model width (0 = schema size)")):
    """Train structural proxy models (MLP->ONNX/Triton + weights JSON, XGBoost->Triton FIL) on synthetic data."""
    from .train.proxy import train as do_train
    meta = do_train(run, hidden=tuple(int(x) for x in hidden.split(",") if x), trees=trees, depth=depth,
                    input_dim=input_dim or None)
    typer.echo(json.dumps({k: meta[k] for k in ("version", "rows", "fraud_rate", "input_dim", "metrics", "parity", "files")}, indent=1))


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


@app.command()
def scenarios():
    """List available scenarios."""
    from .scenario import SCENARIO_DIR
    for p in sorted(SCENARIO_DIR.glob("*.yaml")):
        d = yaml.safe_load(p.read_text())
        typer.echo(f"{p.stem:26s} {d.get('topology', ''):14s} {d.get('description', '')[:80]}")


if __name__ == "__main__":
    app()
