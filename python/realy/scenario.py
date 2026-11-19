"""Scenario schema: one YAML file fully describes a reproducible benchmark run.

The `scoring` and `model_server` sections are proto3-JSON of realy.bench.v1.ScoringConfig /
ModelServerConfig, so the exact same document is pushed to the Java services (admin HTTP or the
`bench.control` topic) without translation.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

import os as _os

# REALY_HOME: directory holding contracts/, rules/, scenarios/, models/, docs/ (the repository root in a
# checkout; /opt/realy in the container images). REALY_DATA: synthetic-world cache. REALY_SCENARIOS:
# writable scenario directory (the console's builder saves there).
REPO_ROOT = Path(_os.environ.get("REALY_HOME") or Path(__file__).resolve().parents[2])
SCENARIO_DIR = Path(_os.environ.get("REALY_SCENARIOS") or REPO_ROOT / "scenarios")
DATA_DIR = Path(_os.environ.get("REALY_DATA") or REPO_ROOT / "data")

Topology = Literal["gateway-kafka", "gateway-grpc", "flink-inline", "echo-kafka"]
FraudKind = Literal[
    "card_testing", "account_takeover", "lost_stolen", "counterfeit", "merchant_breach",
    "atm_cashout", "app_scam_mule", "bust_out", "bot_ecom",
]
FRAUD_KINDS: tuple[str, ...] = FraudKind.__args__  # type: ignore[attr-defined]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class WorldSpec(_Model):
    customers: int = 200_000
    merchants: int = 20_000
    payees: int = 50_000
    mule_fraction: float = 0.002
    dir: str | None = None  # default: data/world-<customers>-<seed>


class RateProfile(_Model):
    """Wall-clock target rate as a function of time since run start."""
    kind: Literal["constant", "ramp", "step", "spike", "sine"] = "constant"
    tps: float = 1000.0              # constant / base rate
    start_tps: float | None = None   # ramp/step start
    end_tps: float | None = None     # ramp end
    step_tps: float | None = None    # step increment
    step_duration_s: float | None = None
    spike_at_s: float | None = None
    spike_duration_s: float | None = None
    spike_multiplier: float = 3.0
    sine_period_s: float = 600.0
    sine_amplitude: float = 0.3

    def rate_at(self, t: float) -> float:
        k = self.kind
        if k == "constant":
            return self.tps
        if k == "ramp":
            s = self.start_tps if self.start_tps is not None else 0.0
            e = self.end_tps if self.end_tps is not None else self.tps
            return s + (e - s) * min(1.0, max(0.0, t / max(self._duration, 1e-9)))
        if k == "step":
            s = self.start_tps if self.start_tps is not None else self.tps
            inc = self.step_tps if self.step_tps is not None else s
            d = self.step_duration_s or 60.0
            return s + inc * int(t // d)
        if k == "spike":
            at = self.spike_at_s or 0.0
            dur = self.spike_duration_s or 30.0
            return self.tps * (self.spike_multiplier if at <= t < at + dur else 1.0)
        if k == "sine":
            import math
            return self.tps * (1.0 + self.sine_amplitude * math.sin(2 * math.pi * t / self.sine_period_s))
        raise ValueError(k)

    _duration: float = 0.0  # set by Scenario for ramps

    def step_index(self, t: float) -> int:
        if self.kind == "step":
            return int(t // (self.step_duration_s or 60.0))
        return 0


class TrafficSpec(_Model):
    profile: RateProfile = Field(default_factory=RateProfile)
    arrivals: Literal["uniform", "poisson"] = "uniform"
    duration_s: float = 60.0
    warmup_s: float = 10.0
    # Simulated seconds per wall second. "auto" = the population's natural rate maps onto the
    # target TPS, so each customer behaves realistically (rules fire at realistic rates).
    time_scale: float | Literal["auto"] = "auto"
    generators: int = 1              # emitter processes (shards of the customer population)
    sim_start: str = "2026-11-02T00:00:00Z"
    backfill_days: float = 0.0       # unpaced history replay before the measured run


class CampaignSpec(_Model):
    kind: FraudKind
    at_s: float = 0.0                # wall seconds after run start
    count: int = 1                   # number of campaigns started at `at_s`
    params: dict[str, Any] = Field(default_factory=dict)


class FraudSpec(_Model):
    # Background campaigns started per 1,000 legitimate events (Poisson), and their mix.
    background_per_1k: float = 0.5
    mix: dict[FraudKind, float] = Field(default_factory=lambda: {
        "card_testing": 0.22, "account_takeover": 0.15, "lost_stolen": 0.12, "counterfeit": 0.10,
        "merchant_breach": 0.08, "atm_cashout": 0.03, "app_scam_mule": 0.12, "bust_out": 0.06,
        "bot_ecom": 0.12})
    campaigns: list[CampaignSpec] = Field(default_factory=list)
    unreported_fraction: float = 0.10       # fraud never labelled
    friendly_fraud_per_1k: float = 0.2      # legit events charged back (label noise)
    chargeback_delay_days: tuple[float, float] = (1.0, 45.0)


class BudgetSpec(_Model):
    e2e_p99_ms: float = 150.0        # Kafka-to-Kafka (async) / request-to-response (sync)
    hot_path_p99_ms: float = 100.0   # service ingest -> decision
    min_compliance: float = 0.99     # fraction of events within e2e budget


class GuardrailSpec(_Model):
    max_consumer_lag: int = 500_000
    max_error_rate: float = 0.05
    max_schedule_lag_ms: float = 2_000.0
    max_missing_fraction: float = 0.01


class SyncSpec(_Model):
    """Synchronous rail (topology gateway-grpc): Java open-loop gRPC emitter settings."""
    mode: Literal["unary", "stream"] = "unary"
    channels: int = 4
    deadline_ms: int = 0           # per-request deadline sent to the gateway (0 = gateway budget)
    target: str | None = None      # default: GATEWAY_GRPC env or localhost:8080


class Scenario(_Model):
    name: str
    description: str = ""
    seed: int = 42
    world: WorldSpec = Field(default_factory=WorldSpec)
    traffic: TrafficSpec = Field(default_factory=TrafficSpec)
    fraud: FraudSpec = Field(default_factory=FraudSpec)
    topology: Topology = "gateway-kafka"
    rules: str = "baseline"
    scoring: dict[str, Any] = Field(default_factory=dict)       # proto3 JSON ScoringConfig
    model_server: dict[str, Any] = Field(default_factory=dict)  # proto3 JSON ModelServerConfig
    budget: BudgetSpec = Field(default_factory=BudgetSpec)
    guardrails: GuardrailSpec = Field(default_factory=GuardrailSpec)
    feature_sample_permille: int = 1000
    sync: SyncSpec = Field(default_factory=SyncSpec)
    # warm: keep Flink keyed state + online-store blobs from earlier runs (realistic state size)
    # fresh: start from empty state so `realy verify` can demand exact feature equality
    flink_state: Literal["warm", "fresh"] = "warm"
    # Flink transactional (exactly-once) decisions sink; the collector then reads read_committed, like
    # any EOS consumer would - decisions become visible only when the checkpoint commits.
    exactly_once: bool = False
    expect: dict[str, Any] = Field(default_factory=dict)  # e.g. {verdict: FAIL}

    @model_validator(mode="after")
    def _wire(self) -> "Scenario":
        self.traffic.profile._duration = self.traffic.duration_s
        return self

    # ------------------------------------------------------------------ helpers
    @property
    def world_dir(self) -> Path:
        if self.world.dir:
            p = Path(self.world.dir)
            return p if p.is_absolute() else REPO_ROOT / p
        return DATA_DIR / f"world-{self.world.customers}-{self.seed}"

    def fingerprint(self) -> str:
        return hashlib.sha256(json.dumps(self.model_dump(mode="json"), sort_keys=True).encode()).hexdigest()[:16]

    def scoring_config(self) -> dict[str, Any]:
        """COMPLETE ScoringConfig JSON (service defaults + this scenario), rule set inlined.

        Services apply config with presence semantics, so a run must send every field explicitly —
        otherwise settings leak from the previous run (e.g. echo mode, injected delays)."""
        cfg = dict(SCORING_DEFAULTS)
        cfg.update({"version": self.fingerprint(), "budget_ms": int(self.budget.hot_path_p99_ms),
                    "feature_sample_permille": self.feature_sample_permille})
        cfg.update(self.scoring)
        rules_path = REPO_ROOT / "rules" / f"{self.rules}.yaml"
        text = rules_path.read_text()
        cfg.setdefault("rule_set_yaml", text)
        cfg.setdefault("rule_set_version", yaml.safe_load(text).get("version", self.rules))
        if self.topology.startswith("echo"):
            cfg["echo"] = True
        return cfg

    def model_server_config(self) -> dict[str, Any]:
        """COMPLETE ModelServerConfig JSON (defaults + this scenario)."""
        cfg = dict(MODEL_SERVER_DEFAULTS)
        cfg.update(self.model_server)
        return cfg


# Explicit values for every ScoringConfig / ModelServerConfig field a scenario may leave out
# (mirrors ScoringConfigHolder.defaults() in Java; {} clears a latency distribution = no delay).
SCORING_DEFAULTS: dict[str, Any] = {
    "budget_ms": 100, "feature_deadline_ms": 30, "model_deadline_ms": 80, "on_deadline": "FALLBACK_RULES",
    "feature_lookup_delay": {}, "model_extra_delay": {}, "feature_source": "redis",
    "microbatch_window_us": 0, "microbatch_max": 0, "hedge_after_ms": 0, "feature_sample_permille": 1000,
    "audit_on_failure": "SPOOL_LOCAL", "self_inclusion": True, "echo": False,
}
MODEL_SERVER_DEFAULTS: dict[str, Any] = {
    "service_time": {"lognormal": {"median_ms": 8, "p99_ms": 25}}, "per_item_us": 0, "workers": 0,
    "max_batch": 1, "max_queue_delay_us": 0, "max_queue": 0,
}


def load_scenario(name_or_path: str | Path, overrides: dict[str, Any] | None = None) -> Scenario:
    p = Path(name_or_path)
    if not p.exists():
        p = SCENARIO_DIR / f"{name_or_path}.yaml"
    data = yaml.safe_load(p.read_text())
    if overrides:
        data = deep_merge(data, overrides)
    return Scenario.model_validate(data)


def deep_merge(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    out = dict(a)
    for k, v in b.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out
