"""Scenario schema: one YAML file fully describes a reproducible benchmark run.

The `scoring` and `model_server` sections are proto3-JSON of realy.bench.v1.ScoringConfig /
ModelServerConfig, so the exact same document is pushed to the Java services (admin HTTP or the
`bench.control` topic) without translation.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

import os as _os

# REALY_HOME: directory holding contracts/, rules/, scenarios/, models/, docs/ (the repository root in a
# checkout; /opt/realy in the container images). REALY_DATA: synthetic-world cache. REALY_SCENARIOS:
# writable scenario directory (the console's builder saves there).
REPO_ROOT = Path(_os.environ.get("REALY_HOME") or Path(__file__).resolve().parents[2])
SCENARIO_DIR = Path(_os.environ.get("REALY_SCENARIOS") or REPO_ROOT / "scenarios")
DATA_DIR = Path(_os.environ.get("REALY_DATA") or REPO_ROOT / "data")

# the topologies contracts/topologies.yaml defines (a new topology there is accepted here)
from .topologies import names as _topology_names  # noqa: E402
from .topologies import runtime as _topology_runtime  # noqa: E402
Topology = Literal[_topology_names()]  # type: ignore[valid-type]
# Fraud typologies are data (contracts/usecases, python/realy/usecases.py): a kind is any enabled
# typology in the registry, validated at load time rather than fixed in a Literal.
FraudKind = str


def fraud_kinds() -> tuple[str, ...]:
    from .usecases import registry
    return tuple(registry().names())


def _check_kind(kind: str) -> str:
    known = fraud_kinds()
    if kind not in known:
        raise ValueError(f"unknown fraud typology {kind!r}; defined: {', '.join(known)}")
    return kind


# a typology name from the use-case registry (contracts/usecases/typologies/*.yaml)
TypologyName = Annotated[FraudKind, AfterValidator(_check_kind)]


def _default_mix() -> dict[str, float]:
    from .usecases import registry
    return registry().default_mix()


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
    duration_s: float = 60.0         # the whole traffic window, warm-up included (measured = duration_s - warmup_s)
    warmup_s: float = 10.0           # the first seconds of the traffic window: sent, excluded from the statistics
    # Simulated seconds per wall second. "auto" = the population's natural rate maps onto the
    # target TPS, so each customer behaves realistically (rules fire at realistic rates).
    time_scale: float | Literal["auto"] = "auto"
    generators: int = 1              # emitter processes (shards of the customer population)
    sim_start: str = "2026-11-02T00:00:00Z"
    backfill_days: float = 0.0       # unpaced history replay before the measured run
    # transactions formed by several linked messages (contracts/transactions): the generator sends each
    # matching payment as its messages, which the pipeline assembles back into one transaction
    flows: list[str] = Field(default_factory=list)
    # message types other than payments (the pipeline's standalone types: logins, profile changes, card
    # lifecycle, device telemetry): messages of each type per payment generated, so the load mix is explicit
    # (2,000 payments/s with login: 0.6 also sends 1,200 logins/s). Each carries the entities of a payment of
    # the same batch: its customer, card, device, IP (gen/sessions.py)
    event_types: dict[str, float] = Field(default_factory=dict)


class CampaignSpec(_Model):
    kind: TypologyName               # any enabled typology in contracts/usecases
    at_s: float = 0.0                # wall seconds after run start
    count: int = 1                   # number of campaigns started at `at_s`
    params: dict[str, Any] = Field(default_factory=dict)


class ChallengeRates(_Model):
    """How a population fares at a step-up (3DS / SCA challenge online, PIN prompt in store, in-app
    confirmation of a transfer): shares that pass, fail and abandon (the rest of 1 counts as fail)."""
    model_config = ConfigDict(extra="forbid", validate_by_name=True, validate_by_alias=True, serialize_by_alias=True)
    passed: float = Field(0.0, alias="pass", ge=0.0, le=1.0)
    fail: float = Field(1.0, ge=0.0, le=1.0)
    abandon: float = Field(0.0, ge=0.0, le=1.0)

    @model_validator(mode="after")
    def _sum(self) -> "ChallengeRates":
        if self.passed + self.fail + self.abandon > 1.0 + 1e-9:
            raise ValueError("pass + fail + abandon must not exceed 1")
        return self


class ChallengeSpec(_Model):
    """Simulated step-up results. The INSTRUMENT draws each event's latent result (deterministically
    from its id), the system under test never sees it: it only matters when the policy answers
    CHALLENGE. Fraud uses its typology's `challenge` rates (contracts/usecases), unless overridden here."""
    legitimate: ChallengeRates = Field(default_factory=lambda: ChallengeRates(passed=0.90, fail=0.02, abandon=0.08))
    typologies: dict[str, ChallengeRates] = Field(default_factory=dict)


class ScoreDist(_Model):
    mean: float = Field(15.0, ge=0, le=99)
    sd: float = Field(10.0, ge=0, le=60)


class AuthRates(_Model):
    """Authorisation signals of a population (realy/gen/auth.py): the share of its online card
    payments that arrive 3DS-authenticated (the rest of a 3DS-capable checkout is "attempted"), of
    its chip / ATM payments with a verified PIN, of its chip, contactless and wallet payments with a
    valid cryptogram (magstripe and keyed never have one), and its card-network risk score."""
    three_ds_authenticated: float = Field(0.95, ge=0, le=1)
    pin_verified: float = Field(0.85, ge=0, le=1)
    cryptogram_valid: float = Field(0.999, ge=0, le=1)
    scheme_score: ScoreDist = Field(default_factory=ScoreDist)


class AuthSpec(_Model):
    """Fraud uses its typology's `auth` block (contracts/usecases), unless overridden here."""
    legitimate: AuthRates = Field(default_factory=AuthRates)
    typologies: dict[str, AuthRates] = Field(default_factory=dict)


class FraudSpec(_Model):
    # Background campaigns started per 1,000 legitimate events (Poisson), and their mix.
    background_per_1k: float = 0.5
    mix: dict[TypologyName, float] = Field(default_factory=_default_mix)
    campaigns: list[CampaignSpec] = Field(default_factory=list)
    unreported_fraction: float = 0.10       # fraud never labelled
    friendly_fraud_per_1k: float = 0.2      # legit events charged back (label noise)
    chargeback_delay_days: tuple[float, float] = (1.0, 45.0)
    challenge: ChallengeSpec = Field(default_factory=ChallengeSpec)
    auth: AuthSpec = Field(default_factory=AuthSpec)


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


# Scenario settings added after the fingerprints were pinned (contracts/testdata/golden/scenarios.json):
# dotted paths left out of `Scenario.fingerprint()` while they equal their default. Every new setting is
# added here (tests/test_golden_payments.py fails otherwise), so existing runs keep their policy_version.
FINGERPRINT_OPTIONAL: tuple[str, ...] = ("traffic.flows", "traffic.event_types", "pipeline")


def _drop_if_default(d: dict[str, Any], base: dict[str, Any], path: str) -> None:
    *parents, leaf = path.split(".")
    for k in parents:
        d, base = d.get(k), (base or {}).get(k)
        if not isinstance(d, dict):
            return
    if leaf in d and d[leaf] == (base or {}).get(leaf):
        del d[leaf]


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
    # the pipeline this scenario needs (contracts/pipelines/<name>.yaml); empty = any. The stack runs one
    # pipeline (PIPELINE on every service), so a scenario of another pipeline is refused before it starts
    pipeline: str = ""

    @model_validator(mode="after")
    def _wire(self) -> "Scenario":
        self.traffic.profile._duration = self.traffic.duration_s
        if self.traffic.flows and not _topology_runtime(self.topology).supports_flows:
            ok = [t for t in _topology_names() if _topology_runtime(t).supports_flows]
            raise ValueError(f"traffic.flows (multi-message transactions) need a topology that carries them "
                             f"({' or '.join(ok)}), not {self.topology}: the Flink job assembles them, and "
                             f"{self.topology} cannot wait for parts (contracts/topologies.yaml)")
        return self

    # ------------------------------------------------------------------ helpers
    @property
    def world_dir(self) -> Path:
        if self.world.dir:
            p = Path(self.world.dir)
            return p if p.is_absolute() else REPO_ROOT / p
        return DATA_DIR / f"world-{self.world.customers}-{self.seed}"

    def fingerprint(self) -> str:
        """Identity of everything this scenario configures. It is the ScoringConfig version, so it becomes
        every decision's `policy_version`: settings added later are left out while they hold their
        default (FINGERPRINT_OPTIONAL), so adding a setting never changes existing scenarios' decisions."""
        d = self.model_dump(mode="json")
        if FINGERPRINT_OPTIONAL:
            base = type(self)(name=self.name).model_dump(mode="json")
            for path in FINGERPRINT_OPTIONAL:
                _drop_if_default(d, base, path)
        return hashlib.sha256(json.dumps(d, sort_keys=True).encode()).hexdigest()[:16]

    def scoring_config(self) -> dict[str, Any]:
        """COMPLETE ScoringConfig JSON (service defaults + this scenario), rule set inlined.

        Services apply config with presence semantics, so a run must send every field explicitly —
        otherwise settings leak from the previous run (e.g. echo mode, injected delays)."""
        cfg = dict(SCORING_DEFAULTS)
        cfg.update({"version": self.fingerprint(), "budget_ms": int(self.budget.hot_path_p99_ms),
                    "feature_sample_permille": self.feature_sample_permille})
        cfg.update(self.scoring)
        text = resolve_rule_set(self.rules)
        cfg.setdefault("rule_set_yaml", text)
        if _topology_runtime(self.topology).echo:
            cfg["echo"] = True
        return cfg

    def model_server_config(self) -> dict[str, Any]:
        """COMPLETE ModelServerConfig JSON (defaults + this scenario)."""
        cfg = dict(MODEL_SERVER_DEFAULTS)
        cfg.update(self.model_server)
        return cfg


def resolve_rule_set(name: str) -> str:
    """The YAML of rules/<name>.yaml as the services receive it. A rule set may `extends: <base>`: the
    base's rules first (a rule of the same id replaced), then its own; its thresholds and step-up
    settings override the base's; its version is its own. Without `extends` the file is sent as written."""
    return resolve_rule_file(REPO_ROOT / "rules" / f"{name}.yaml")


def resolve_rule_file(path: Path) -> str:
    """resolve_rule_set for a rule-set file anywhere (a candidate for `rules-whatif`): its `extends:` base is
    looked up beside it, else in rules/."""
    path = Path(path)
    name = path.stem
    text = path.read_text()
    doc = yaml.safe_load(text) or {}
    base_name = doc.get("extends")
    if not base_name:
        return text
    beside = path.parent / f"{base_name}.yaml"
    base = yaml.safe_load(resolve_rule_file(beside) if beside.exists() else resolve_rule_set(str(base_name))) or {}
    own_ids = {r.get("id") for r in doc.get("rules") or []}
    merged = dict(base)
    for k, v in doc.items():
        if k in ("rules", "extends"):
            continue
        merged[k] = {**(base.get(k) or {}), **v} if isinstance(v, dict) and isinstance(base.get(k), dict) else v
    merged["rules"] = [r for r in base.get("rules") or [] if r.get("id") not in own_ids] + list(doc.get("rules") or [])
    head = f"# rules/{name}.yaml resolved: extends {base_name} (python -m realy.scenario resolves it)\n"
    return head + yaml.safe_dump(merged, sort_keys=False, allow_unicode=True)


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


def run_rules(run_dir: Path, sc: "Scenario") -> tuple[str, str]:
    """The rule-set YAML a finished run actually used: its snapshot (rules.yaml, written at run start),
    else the scenario's current rule file (runs that predate snapshots). Returns (yaml, source)."""
    snap = Path(run_dir) / "rules.yaml"
    if snap.exists():
        return snap.read_text(), "snapshot"
    return sc.scoring_config()["rule_set_yaml"], "current"
