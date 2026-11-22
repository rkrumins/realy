"""Sharded, deterministic event generator (legit behaviour + fraud campaigns).

Determinism: the same world, scenario, shard and the same sequence of `generate(n, t0, t1)` /
`schedule_campaign` calls produce byte-identical payloads.

Volume: `generate(n, ...)` returns exactly n events unless more campaign events are due in the
interval than n (e.g. an ATM cash-out spike) -- then all due campaign events are returned and
len(batch) > n. Callers emitting at a fixed rate should treat the excess as a burst (that is the
point of a cash-out scenario) or subtract it from the next calls.
"""
from __future__ import annotations

import math
from datetime import datetime

import numpy as np

from ..scenario import Scenario
from ..world import World
from .batch import EventBatch, concat_chunks, finalize, make_chunk, take
from .behaviour import US, LegitEngine, MerchantIndex
from .campaigns import CampaignEngine

SPLIT_KEYS = ("cards", "victims")


def natural_rate_total(world: World) -> float:
    """Expected events per simulated second for the whole population (diurnal mean)."""
    return float(world.customers["activity_per_day"].astype(np.float64).sum() / 86400.0)


def auto_time_scale(world: World, target_tps: float) -> float:
    """Simulated seconds per wall second so that the natural population rate equals target_tps."""
    return max(1e-6, target_tps / natural_rate_total(world))


def sim_start_us(scenario: Scenario) -> int:
    s = scenario.traffic.sim_start.replace("Z", "+00:00")
    return int(datetime.fromisoformat(s).timestamp() * US)


class Generator:
    def __init__(self, world: World, scenario: Scenario, shard: int = 0, n_shards: int = 1, registry=None) -> None:
        if not 0 <= shard < n_shards or n_shards > 0xFFFF:
            raise ValueError("bad shard")
        self.world, self.scenario, self.shard, self.n_shards = world, scenario, shard, n_shards
        self.gen_id = shard
        ss = np.random.SeedSequence([scenario.seed, shard, n_shards, 0xC0FFEE])
        r_legit, r_camp, r_bg = (np.random.Generator(np.random.PCG64(s)) for s in ss.spawn(3))
        self.bg_rng = r_bg
        ids = world.customers["customer_id"]
        self.rows = np.flatnonzero(ids % np.uint64(n_shards) == np.uint64(shard)).astype(np.int64)
        if self.rows.shape[0] == 0:
            raise ValueError("shard has no customers")
        self.merchants = MerchantIndex(world)
        f = scenario.fraud
        self.legit = LegitEngine(world, self.rows, shard, r_legit, self.merchants, f.friendly_fraud_per_1k,
                                 tuple(f.chargeback_delay_days))
        from ..usecases import registry as _registry
        self.reg = registry or _registry()
        from .auth import AuthSignals
        self.auth = AuthSignals(scenario, self.reg)
        self.campaigns = CampaignEngine(world, self.rows, shard, n_shards, r_camp, self.merchants,
                                        f.unreported_fraction, tuple(f.chargeback_delay_days), registry=self.reg)
        # registry order (by id) keeps the background draw identical to the former hard-coded tuple
        kinds = [k for k in self.reg.names() if f.mix.get(k, 0) > 0]
        w = np.array([f.mix[k] for k in kinds], dtype=np.float64)
        self.bg_kinds = kinds
        self.bg_p = w / w.sum() if w.sum() > 0 else w
        self.flows = None
        if scenario.traffic.flows:
            from ..bindings.transactions import transaction
            from .flows import FlowExpander
            if len(scenario.traffic.flows) != 1:
                raise ValueError("one flow per run for now (traffic.flows)")
            # a stream of its own (spawned children are index-keyed: the three above are unchanged)
            r_flow = np.random.Generator(np.random.PCG64(ss.spawn(1)[0]))
            tx = transaction(scenario.traffic.flows[0])
            # a scored lifecycle stage (the submission) is a decision of its own: traced, expected
            self.flows = FlowExpander(tx, r_flow, self.reg.fraud_type_names(), shard,
                                      score_stages=any(st.scored for st in tx.stages))
        self.pending = make_chunk(0)
        self.seq = 0
        self.order = 1 << 40
        self.last_t1 = None
        self.campaigns_started: dict[str, int] = {}

    # ------------------------------------------------------------------ rates
    def natural_rate_per_sim_s(self) -> float:
        return float(self.legit.natural_rate)

    # ------------------------------------------------------------------ campaigns
    def schedule_campaign(self, kind: str, at_sim_us: int, params: dict | None = None, split: bool = False) -> int:
        """Script a campaign starting at simulation time `at_sim_us`. Returns its event count.

        split=True divides `cards`/`victims` by the number of shards (scenario timeline totals).
        """
        t = self.reg.by_name.get(kind)
        if t is None or not t.campaigns():
            raise ValueError(f"unknown or disabled campaign kind {kind}")
        params = dict(params or {})
        if split and self.n_shards > 1:
            for key in SPLIT_KEYS:
                if key in params:
                    params[key] = max(1, math.ceil(int(params[key]) / self.n_shards))
        chunk = self.campaigns.build(kind, int(at_sim_us), params)
        n = chunk["event_time_us"].shape[0]
        if n:
            chunk["order"] = self.order + np.arange(n, dtype=np.int64)
            self.order += n
            merged = concat_chunks([self.pending, chunk])
            self.pending = take(merged, np.argsort(merged["event_time_us"], kind="stable"))
        self.campaigns_started[kind] = self.campaigns_started.get(kind, 0) + 1
        return n

    def schedule_timeline(self, sim_start: int, time_scale: float) -> int:
        """Schedule every scenario.fraud.campaigns entry (at_s wall seconds -> simulation time)."""
        total = 0
        for spec in self.scenario.fraud.campaigns:
            count = spec.count // self.n_shards + (1 if self.shard < spec.count % self.n_shards else 0)
            for _ in range(count):
                at = sim_start + int(spec.at_s * time_scale * US)
                total += self.schedule_campaign(spec.kind, at, spec.params, split=True)
        return total

    # ------------------------------------------------------------------ batches
    def generate(self, n: int, sim_t0_us: int, sim_t1_us: int) -> EventBatch:
        t0, t1 = int(sim_t0_us), int(max(sim_t1_us, sim_t0_us + 1))
        if self.last_t1 is not None and t0 < self.last_t1:
            t0 = self.last_t1  # keep per-customer time monotonic across calls
            t1 = max(t1, t0 + 1)
        self.last_t1 = t1
        # background campaigns (Poisson in the number of legit events)
        if self.bg_kinds and self.scenario.fraud.background_per_1k > 0 and n > 0:
            k = int(self.bg_rng.poisson(self.scenario.fraud.background_per_1k * n / 1000.0))
            for _ in range(k):
                kind = self.bg_kinds[int(self.bg_rng.choice(len(self.bg_kinds), p=self.bg_p))]
                self.schedule_campaign(kind, int(self.bg_rng.integers(t0, t1)))
        due = self.pending["event_time_us"] < t1
        n_due = int(due.sum())
        if n_due:
            camp = take(self.pending, np.flatnonzero(due))
            camp["event_time_us"] = np.maximum(camp["event_time_us"], t0)
            self.pending = take(self.pending, np.flatnonzero(~due))
        else:
            camp = make_chunk(0)
        legit = self.legit.generate(max(0, n - n_due), t0, t1, order0=self.seq)
        batch = finalize(concat_chunks([legit, camp]), self.gen_id, self.seq, self.auth)
        self.seq += len(batch)
        if self.flows is not None:
            batch.records = self.flows.expand(batch.columns, batch.payloads, batch.keys, t1)
        return batch

    @property
    def pending_campaign_events(self) -> int:
        return int(self.pending["event_time_us"].shape[0])
