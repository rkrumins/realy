# Scenarios

A scenario is one YAML document that fully determines a run: the synthetic world, the load shape, the
fraud campaigns, the pipeline topology, every injected delay and capacity limit, and the pass/fail
budget. The same scenario and seed always produce the same events, byte for byte, which makes runs
comparable across topologies, builds and clusters.

* **Bundled packs:** listed in the [scenario catalog](reference/scenarios.md).
* **Field reference:** [scenario-schema.md](reference/scenario-schema.md).
* **`scoring` / `model_server` sections:** see [config.md](reference/config.md).

## Building one in the console

The **Scenarios** page is a visual builder kept in two-way sync with the YAML. Changing a form field
rewrites the YAML; editing the YAML updates the form. It has these tabs:

* **Traffic:** rate profile (constant, ramp, step, spike, sine) with a live preview of TPS over
  time, duration, warm-up, arrivals (uniform or Poisson), backfill days.
* **World:** customers, merchants, payees, mule fraction, seed.
* **Topology:** gateway-kafka, gateway-grpc, flink-inline or echo-kafka, with a diagram of each,
  plus Flink state (warm or fresh) and exactly-once.
* **Scoring:** deadlines, fallback policy, and latency-distribution editors for feature lookup and
  model delay. Each editor shows the distribution's p50/p99/p99.9 and its CDF as you type.
* **Model:** capacity model (workers, dynamic batching, queue limit, per-item cost, service time).
* **Fraud:** background campaign rate, typology mix sliders, and a timeline of scheduled campaigns.
* **Budget:** e2e and hot-path p99 budgets, compliance, and guardrails (lag, error rate, schedule
  lag, missing decisions).

**Validate** checks the document against the schema. **Budget check** runs a Monte Carlo
feasibility estimate of the p99 from the configured distributions, so you know before spending a run
whether a 100 ms budget with a 30-100 ms model is even possible. **Save** writes the scenario to the
console's scenario directory (`/data/scenarios`, which persists across restarts), and **Duplicate**
starts a variant. Saved scenarios appear on **Launch** immediately.

## Anatomy

```yaml
name: card-testing-wave
description: A card-testing wave at t=30s (40 cards hitting one merchant) on top of background fraud.
seed: 12                                   # world + traffic + fraud are all derived from it
world: { customers: 200000, merchants: 20000, payees: 40000 }
traffic:
  profile: { kind: constant, tps: 500 }    # wall-clock target rate
  duration_s: 120                          # measured window (after warm-up)
  warmup_s: 15                             # traffic that is sent but excluded from latency stats
  backfill_days: 1                         # unpaced history first, so velocity features are warm
topology: gateway-kafka
scoring:                                   # proto3 JSON ScoringConfig
  budget_ms: 100
  feature_deadline_ms: 30
  model_deadline_ms: 80
  feature_lookup_delay: { lognormal: { median_ms: 2, p99_ms: 8 } }
model_server:                              # proto3 JSON ModelServerConfig
  service_time: { lognormal: { median_ms: 8, p99_ms: 25 } }
  workers: 64
budget: { e2e_p99_ms: 150, hot_path_p99_ms: 100 }
fraud:
  background_per_1k: 0.3                   # background campaigns per 1,000 legit events
  campaigns:
    - { kind: card_testing, at_s: 30, count: 1, params: { cards: 40 } }
    - { kind: card_testing, at_s: 60, count: 6 }
```

### Load shapes

| `traffic.profile.kind` | Parameters | Use for |
|---|---|---|
| `constant` | `tps` | steady-state latency, soak |
| `ramp` | `start_tps`, `end_tps` (over `duration_s`) | smooth search for the knee |
| `step` | `start_tps`, `step_tps`, `step_duration_s` | latency vs TPS per step (the report plots each step) |
| `spike` | `tps`, `spike_at_s`, `spike_duration_s`, `spike_multiplier` | peak headroom, recovery |
| `sine` | `tps`, `sine_period_s`, `sine_amplitude` | diurnal patterns |

`time_scale: auto` maps the population's natural transaction rate onto the target TPS. At 500 TPS
or at 10k TPS each customer still behaves like a real one: the same velocity patterns, so velocity
rules fire at realistic rates.

### Injected latency: distributions

Any `LatencyDist` field takes one of the following (all values in milliseconds), optionally with
`error_rate` / `timeout_rate`:

```yaml
{ fixed: { ms: 100 } }
{ uniform: { min_ms: 30, max_ms: 100 } }                     # "between 30 and 100 ms"
{ normal: { mean_ms: 50, sd_ms: 10, min_ms: 20, max_ms: 120 } }
{ lognormal: { median_ms: 8, p99_ms: 25, max_ms: 200 } }      # realistic service times
{ bimodal: { median_ms: 8, p99_ms: 20, tail_prob: 0.01, tail_min_ms: 150, tail_max_ms: 400 } }
{ empirical: { quantiles: [0, 0.5, 0.99, 1], ms: [2, 6, 30, 80] } }   # from production histograms
```

* **Feature lookup:** `scoring.feature_lookup_delay` adds delay on top of the real Redis lookup.
* **Model:** use `model_server.service_time` for the model server's own time per batch. That delay
  queues against the server's capacity (workers, batching), as on a GPU. `scoring.model_extra_delay`
  adds delay on the client side.

### Deadlines and degradation

`feature_deadline_ms` and `model_deadline_ms` bound each stage. When one fires, `on_deadline`
decides what happens:
* `FALLBACK_RULES`: a rules-only decision flagged `degraded`;
* `TIMEOUT`: no decision;
* `PROVISIONAL_THEN_ASYNC`: answer now, final decision later.

Reports count degraded decisions separately, so you can see how often the fallback protected the
p99.

### Fraud

* **Background fraud:** `background_per_1k` campaigns per 1,000 legitimate events, drawn from `mix`.
* **Scheduled campaigns:** `campaigns` fire at `at_s` wall seconds, with per-typology `params`.
* **The nine typologies:** card testing, account takeover, lost/stolen, counterfeit, merchant
  breach, ATM cash-out, APP-scam mules, bust-out and bots. They are described in
  [synthetic-data.md](synthetic-data.md).
* **Ground-truth labels** arrive with realistic delays (`chargeback_delay_days`). Some fraud is
  never reported (`unreported_fraction`) and some legitimate payments are charged back
  (`friendly_fraud_per_1k`), so detection metrics have production-like label noise.

### Pass / fail

A run **passes** when every condition holds:
* the e2e and hot-path p99 are within `budget`;
* at least `min_compliance` of events are within the e2e budget;
* missing decisions stay below `guardrails.max_missing_fraction`;
* on the sync rail, every request got a successful response;
* the run was sustainable, with no growing backlog and no p99 drift across the run;
* the run was not aborted.

The report lists every failed condition as a reason. Guardrails also protect the run while it is
running: if the emitter's schedule-lag p99 exceeds `max_schedule_lag_ms`, or the backlog (emitted
minus decided) exceeds `max_consumer_lag`, the driver aborts the run with a `GUARDRAIL` log line and
the report shows the reason. That way an overloaded pipeline fails fast instead of queueing for
minutes. `max_error_rate` is accepted but not enforced yet.

`expect: { verdict: FAIL }` marks a scenario designed to fail, such as `user-example-100ms`, so CI
treats the FAIL as the correct outcome.

## Overrides without editing

```bash
realy run smoke --set traffic.profile.tps=2000 --set traffic.duration_s=300
realy run step-ramp --set topology=flink-inline --set flink_state=fresh
```

The console's **Launch** page has the same overrides as fields (TPS, duration, topology, backfill,
state, verify). It can also schedule a run once, every N minutes, or N times.
