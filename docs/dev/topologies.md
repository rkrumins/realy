# Building a topology

A topology is how events travel from the load generator to a decision: which components handle them, in
which order, and who decides. The four that ship are described, step by step and setting by setting, in
the [topologies reference](../reference/topologies.md). This guide is about changing them: tuning a step,
building a new topology from the existing components, and adding a component of your own.

## What a topology is made of

One file describes them all: [`contracts/topologies.yaml`](../../contracts/topologies.yaml).

| Section | Says | Read by |
|---|---|---|
| `components` | every building block (emitter, topics, Flink assembler / features / scoring, gateway, Redis, model server, collector, ClickHouse): what it does, its code, every setting with where it is set and its default, how to swap it | the reference page, the console's Topologies page |
| `topologies.<name>.runtime` | the choices the runner makes: how events reach the scorer (`ingress`), who decides (`scorer`), the Flink job's mode (`flink_mode`), how multi-message transactions reach the scorer (`flows`), the verifier's exact-feature target | the runner, the scenario model, the verifier (`realy.topologies.runtime`) |
| `topologies.<name>.steps` | the steps in order, each one a component: its position on the live map, the metrics that feed it, the hops out of it; `steps_with_flows` when multi-message transactions change the path | the console's live map (`realy.topologies.graph`), the reference page |
| `capabilities`, `choose_when`, `avoid_when`, `enhancements` | what it can and cannot do, and why; when to use it; the known limits and what would lift them | the reference page, the Topologies page, the Launch explainer |
| `advice` | what to try when a stage dominates the slow tail, per topology | the run report, the console's advice |

`pytest tests/test_topologies.py` validates the file: every
component and step complete, every hop to a real step, every metric reference well formed, every runtime
choice one the runner implements.

## 1. Tune a step

Every step's settings are listed under its component in the reference, with where they are set:

| Set in | How |
|---|---|
| scenario | the scenario YAML (`scoring.*`, `model_server.*`, `traffic.*`, `sync.*`, `flink_state`, ...); the console's Scenarios builder edits them |
| contract | the contract file named (`contracts/features/feature_schema.yaml`, `contracts/transactions/*.yaml`, `contracts/pipelines/*.yaml`, `contracts/kafka/topics.yaml`) |
| env | an environment variable of the service named; all of them: [reference/environment.md](../reference/environment.md) |
| Flink job args | passed by the runner (`python/realy/runner/flink.py`) |

Measure the effect: run the scenario before and after, then **Compare** the two runs (per-stage p50 / p99 /
p99.9). The run report's advice names the stage to look at first.

## 2. A new topology from existing components

When the components exist and only their combination is new, a topology is YAML. For example, a variant
of `gateway-kafka` with a tighter verifier target and its own description:

```yaml
topologies:
  gateway-kafka-strict:
    title: Gateway on Kafka (strict)
    tagline: The async gateway, verified against a 99.5% exact-feature target.
    decides: the scoring gateway
    flink: "`features` job"
    runtime: { ingress: kafka, scorer: gateway, flink_mode: features, flows: assembled_topic, exact_target: 0.995 }
    steps: [...]             # copy gateway-kafka's steps (and steps_with_flows)
    capabilities: { ... }    # every capability, with a note saying why
    choose_when: [...]
```

Then:
1. `pytest tests/test_topologies.py`: the contract is complete.
2. A scenario with `topology: gateway-kafka-strict` validates (the scenario model accepts every topology
   the contract defines), runs, and shows its own map in Live.
3. `python -m realy.docsgen` adds it to the reference.

What YAML alone can combine is bounded by the runtime values the runner implements:

| Runtime | Values | Each means |
|---|---|---|
| `ingress` | `kafka`, `grpc` | the gateway consumes the events topic / the Java emitter calls the gateway's Score API |
| `scorer` | `gateway`, `flink`, `echo` | the stateless gateway / the Flink job's async scoring operator / the gateway answering without scoring |
| `flink_mode` | `features`, `both`, `none` | features to Redis / features and scoring in the job / no Flink |
| `flows` | `assembled_topic`, `in_job`, `none` | Flink assembles and publishes to `topics.assembled` / Flink assembles and scores in the job / cannot carry multi-message transactions |

A value outside these is a new component (section 3): the validator says so.

## 3. A new component

The extension points, and what a new component touches:

| To add | Implement | Then |
|---|---|---|
| another feature store (Aerospike, DynamoDB, Feast online) | `io.realy.scoring.store.OnlineStore` (`get`, the pipelined batch) | select it in `GatewayMain` / `ScoringAsyncFunction`; add a `components` entry; the verifier is unchanged (it recomputes features from events) |
| another model protocol | `io.realy.scoring.model.ModelClient` | select it in `GatewayMain` / `ScoringAsyncFunction`; any KServe v2 server (Triton, KServe) needs no code |
| another transaction, event type, feature, filter, window, entity | contracts only | [transactions](transactions.md), [schemas](schemas.md), [features](features.md) |
| a new aggregation kind | `EntityAggregator` and its Python twin (`realy.features.engine`) today; an `Aggregation` extension point is planned | a conformance fixture in both languages |
| another transport into the scorer (a queue other than Kafka, HTTP push) | a source in the gateway (beside `KafkaIngress`) and in the Flink job (beside the `KafkaSource`) | a new `ingress` value: `RUNTIME_VALUES` in `python/realy/topologies.py`, the runner's set-up for it (`python/realy/runner/run.py`, `apply_topology`), a `components` entry |
| another scorer (a different decision service) | it must read the events topic or serve the Score API, and publish `DecisionRecord`s with the trace it received to the decisions topic | a new `scorer` value; the collector, verifier and audit then work unchanged, because they read the decisions topic |
| a step on the live map | a `components` entry and a step with the metrics its service publishes on `bench.metrics` (stage spans or counters) | the map draws it; `rate` / `p99` name the source and the stage |

The rule that keeps a new component honest: every decision carries the event's trace (`x-bench-trace`)
and goes to the decisions topic. Then the collector measures it end to end, ClickHouse audits it, and
`pulsebench verify` checks its features, rules, score and outcome against the Python twin, whatever built it.

## 4. Prove it

| Check | Command |
|---|---|
| the contract is complete | `pytest tests/test_topologies.py` |
| the map and the reference | open Live during a run; `python -m realy.docsgen` |
| it decides correctly | `pulsebench run <scenario> --verify`: rules, score and outcome parity 100%, the exact-feature target, the audit complete |
| how fast, and where the time goes | the run report (per-stage latency, tail attribution, resources) and its advice; **Compare** against the topology it replaces |
| it holds under faults | `make live-test` (broker, Flink and gateway faults during a run) |
