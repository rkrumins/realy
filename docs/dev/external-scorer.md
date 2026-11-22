# Benchmark your own scorer (the `external` topology)

Once you have built a decision service, from this repository or not, PulseBench can benchmark it as a
black box. The same scenarios, world, open-loop load, clock and completeness checks apply as for the
bundled topologies, so your service's numbers sit next to theirs on the same host. PulseBench drives
the load, collects every decision and audits it. Your service decides, and the run configures nothing on
its path: no scoring config, model settings, online-store reset or Flink job. On a stack that also runs
the bundled scorers, the run switches off whatever would consume the same events: gateway ingress over
`bench.control`, and an inline Flink job, stopped with a savepoint. The decisions collected are then
your service's alone, and any duplicate is counted.

## The contract

Two topics and two messages. The topic names come from the pipeline
(`contracts/pipelines/payments.yaml` `topics`).

| Direction | Topic | Message | What your service must do |
|---|---|---|---|
| in | `topics.events` (`payments.events.raw`) | `realy.payments.v1.TransactionEvent`, keyed by the partition entity (the customer) | consume it |
| in | (same records) | header `x-bench-trace`: a serialized `realy.bench.v1.BenchTrace` | keep it with the event |
| in | (same records) | header `x-backfill: 1`: unmeasured history | decide it or skip it; it is not measured |
| in | (same records) | header `x-event-type`: a part of a multi-message transaction ([transactions](transactions.md)) | assemble it, or skip it if you do not run flows |
| out | `topics.decisions` (`payments.decisions`) | one `realy.scoring.v1.DecisionRecord` per event | set `event_id`, `outcome`, `decided_at_us`, and copy the received BenchTrace into `trace` |

Optional, and used when present:
- the event itself in `DecisionRecord.event`: the ClickHouse audit and the Explorer show its fields;
- `reason_codes`, `rule_hits`, `score` and `policy_version`;
- `features`, a vector in the schema's order: the Explorer shows it;
- spans of your own stages in `trace.spans`. Each span needs a clock `domain` of its own: the collector
  never compares timestamps across domains, so a machine with a skewed clock cannot distort the numbers.

End-to-end latency is measured on the driver's clock, from each event's intended send time (open loop:
coordinated omission is charged to the system) to when the collector received its decision. Your
service's clock is never used for it.

## Try it on the bundled stack

The reference scorer ([`python/realy/external_scorer.py`](../../python/realy/external_scorer.py)) is the
smallest service that satisfies the contract. It applies one fixed rule (amounts above a threshold go to
review), keeps no state and copies the trace:

```bash
C="docker compose -f deploy/compose/docker-compose.yml exec -T console"
$C python -m realy.external_scorer &            # your service, here the reference one
$C pulsebench infra check external-smoke        # Kafka, ClickHouse and the contract (your scorer is not probed)
$C pulsebench run external-smoke --verify
```

## What is measured and what is not

| Check | `external` | Bundled topologies |
|---|---|---|
| every event has exactly one decision (missing and duplicates counted) | yes | yes |
| every outcome is valid (APPROVE, REVIEW, DECLINE, CHALLENGE) | yes | yes |
| end-to-end latency and the budget, on one clock | yes | yes |
| the ClickHouse audit is complete | yes | yes |
| per-stage latency (feature lookup, model, rules) | only the spans your service adds | yes |
| features against the Python reference | not checkable: PulseBench does not know how your service computes them | yes |
| rules, score and outcome parity | not checkable | yes |
| detection quality (recall, false positives, $ caught, by typology) | yes: the labels are PulseBench's own | yes |

`pulsebench verify` reports the not-checkable parts by name. It never marks them passed.

## Comparing with a bundled topology

Run the same scenario both ways on the same host. For example, copy `external-smoke` with
`topology: gateway-kafka` and the scoring settings of `smoke`. Then compare the two reports, or open one
in Live with the other as its overlay. Run the `echo-kafka` topology first: its end-to-end time is the
instrument's and the brokers' own floor, which every other number includes.

## Limits

- **Asynchronous only.** For a synchronous API, benchmark it with your own client against the same
  scenario. The gateway's gRPC rail (`gateway-grpc`) shows how PulseBench measures one.
- **Not configured by the run.** Scenario settings for scoring and the model server do not reach your
  service. Configure it yourself, and record what you ran in the scenario's description.
