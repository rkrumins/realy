# Transactions formed by several messages

A payment hub rarely sees a payment as one message. A transfer starts with the customer's initiation;
the app sends the device and session context beside it; the payee's bank answers a payee check; the
customer authenticates; the payment is submitted to the scheme, which later settles, rejects or
returns it. A card payment is an authorisation, later a capture, sometimes a refund or a chargeback.
These are different messages, linked by an id, arriving at different times.

PulseBench takes them as they come and assembles each transaction into the unified payment event
(`TransactionEvent`), plus a context. Every feature, rule, model, verifier check, report and Explorer
view works on an assembled transaction unchanged. Rules, filters and expression features also see
what only the flow knows, as `event.txn.*`.

| Contract | Says |
|---|---|
| [`contracts/transactions/<name>.yaml`](../../contracts/transactions/a2a_transfer.yaml) | the messages (roles) of one kind of transaction, the field that links them, how they assemble into the unified event, its lifecycle stages, its context values, and how the generator produces it |
| [`contracts/events/<type>.yaml`](../../contracts/events/payment_initiation.yaml) | each message's binding, as for any event type ([schemas](schemas.md)) |
| [`contracts/proto/realy/flows/v1/`](../../contracts/proto/realy/flows/v1/a2a.proto) | the messages themselves (protobuf); `txn.proto` has `TxnContext` (what travels with an assembled transaction and is recorded on its decision) |

## The worked example: an account-to-account transfer

`a2a_transfer` has six messages linked by `payment_id`:

| Role | Message | Part of |
|---|---|---|
| `initiation` | `PaymentInitiation` (pain.001-like): payer, payee, amount, channel, new payee | the trigger |
| `device` | `DeviceContext`: device, IP, session age, remote access, on a call, emulator | optional |
| `payee_check` | `PayeeCheck` (confirmation of payee): match, close match, no match, not checked | required |
| `authentication` | `ScaResult`: method, passed, attempt | lifecycle stage |
| `submission` | `PaymentSubmission` (pacs.008-like): may name another payee | stage, decided again |
| `status` | `PaymentStatus` (pacs.002 / pacs.004-like): settled, rejected, returned, recalled | terminal stage |

The context (`event.txn.*`) includes `payee_check`, `new_payee`, `remote_access`, `on_call`,
`session_age_s`, `sca_attempts`, `sca_passed`, `payee_changed`, `amount_changed`, `secs_to_submit`,
plus `transaction`, `stage`, `partial` and `parts` (the bits of the roles present). A one-message
payment reads every name's default. `rules/baseline-flows.yaml` uses it:

```yaml
- id: APP_SCAM_PAYEE_MISMATCH
  when: "event.txn.payee_check == 'PAYEE_NO_MATCH' && event.txn.new_payee"
  action: REVIEW
```

## Assembly: when, where, and why it is exact

- **When.** A transaction is assembled when its trigger and every required part have arrived. Optional
  parts that arrived before them are included. A required part still missing `wait_ms` after the
  trigger means the transaction is assembled without it (`on_timeout: score_partial`), and its decision
  says so (`txn.partial`, `txn.parts_present`).
- **Lifecycle.** Messages after assembly move the stage and update the context. A stage marked
  `scored` (the submission) is decided again. It is scored as the assembled event with that stage's field
  overrides (its own event id and time, the payee it was really submitted to), flagged read-only. Its
  features read the state as of its time without counting it: the transaction was counted once, when
  it was assembled. That holds in Flink (`EntityAggregator.read`), at the gateway (read-only stage
  decisions re-window the stored state without self-inclusion) and in the Python twin
  (`ReferenceEngine.read`). A bank that scores at initiation and again before release is exactly this.
- **Where.** In the Flink job, keyed by the pipeline's partition entity (the customer), before the
  feature stages. Every message of one customer is on one Kafka partition (the emitter and the payment
  hub key them by customer), so they arrive in one order. Assembly is therefore a pure function of that
  order, and the Python twin (`realy.bindings.transactions.Assembler`) reproduces it exactly. The one
  exception is a wall-clock wait that expired before a required part arrived; the decision records it
  and the verifier replays it as recorded. Open transactions live in keyed state (`TxnState`), so they
  survive restarts with the checkpoint.
- **Topologies.**
  - `flink-inline`: Flink assembles, computes the features and scores, with no extra hop.
  - `gateway-kafka`: Flink publishes every scoreable event to `payments.events.assembled` (assembled or
    passed through, with the trigger's trace, the assembly wait as a span, and `x-txn`). The gateway
    scores from there. That is one more Kafka hop, measured, and the report shows the assembly wait as
    its own step.
- **Timing.** End-to-end latency runs from the customer's initiation (the trigger's intended send time)
  to the decision, so it includes the wait for the payee check: the time the customer actually waits.

## What proves it

| Test | Proves |
|---|---|
| `tests/test_transactions.py` | the contract is valid; assembly, optional and required parts, stages, holding a stage that came first, the deadline, a replayed partial; the generated parts assemble back into exactly the transfers the generator drew; the verifier replays a run in the pipeline's order |
| `AssemblyTest` (Java) | the Java assembler equals the Python twin on the golden stream (`contracts/testdata/golden/flows`: 8,918 messages, 6,581 outputs): every unified event, context, stage and parts mask |
| `FlowJobTest` (Flink MiniCluster, RocksDB, parallelism 2) | the real job graph assembles the golden stream as the twin does, and every output's customer and card features (re-scored stages read-only) equal the twin's |
| `pulsebench run flows-smoke --verify` / `flows-inline` | the whole stack: rules, score and outcome parity, exact customer and card features, a complete audit |

Measured on this stack (4 vCPU, 500 TPS, 2026-11-22):

| Run | Transfers assembled | Partial | Assembly wait p50 / p99 | e2e p99 | Verify |
|---|---|---|---|---|---|
| `flows-inline` (Flink scores) | 2,982 from 14,751 more messages | 0.67% | 1.5 / 81 ms | 135 ms (PASS) | exact features 100%, rules / score / outcome 100%, audit complete |
| `flows-smoke` (gateway scores from the assembled topic) | 1,986 from 9,831 more messages | 0.55% | 1.7 / 68 ms | 341 ms (FAIL: Kafka on this host, one more hop) | exact-set 99.83% (gateway store lag), rules / score / outcome 100%, audit complete |

The flow's own signals carry detection: of the transfers whose payee name did not match the account,
65 of the 67 frauds were stopped (`flows-inline`).

## Walkthrough: your own flow

1. **Messages.** Write the protobuf messages under `contracts/proto`, each carrying the link id, the
   partition key and its own event id and time. Then run `scripts/gen-proto-python.sh` and
   `python -m realy.proto_compat --update`.
2. **Bindings.** Add one `contracts/events/<type>.yaml` per message, mapping its entities (including
   `txn: <link field>`). List the types in the pipeline (`contracts/pipelines/payments.yaml` `event_types`,
   folded) and the transaction under `transactions`.
3. **Contract.** Write `contracts/transactions/<name>.yaml`:
   - `messages` (role → type);
   - `assemble`: `into`, `trigger`, `require`, `optional`, `wait_ms`, and `fields` (unified field ←
     `role.field`, `{const}`, or `{from, map}` for enum names);
   - `stages`;
   - `context`: `from` / `count` / `changed` / `elapsed`.

   Every field the unified event needs has to be mapped. The test that reassembles the generator's
   events catches a missing one.
4. **Data.** Give it a `generate:` section to have the synthetic generator expand matching payments into
   these messages (distributions and per-typology overrides are data), or send your own messages to the
   raw topic with `x-event-type` (replay of recorded flows comes with the replay source).
5. **Use it.** Add rules or expression features over `event.txn.*`, and a scenario with
   `traffic.flows: [<name>]`.
6. **Prove it.** `pytest`, `./gradlew test`, then `python -m realy.golden --flows` if you want a golden
   stream for it, and a verified run.
