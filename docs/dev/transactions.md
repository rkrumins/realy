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

Two contracts ship, and one run can carry both (`traffic.flows: [a2a_transfer, card_payment]`). Each
payment the generator draws becomes the flow of the first contract that expands it. A plain
`TransactionEvent` (an ATM withdrawal, say) is a one-message transaction and passes straight through.

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

## The second example: a card payment

`card_payment` has six messages linked by `auth_id`
([`card.proto`](../../contracts/proto/realy/flows/v1/card.proto)):

| Role | Message | Part of |
|---|---|---|
| `three_ds` | `CardAuthentication`: the EMV 3-D Secure result (authenticated, attempted, not authenticated, rejected), challenge or frictionless, the access control server's risk score | optional, and it arrives *before* the trigger |
| `authorization` | `CardAuthorization` (ISO 8583 0100-like): card, merchant, terminal, amount, entry mode, CVV / AVS, PIN, cryptogram, scheme score | the trigger |
| `capture` | `CardCapture` (clearing): may carry another amount | lifecycle stage |
| `reversal` | `CardReversal`: an authorisation never captured | terminal stage |
| `refund` | `CardRefund` | lifecycle stage |
| `chargeback` | `Chargeback`: fraud card-absent, fraud card-present, not received, not as described, ... | terminal stage |

It differs from the transfer in the ways real card flows do:
- **Nothing is waited for.** `require: []`: the decision is taken the moment the authorisation arrives,
  with the 3DS result that came before it. The wait for a part, and its timeout, never apply.
- **A part before the trigger.** The 3DS result is held under the link id until its authorisation comes.
- **The lifecycle is long.** Captures, refunds and chargebacks come minutes to days later; they move the
  stage and the context (`captures`, `secs_to_capture`, `capture_amount_changed`, `refunds`,
  `chargeback_reason`), and are kept in Flink state until the transaction's TTL.

Its context at the decision includes `three_ds_status`, `three_ds_challenged`, `acs_risk_score`,
`three_ds_device_changed` and `secs_from_3ds`. `rules/baseline-flows.yaml` uses it:

```yaml
- id: CNP_3DS_HIGH_RISK_UNAUTHENTICATED
  when: "event.txn.acs_risk_score >= 60 && event.txn.three_ds_status != 'TDS_AUTHENTICATED'"
  action: REVIEW
```

`event.txn` holds the names of every contract, so a rule over a card context reads its default on a
transfer or a one-message payment, and never fails (Java and Python agree on this).

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
| `tests/test_flows_generate.py` | both contracts and their generators are valid; a stream of transfers and card payments assembles back into exactly the payments drawn; a 3DS result comes first and only with a 3DS outcome; typologies shape the card lifecycle; part ids are unique across flows; the step language reports its mistakes |
| `AssemblyTest` (Java) | the Java assembler equals the Python twin on the golden stream (`contracts/testdata/golden/flows`, both contracts: 15,053 messages, 6,508 outputs): every unified event, context, stage and parts mask |
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
4. **Data.** Give it a `generate:` section to have the synthetic generator send matching payments as
   these messages (see [Generating a flow](#generating-a-flow); no code), or send your own messages to
   the raw topic with `x-event-type` (replay of recorded flows comes with the replay source).
5. **Use it.** Add rules or expression features over `event.txn.*`, list the contract in the pipeline's
   `transactions`, and add a scenario with `traffic.flows: [<name>, ...]`. `report: {by: [<context
   name>]}` in the contract breaks the run report's decisions down by that value, with ground truth.
6. **Prove it.** `pytest`, `./gradlew test`, then `python -m realy.golden --flows` if you want a golden
   stream for it, and a verified run.

## Generating a flow

The synthetic generator draws each payment as a unified event, with its typology and labels. A contract's
`generate:` turns the payments it `expands` into its messages, declaratively
([`realy/gen/flows.py`](../../python/realy/gen/flows.py)). The card payment's:

```yaml
generate:
  expands: { payment_type: [CARD_PURCHASE] }      # which payments become this flow
  delays_ms: { three_ds: [-25000, -1500], capture: [30000, 7200000], ... }
  steps:
    - role: three_ds
      if: { three_ds: { not: [THREE_DS_NONE] } }  # only for a payment drawn with a 3DS outcome
      after: authorization                        # its time: that step's time + delay_ms
      delay_ms: three_ds
      set: { challenged: { p: challenge }, acs_risk_score: { uniform: acs_risk_score } }
    - role: authorization                         # the trigger: the payment's own event id and time
    - role: capture
      after: authorization
      delay_ms: capture
      p: capture                                  # sent with this probability
      set: { amount_usd_minor: { alter: capture_amount_changed } }
    - role: reversal
      unless: [capture]                           # only when no capture was sent
      ...
    - role: chargeback
      needs: [capture]
      set: { reason: { pick: chargeback_reason } }
  rates: { capture: 0.985, chargeback: 0.0008, ... }
  fraud:                                          # per typology, rates overridden by name
    card_testing: { capture: 0.05, reversal: 0.6, acs_risk_score: [40, 99] }
    friendly_fraud: { chargeback: 0.9, chargeback_reason: { CB_FRAUD_CARD_ABSENT: 0.6, CB_NOT_RECEIVED: 0.4 } }
```

| Step key | Means |
|---|---|
| `role` | the message this step sends |
| `after`, `delay_ms` | its event time: an earlier step's time (or the trigger's) plus a uniform delay, `[lo, hi]` ms or a `delays_ms` name. A step that is skipped still has a time, so later steps can follow it |
| `p` | send it with this probability |
| `needs`, `unless` | only if these earlier steps were sent / were not sent |
| `if` | only for payments whose fields have these values (enum names or numbers; `{not: [...]}`) |
| `set` | the message's own fields (below) |
| `repeat` | send it several times: `{p, max, gap_ms, counter, earlier, last}` (a failed SCA attempt, then a passed one) |
| `stop` | then end the flow with a probability that depends on a field's value (`{field: result, p: abandon}`: warned of a payee mismatch, the customer gives up) |

| Value in `set` | Gives |
|---|---|
| a literal | that value (an enum field takes its name) |
| `{p: R}` | true with probability R |
| `{pick: R}` | one of R's names, by weight |
| `{uniform: R}` | uniform in R = `[lo, hi]` |
| `{col: name}` | the payment's own field |
| `{alter: R}` | with probability R another value than the mapped one: another id, or an amount times U(1.1, 3) (malware rewriting a submission's payee) |

R is a literal or the name of a `rates` entry, and `fraud.<typology>` overrides rates by name. The
typology is the payment's own, or a label-only one: friendly fraud is a genuine purchase whose cardholder
later disputes it.

**Why the parts assemble back exactly.** Nothing in `generate:` repeats the assembly mapping. Every field
the assembly maps (`assemble.fields`, a scored stage's `fields`) is filled by reading that mapping
backwards (an enum map's first source of each name wins). A message field named like a payment field, of
the same type, is filled from the payment too: a capture's card, merchant and amount. The link id, the
partition key, the message's event id and time are always set. `tests/test_flows_generate.py` assembles a
generated stream of both flows and checks every field of every payment.

**Event ids.** The trigger keeps the payment's event id, so labels and the collector join on it
unchanged. Every other message gets a part id: the generator's id space with bit 47 set, one sequence
across all flows of the run. A re-scored stage's decision carries its message's part id.

**Ordering.** Every message keeps its own event time. Parts that fall after a batch's window wait for a
later batch, and parts after the end of the run are not sent, as in production. A part timed before its
trigger (the 3DS result) is sent in the trigger's batch, just before it; one customer's messages stay on
one partition, so the pipeline sees them in the order the verifier replays.
