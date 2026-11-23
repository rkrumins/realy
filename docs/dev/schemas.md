# Schemas: entities, event types and pipelines

The pipeline does not hard-code its messages. Three contracts say what they mean, and Java
(`io.realy.scoring.binding`) and Python (`realy.bindings`) read the same files:

| Contract | Says | Generated reference |
|---|---|---|
| [`contracts/entities.yaml`](../../contracts/entities.yaml) | every kind of thing events refer to and state is kept for: id type, which entity owns it, its `Entity` enum value. The order is the Flink job's keyed-stage order | [reference/bindings.md](../reference/bindings.md#entities) |
| [`contracts/events/<type>.yaml`](../../contracts/events/payment.yaml) | what a protobuf message means: its id, event time, which fields hold which entities, location, amount | [reference/bindings.md](../reference/bindings.md) (every field) |
| [`contracts/pipelines/<name>.yaml`](../../contracts/pipelines/payments.yaml) | which event types flow (scored or only folded into state), the partition entity, the namespace, topics, features and model | [reference/bindings.md](../reference/bindings.md#pipelines) |

A transaction made of several linked messages (an initiation, a payee check, an authentication...) is
a fourth contract, [`contracts/transactions/<name>.yaml`](../../contracts/transactions/a2a_transfer.yaml):
see [transactions](transactions.md).

`PIPELINE` selects the pipeline (default `payments`). Each run writes the resolved binding to its directory
(`pipeline.json`), so it is always read back with the bindings it ran with.

## How a message is decoded

A bound message is compiled once per type into a codec: in Java `WireCodec` (one pass over the protobuf
bytes driven by a field-number plan; no generated class needed, so any message in `contracts/proto`
works), in Python `TypeCodec`. Every flat field (scalars and enums) becomes part of the CEL `event` view
that rules, filters and expression features see. The same rules hold in both languages, and the golden
tests (`BindingTest`, `tests/test_bindings.py`) prove the codec reproduces the hand-written payment view
on 6,000 events:

- integers read as integers: a `uint32` unsigned, a `uint64` as the signed 64-bit value (Java's `long`);
- enums read by name; a number the contract does not name (a newer producer) reads as its decimal string;
- a field an event lacks (one logged before the field existed) reads its proto3 default;
- nested and repeated fields are not part of the flat view.

The keyed Flink stages do not see the view: the source decodes each event once into an `EventRow` (the
key of every registry entity, and one slot per field a stream feature reads) laid out by the schema's
`RowPlan`.

## Exactness follows ownership

A pipeline's records are keyed by its `partition_key` entity, so one customer's events are totally
ordered. An entity `owned_by` the partition entity (directly or through a chain: a card belongs to one
customer) sees its events in that order too, so its features are exact; `verify` requires 100%. Other
entities (a device, a merchant, a payee) are shared across partitions: their order is arrival-dependent,
as in production, and `verify` reports an agreement rate. The list is derived, not configured:
`pipeline().exact_entities()`.

## Message types that carry a subset of the entities

A payment names all seven entities. Most other messages name only some: a login has a customer, a device
and an IP; a card's lifecycle event a customer, the card and (for a wallet token) a device; a device
report only the device and its IP. Each such type is a binding of its own (`contracts/events/<type>.yaml`
maps just the entities it has), listed in the pipeline's `event_types`. A type that is neither the
default type nor a part of a [multi-message transaction](transactions.md) is *standalone*: it folds into
the features that list it and is never decided (unless it is `scored`).

The worked example is the pipeline [`payments-ext`](../../contracts/pipelines/payments-ext.yaml) with its
schema [`feature_schema_ext.yaml`](../../contracts/features/feature_schema_ext.yaml), which `extends:` the
base schema: the 62 payment features keep their indices and their state bytes, and 10 features fed by the
other types are appended.

| Type | Entities | Topic (partition key) | Feeds, for example |
|---|---|---|---|
| `payment` | all seven | events (customer) | every base feature; scored |
| `login` | customer, device, IP | events (customer) | `cust_failed_logins_1h`, `cust_distinct_login_ips_24h` |
| `profile_change` | customer, device, IP | events (customer) | `cust_secs_since_credential_change` |
| `card_lifecycle` | customer, card, device | events (customer) | `card_tokens_provisioned_30d`, `card_secs_since_pin_change` |
| `device_telemetry` | device, IP | its own topic (device) | `dev_max_risk_24h`, `ip_max_reputation_risk_24h` |

**How each event folds**, at each keyed stage (Java `EntityAggregator.process(.., type)`, Python
`ReferenceEngine.process(.., event_type=)`):

- a feature folds the types its `event_types` lists (absent: the pipeline's default type), so adding a
  type changes no existing feature;
- a scored event (a payment) reads a feature it does not fold as of its own time: a payment sees the
  failed logins of the last hour without being counted as one;
- a fold-only event (a login) leaves every other feature alone, and a stage where it folds nothing does
  not touch state at all. After the last stage that folds its type, the Flink job drops it, so a login
  never reaches the merchant, terminal or payee shuffles;
- one feature can be fed by several types: `dev_distinct_customers_all_24h` counts the customers seen
  on a device by a login or a payment.

**Filters per type.** A filter written `{when: <CEL>, event_types: [login]}` is evaluated only for those
types, over that type's message (`event.outcome` is a login's field); a plain filter applies to the
default type. The pipeline check (`pulsebench bindings check`) fails when a filter names a field its types
lack, or a feature names a type that lacks its entity or value field.

**Routing.** Types on the events topic share its partition key, so one customer's logins, profile
changes, card events and payments keep one order. A type whose `topic:` is another one (device telemetry,
`partition_key: device`) gets a source of its own in the Flink job, joined to the stage chain just before
the first stage that folds it (the device stage), so it never touches the customer or card stages. The
generator sends each type with its `x-event-type` header to its topic; the gateway and ClickHouse's
`events_raw` skip typed records; the collector expects decisions only for scored types.

**Exactness.**

| Features of | Fed by | Verify |
|---|---|---|
| customer, card (and other entities owned by the customer) | payments, logins, profile changes, card lifecycle | exact: one topic, one order per customer (inline 100%) |
| device, IP and other shared entities | payments, logins, device telemetry | agreement rate: arrival order differs across partitions and topics, as for payments alone |

`verify` folds each standalone message from the run's log (`parts-*.parquet`) into the reference engine in
emission order, decoded by its binding and masked with its own type's filters, and reports the counts per
type (`event_types.standalone`).

**Running it.** The stack runs one pipeline: every service reads `PIPELINE` (gateway, Flink, model server,
console). `make up PIPELINE=payments-ext` starts it; a scenario names the pipeline it needs (`pipeline:`),
and the runner and the Flink job refuse a stack on another one. The load mix is per decided payment:

```yaml
pipeline: payments-ext
traffic:
  profile: { kind: constant, tps: 500 }          # decided payments per second
  event_types: { login: 0.6, profile_change: 0.03, card_lifecycle: 0.05, device_telemetry: 0.3 }
```

`multi-type-smoke` (gateway) and `multi-type-inline` (Flink scoring) run exactly this; the report's
*message mix* line gives each type's count per decided payment, the load to size for.

## Walkthrough: add your own message type

Say your platform publishes `SessionEvent` with a customer, a device and an IP.

1. **Proto.** Add the message under `contracts/proto` (prefix enum values: proto3 enum names share the
   package scope), then `scripts/gen-proto-python.sh` and `python -m realy.proto_compat --update`.
2. **Binding.** `contracts/events/session.yaml`: `message`, `id`, `time`, and `entities` mapping only the
   entities it carries (`customer: customer_id`, `device: device_id`, `ip: ip_v4`).
3. **Pipeline.** Add `session: {}` to the pipeline's `event_types` (`topic: <key>` and `partition_key:`
   if it travels on a topic of its own, which must then be in `topics:` and `contracts/kafka/topics.yaml`).
4. **Features.** In the pipeline's feature file (one that `extends:` the base keeps the payment features
   as they are), add features with `event_types: [session]` and, where needed, filters scoped
   `event_types: [session]`. `pulsebench bindings check` validates the fields against the message.
5. **Traffic.** `traffic.event_types: {session: 0.5}` makes the generator draw them around payments,
   taking each payment's entities through your binding (`python/realy/gen/sessions.py`; add a fill function
   there for realistic field values). Your own producer works the same way: the message on its topic, keyed
   by its partition entity, with header `x-event-type: session`.
6. **Prove it.** `make up PIPELINE=<your pipeline>`, a fresh-state scenario with `--verify`.

Limits: at most 64 event types and 63 filters per pipeline; a feature's state layout changes when its
`event_types` change, so the next run starts that state fresh.

## Walkthrough: add a field to an event

What used to take six files now takes the contract and the producer.

1. **Contract.** Append the field to the message in `contracts/proto` with the next free number, run
   `scripts/gen-proto-python.sh` and `python -m realy.proto_compat --update` (the compatibility test fails
   until you do, so additions are deliberate).
2. **Producer.** Fill it in the generator (`python/realy/gen/batch.py` reflects every field of the
   message into its columns) or in your own producer.
3. **Regenerate.** `make codegen && make docs`: the ClickHouse events table and the decisions table's
   `event.*` columns gain it (from the descriptor), and the bindings reference lists it. `make migrate`
   adds the columns to a running stack in place.

Nothing else: the CEL `event` view in both languages has it (rules and expression features can use it at
once), and event logs written before it read its default. To
aggregate over it in a stream feature (`value: <field>`), declare the feature; the row plan gives it a
slot. Prove it with `pytest`, `./gradlew test` and a verified run.

## Walkthrough: add an entity

Append it to `contracts/entities.yaml` (with `owned_by` if it belongs to one partition entity), map it in
each event type that carries it (`entities: {beneficiary_bank: bank_code}`; a string id is hashed with
FNV-1a 64 into the key) and in the feature schema's `entities:` map (the same field; a test keeps the two
consistent), then declare stream or profile features on it. The Flink job gains a keyed stage
for it (uid `features-<entity>`), the online store gains `fs:<entity>:<id>` blobs, and the state layout
changes, so the next run starts the feature state fresh (the runner says so) and warms it from history.
