# Schemas: entities, event types and pipelines

The pipeline does not hard-code its messages. Three contracts say what they mean, and Java
(`io.realy.scoring.binding`) and Python (`realy.bindings`) read the same files:

| Contract | Says | Generated reference |
|---|---|---|
| [`contracts/entities.yaml`](../../contracts/entities.yaml) | every kind of thing events refer to and state is kept for: id type, which entity owns it, its `Entity` enum value. The order is the Flink job's keyed-stage order | [reference/bindings.md](../reference/bindings.md#entities) |
| [`contracts/events/<type>.yaml`](../../contracts/events/payment.yaml) | what a protobuf message means: its id, event time, which fields hold which entities, location, amount | [reference/bindings.md](../reference/bindings.md) (every field) |
| [`contracts/pipelines/<name>.yaml`](../../contracts/pipelines/payments.yaml) | which event types flow (scored or only folded into state), the partition entity, the namespace, topics, features and model | [reference/bindings.md](../reference/bindings.md#pipelines) |

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
