# Features: add, change and prove them

Every feature is declared once in [`contracts/features/feature_schema.yaml`](../../contracts/features/feature_schema.yaml).
That one file drives the Flink job (keyed aggregators), the online-store layout, the model's input
tensor (feature order = tensor index), the ClickHouse decision columns and the Python reference used by
`verify`. Concepts (windows, buckets, online state, freshness) are in
[the feature store guide](../feature-store-and-inference.md); this page is the how-to. The generated
list of every feature is [reference/features.md](../reference/features.md).

## The four kinds

| Source | Computed by | Example |
|---|---|---|
| `stream` | Flink, per entity, over event-time windows (`count`, `sum`, `max`, `avg`, `distinct`, `novel`, `time_since_last`, `geo_speed_kmh`), optionally behind a named `filter` | `card_small_cnp_count_10m`: small card-not-present payments on the card in 10 minutes |
| `profile` | seeded from the world into Redis (`pf:<entity>:<id>`) | `cust_tenure_days` |
| `derived` with `fn:` | a built-in function at scoring time (Java `DerivedFeatures`, Python `features/derived.py`) | `dist_from_home_km` |
| `derived` with `expr:` | a CEL expression over `event` and the features computed before it (`f.<name>`) | `cvv_mismatch` |

Semantics that matter when you add one: windows are bucketed (`granularity`), count / sum / max / avg /
distinct include the current event ("update, then read"), novel / time-since-last compare with the
*previous* events, and an event without the entity gets the feature's `default` and is flagged missing.

## Rule one: append, never insert

Features are only ever appended to the end of the list. The vector of a run under an earlier schema is
then a prefix of today's, so old decisions, old models and old event logs keep working (models bind
their inputs by name; narrower vectors are padded with defaults). Moving or deleting a feature would
silently shift every model input after it.

## Walkthrough 1: an expression feature (no code)

Flag online payments well above the customer's usual ticket that were not 3-D Secure authenticated:

<!-- walkthrough:expression-feature (python/tests/test_walkthroughs.py runs this snippet) -->
```yaml
  - { name: big_unauthenticated_cnp, source: derived,
      expr: "!event.card_present && event.payment_type == 'CARD_PURCHASE' && event.three_ds != 'THREE_DS_AUTHENTICATED' && f.amount_to_avg_ratio > 5.0 ? 1.0 : 0.0" }
```

Append it at the end of `features:`. `event.*` exposes every event field (enums by name), `f.*` every
feature computed before it in the list. Both Java and Python evaluate the same CEL, so no code changes.

## Walkthrough 2: a stream feature

Count a device's distinct payees in 24 hours (one device paying many new payees is a mule-herding
shape):

<!-- walkthrough:stream-feature (python/tests/test_walkthroughs.py runs this snippet) -->
```yaml
  - { name: dev_distinct_payees_24h, source: stream, entity: device, agg: distinct, value: payee_id,
      filter: transfer, window: 24h, granularity: 30m }
```

- `value` is an event field; `filter` a named predicate from `filters:` (add one if needed, as CEL).
- Choose the granularity for the precision you need: buckets per window cost state (about 4–8 bytes
  each in the compact layout) on every entity.
- The entity's state layout changes, so its keyed state resets at its next event and the runner starts
  the Flink state fresh, warmed with history (the state layout hash is part of the provenance identity).
  Plan a history replay (`traffic.backfill_days`) long enough for the window.

## Walkthrough 3: a profile feature

Add the attribute to the world (`world/build.py`), export it in `customer_profile_arrays()` (or the
merchant one; `World.profile_arrays(entity)` is what the seeder and the verifier read) and declare it
with `source: profile`. Nothing else: each entity's profiles carry a marker (`pf:_meta:<entity>`) with
the world's hash and the blob layout's hash, so the next run's "profiles" step (or `pulsebench seed`)
reseeds exactly the entities whose layout or world changed. An entity with profile features that the
world cannot supply fails the seeding with its name instead of serving defaults.

## Prove it

1. `pytest` and `./gradlew test` (the Java and Python schema loaders must agree).
2. A verified run: `pulsebench run smoke --verify` (or *verify correctness* in the console). The verifier
   recomputes every feature from the event log in Python and compares with what the pipeline served:
   exact for customer / card / profile / derived features, an agreement rate for entities shared across
   customers (their order across partitions is arrival-dependent, as in production).
3. The new column appears in the Explorer's decision view and, after `make codegen && make migrate`,
   in ClickHouse (`f_<name>`).
4. To make the model use it, retrain (`pulsebench train`) and deploy the new version; models trained
   before it keep working (they bind their inputs by name).

## Where the code is

| Concern | Java | Python twin |
|---|---|---|
| Schema loading | `scoring-core/.../features/FeatureSchema.java` | `features/schema.py` |
| Stream aggregation | `EntityAggregator.java` | `features/engine.py` |
| Derived and CEL features | `DerivedFeatures.java` | `features/derived.py` |
| Event view (what CEL sees) | `EventView.java` | `event_view` in `features/derived.py` |
| Assembly at scoring time | `FeatureAssembler.java` | `verify/verify.py` |

The twins are kept equal by `verify`: any difference shows up as a feature mismatch.
