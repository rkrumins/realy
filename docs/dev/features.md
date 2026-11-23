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
| `stream` | Flink, per entity, over event-time windows (`count`, `sum`, `max`, `avg`, `distinct`, `novel`, `time_since_last`, `geo_speed_kmh`, `latest`, or a [plugin aggregation](extending.md)), optionally behind a named `filter` | `card_small_cnp_count_10m`: small card-not-present payments on the card in 10 minutes |
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

## Features fed by other message types

A stream feature folds the events of its pipeline's default type (payments) unless it says otherwise:

```yaml
filters:
  login_failed: { when: "event.outcome != 'LOGIN_SUCCESS'", event_types: [login] }   # a login's field
features:
  - { name: cust_failed_logins_1h, source: stream, entity: customer, agg: count, filter: login_failed,
      event_types: [login], window: 1h, granularity: 1m }
  - { name: dev_distinct_customers_all_24h, source: stream, entity: device, agg: distinct, value: customer_id,
      event_types: [payment, login], window: 24h, granularity: 30m }
```

- `event_types` lists the types that fold into the feature; a payment reads it as of its own time
  without being counted. The key is `event_types`, not `on`, because YAML 1.1 reads a bare `on` as
  `true`, so the two languages would disagree; both loaders reject `on:`.
- A filter with `event_types` applies only to those types and reads their fields; a plain filter
  applies to the default type. For any other type its bit is 0.
- A feature file can `extends: <base file>`: the base's entities, filters and features come first,
  unchanged (same indices, same state bytes), then the file's own. Both languages hash the bundle
  (base, then extension), and a name defined twice is a load error.

Which messages exist, how they are routed and which features stay exact: [schemas](schemas.md#message-types-that-carry-a-subset-of-the-entities).

## Entity status from change feeds

"Is this card blocked?", "is this account frozen?", "is this payee a known mule?" are statuses owned by
other systems (card management, core banking, fraud operations). When those systems publish their changes,
the pipeline folds them into state like any other message, and a payment reads the status as a feature of
its own time: computed where its other features are, with nothing to look up on the hot path
(pipeline `payments-status`, `contracts/features/feature_schema_status.yaml`).

```yaml
filters:
  card_blocking: { when: "event.status in ['CARD_STATUS_BLOCKED', 'CARD_STATUS_LOST', 'CARD_STATUS_STOLEN']",
                   event_types: [card_status_changed] }
features:
  - { name: card_blocked, source: stream, entity: card, agg: latest, filter: card_blocking,
      event_types: [card_status_changed] }
  - { name: card_status_code, source: stream, entity: card, agg: latest, value: status,
      event_types: [card_status_changed], default: 1 }
  - { name: dev_blocked_card_attempts_24h, source: stream, entity: device, agg: count, gate: card_blocked,
      window: 24h, granularity: 1h }
```

- **`agg: latest`** is the value of the latest folded change, as of the event's time. Without `value:` it
  is the change's filter bit, so `card_blocked` is 1 after a blocking change and 0 after an unblock; with
  `value:` it is the field (an enum reads as its number). A change older than the latest is ignored (it
  arrived late); read before the latest change's time it gives the value before it. State: the
  `last_geo` shape (time, value, value before), so the state encoding is unchanged.
- **No `window`: sticky.** The value holds until it changes, including past the keyed state's TTL
  (`STATE_TTL_DAYS`, 8 days of processing time): a stolen card left dormant for a month still reads
  blocked. The Flink stage keeps a sticky value in a second state (`s-<entity>`) that never expires,
  only while it is not the default (an unblock clears it), and writes the entity's online-store key
  without expiry meanwhile. With a `window`, the value reads the default once it is older than the
  window. Pick the default to be the common state (`card_status_code` defaults to 1, ACTIVE), or every
  card that ever changed would be kept.
- **`gate: <feature>`** folds the feature only while `<feature>` is non-zero for the event: a status
  feeding a Flink feature. The gate must be computed earlier, in an earlier stage (registry order) or
  earlier in the same entity, which both loaders check; a gated feature folds only scored types (a
  fold-only message carries no feature vector to read its gate from).
- **Exactness.** Card and account changes carry the customer and travel on the events topic, so they
  are exact against the payments (the account is computed inside its customer's stage:
  [schemas](schemas.md#exactness-follows-ownership)). A payee listing has no customer: it travels on its
  own topic keyed by payee and its features are agreement-only.
- **Who owns the data.** The systems of record do; the pipeline owns its copy as state, rebuilt by
  replaying the change topics. For data no system publishes changes of (a third-party risk score),
  a lookup at decision time is the other path: planned as `contracts/lookups` (Redis keys and HTTP
  services, with a cache and a deadline), and audited per decision.

The scenario side is `traffic.status` (`gen/sor.py`): simulated systems of record that block a card at
its k-th fraudulent payment (the issuer's monitoring) or when its fraud is reported, lock cards and put
accounts on AML holds for a while, freeze an account taken over and list a payee at its k-th fraud label.
Genuine payments stop on a blocked card while fraud attempts continue, which is what a blocked-card rule
(`rules/baseline-status.yaml`) catches.

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

## A new aggregation kind

The nine built-ins cover counts, sums, averages, maxima, distinct counts, novelty, recency, speed and the latest value of a change feed.
Anything else (a minimum, a weighted sum, a ratio of two windows) is an `Aggregation` plugin: one Java
class and its Python twin, proved equal by shared fixtures. It reuses one of the four state shapes, so
the state codec and the online-store layout stay as they are. Walkthrough and rules:
[extending the platform](extending.md#walkthrough-add-min).

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
| Aggregations (registry, built-ins, plugins) | `Aggregations.java`, `Aggregation.java` | `features/aggregations.py` |
| Stream aggregation (state, codec, stages) | `EntityAggregator.java` | `features/engine.py` |
| Derived and CEL features | `DerivedFeatures.java` | `features/derived.py` |
| Event view (what CEL sees) | `EventView.java` | `event_view` in `features/derived.py` |
| Assembly at scoring time | `FeatureAssembler.java` | `verify/verify.py` |

The twins are kept equal by `verify`: any difference shows up as a feature mismatch.
