# Extending the platform: what is configuration, what is code, and how to add code safely

Most changes to the pipeline are contract edits, with no code: features, filters, windows, entities,
messages, multi-message transactions, rules, thresholds, topics and topologies. This page covers the
rest: the places where you add Java (and its Python twin) and the rules that keep the benchmark exact
while you do it.

| You want | How | Guide |
|---|---|---|
| a feature, filter, window or entity | `contracts/features/feature_schema.yaml` | [features](features.md) |
| a feature computed from other features or event fields | `derived` with a CEL `expr:` | [features](features.md#walkthrough-1-an-expression-feature-no-code) |
| a message or a multi-message transaction | `contracts/events/`, `contracts/transactions/` | [schemas](schemas.md), [transactions](transactions.md) |
| a rule, threshold or step-up policy | `rules/*.yaml`, live config | [rules](../reference/rules.md) |
| a topology (which components, in which order) | `contracts/topologies.yaml` | [topologies](topologies.md) |
| **an aggregation kind** (`agg:` other than the eight built-ins) | an `Aggregation` plugin, Java + Python twin | this page |
| another feature store | `OnlineStore` (Java interface, `io.realy.scoring.store`) | [feature store](../feature-store-and-inference.md) |
| another model protocol | `ModelClient` (`io.realy.scoring.model`) | [feature store and inference](../feature-store-and-inference.md) |

## The twin rule

Every computation on the decision path exists twice: in Java (Flink and the gateway compute it) and in
Python (`pulsebench verify` recomputes it independently from the event log). The benchmark's
correctness claims (exact features, rule, score and outcome parity) rest on the two agreeing. So an
extension is accepted only with its Python twin, and both must pass the same fixtures:

- Java: [`AggregationConformance`](../../java/scoring-core/src/main/java/io/realy/scoring/features/AggregationConformance.java)
  replays a fixture through the registered Java aggregation;
- Python: [`realy.features.aggregations`](../../python/realy/features/aggregations.py) writes the
  fixture (`contracts/testdata/aggregations/*.json`) from the twin;
- values agree to 1e-9 (relative), which is far stricter than verify's tolerance and only holds when the
  windowing, eviction and fold logic agree exactly.

The preflight enforces the rule at run time: when the gateway reports a plugin aggregation that has no
Python twin loaded, `pulsebench infra check` fails with the missing names (verify could not check them).

## Aggregations: how the extension point works

A stream feature names its aggregation with `agg:`. Each aggregation declares:

| Property | Values | What it decides |
|---|---|---|
| `name` | the `agg:` value | how features refer to it |
| `shape` | `BUCKETS` (time buckets with a sum `a` and count `b` each), `SEEN` (keys with their last bucket), `LAST` (the last event's time), `LAST_GEO` (time and location) | how its state is kept and encoded in the entity's state blob |
| `input` | `NONE`, `NUMERIC` (`value:` is a numeric field), `KEY` (`value:` is a field used as a key), `LOCATION` | which event values the source decodes for it (`RowPlan` slots) and whether `value:` is required |
| `apply` | fold the matching event in, then return the value ("update, then read") | every folded event |
| `read` | the value as of the event's time, without folding it | a re-scored lifecycle stage |

Because a new aggregation reuses one of the four state shapes, it needs no new state encoding: the
codec, the online-store blob layout, eviction and restore are the built-ins'. The registry is
[`Aggregations`](../../java/scoring-core/src/main/java/io/realy/scoring/features/Aggregations.java)
(Java) and [`aggregations.py`](../../python/realy/features/aggregations.py) (Python). The feature schema
refuses an unknown `agg:` in both languages, and refuses a numeric or key aggregation without `value:`.

The built-ins are registered the same way: `count`, `sum`, `avg` and `max` are `BucketAgg`s that differ
only in their contribution, merge and window fold; `distinct`, `novel`, `time_since_last` and
`geo_speed_kmh` implement the interface directly.

## Walkthrough: add `min`

The repository ships this example as [`java/plugin-example`](../../java/plugin-example) and
[`python/realy/plugin_example.py`](../../python/realy/plugin_example.py).

**1. Java.** A `BucketAgg` with `Math::min` as the merge (no count merge), folded to the window's
minimum:

```java
public final class MinAggregation extends Aggregations.BucketAgg {
    public MinAggregation() {
        super("min", Aggregation.Input.NUMERIC, Math::min, false);
    }

    @Override
    protected double window(FeatureSpec s, double a, double b, double folded, boolean any) {
        return any ? folded : s.defaultValue();
    }
}
```

Register it in `src/main/resources/META-INF/services/io.realy.scoring.features.Aggregation` (one class
name per line). The project depends on `scoring-core` as `compileOnly`: the gateway and the Flink job
provide it.

**2. Python twin.** The same aggregation, registered under the same name:

```python
from realy.features.aggregations import BucketAgg, register

register(BucketAgg("min", "numeric", merge=min, count_merge=False,
                   window=lambda s, a, b, folded, any_: folded if any_ else s.default))
```

An aggregation that does not fit `BucketAgg` subclasses `Aggregation` (Python) or implements
`Aggregation` (Java) with `apply` and `read` over the `Window` of its shape.

**3. Fixture.** Write the conformance fixture from the twin, then check the Java side against it:

```bash
cd python && python -m realy.features.aggregations conformance --plugin realy.plugin_example --agg min
# -> contracts/testdata/aggregations/min.json (NAME:window_s:granularity_s:default to choose the window)
gradle -p java :plugin-example:test      # MinAggregationTest: ServiceLoader finds it; conformant
```

`python -m realy.features.aggregations list` shows every registered aggregation, its shape and input.

**4. Package and deploy.** Build the jar and put it, with the twin, where every component that computes
or verifies features can load it:

| Component | Setting | Compose (`deploy/compose/plugins/`, mounted at `/opt/realy/plugins`) |
|---|---|---|
| gateway | `PLUGINS_DIR` (every `*.jar` in it) | set |
| Flink JobManager and TaskManagers | `PLUGINS_DIR` (the same jars as the gateway) | set |
| console, runner, verify | `REALY_AGGREGATION_PLUGINS` (twin module names); `PLUGINS_DIR/python` is on the import path; installed packages can use the `pulsebench.aggregations` entry point group instead | `PLUGINS_DIR` set; set `REALY_AGGREGATION_PLUGINS` when you start the stack |

```bash
gradle -p java :plugin-example:jar
cp java/plugin-example/build/libs/realy-plugin-example.jar deploy/compose/plugins/
REALY_AGGREGATION_PLUGINS=realy.plugin_example docker compose -f deploy/compose/docker-compose.yml up -d
```

In Kubernetes, mount the jars (an init container or a volume) on the gateway and Flink pods, set
`PLUGINS_DIR` there, and install the twin in the console image.

**5. Use it.** Append a feature to the schema:

```yaml
  - { name: cust_min_amount_1h, source: stream, entity: customer, agg: min, value: amount_usd_minor,
      window: 1h, granularity: 1m }
```

**6. Prove it.** `pulsebench infra check` lists the gateway's plugin aggregations (from `/admin/info`,
`extensions`) and fails if a twin is missing. A verified run (`pulsebench run smoke --verify`) then
recomputes the new feature from the event log and compares it with what the pipeline served.

## Limits

- **State shapes are fixed.** An aggregation that needs state none of the four shapes can hold (a
  sketch, a histogram) needs a new shape: a change to the state codec in both languages and a new state
  layout version. That is a core change, not a plugin.
- **The legacy fallback is built-in only.** When an online-store blob carries no state (a blob written
  before state blobs, or by an external writer), the gateway re-adds the current event to the stored
  values for the built-ins only. A plugin feature on such a blob reads the stored value. Every blob the
  bundled Flink job writes carries its state, so this does not apply to it.
- **The same jars everywhere.** The gateway, every Flink process and the verifier must run the same
  version of a plugin. A difference shows up as feature mismatches in verify; the preflight checks only
  that the names are present.
- **Changing a plugin's semantics changes results.** The schema's state layout hash covers the feature
  definitions, not the plugin's code. Bump the aggregation's name (`min_v2`) when its semantics change,
  so the layout changes and the state starts fresh.

## Planned extension points

- `DerivedFn`, for `fn:` features: today the library functions are Java (`DerivedFeatures`) and Python
  (`features/derived.py`) code, and CEL `expr:` covers most needs without code.
- A `DecisionSink` interface for the gateway (today Kafka plus the on-disk spool).
