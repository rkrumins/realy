# Online feature store layout (Redis)

All values are **binary blobs** read with pipelined `GET`s (one round trip for all entities of an
event; works on Redis Cluster because the client pipelines per slot).

| Key | Writer | Contents |
|-----|--------|----------|
| `fs:{entity}:{id}` | Flink feature job (`features` mode) | stream features of that entity |
| `pf:customer:{id}` | `realy world seed-redis` | customer profile features + attributes |
| `pf:merchant:{id}` | `realy world seed-redis` | merchant profile features |

`{entity}` is the lowercase entity name from `feature_schema.yaml` (`customer`, `card`, `device`,
`ip`, `merchant`, `terminal`, `payee`); `{id}` is the decimal entity key.

## Blob format (little-endian)

```
u16  schema_version          | 0x8000 = state blob (v2, below)
u16  n                       number of float values that follow
i64  as_of_us                event time the values were computed at
i64  updated_at_us           wall time of the write (staleness = now - updated_at_us)
f32  values[n]               in feature_schema.yaml order, restricted to that entity's features
                             (profile blobs: profile features, then profile_attributes)
--- state blobs only (stream entities listed in the Flink job's --store-state, default customer,card)
u64  last_event_id           the event these values were computed for
u32  state_len
u8   state[state_len]        the entity's windowed aggregator state (EntityAggregator encoding:
                             sparse time buckets, distinct/novel key sets, last-event fields)
```

**Values blobs** hold features *as of the entity's last event*: a 10-minute count read 25 minutes
later still counts events that have left the window (measured: 82.6% exact agreement on the gateway
path). **State blobs** let the gateway compute exact values instead
(`FeatureAssembler.fillStream`):
* `last_event_id` = the current event (Flink processed it first): serve `values` as stored;
* otherwise re-window `state` at the event's own time and apply the event: `EntityAggregator.evaluate`,
  the same code and update-then-read semantics as the Flink-inline path;
* no blob although the lookup succeeded: an entity never seen before, so first-event values.

The state section comes after the values, so a values-only reader keeps working. Costs: blobs grow
from ~60-90 B to typically 0.3-1.5 KB for a customer/card (bounded by its windows; see
`docs/production.md` for the Redis bandwidth this implies at 20k TPS). Shared, high-rate entities
(merchant, device, ip, terminal, payee) stay values-only: frequent writes keep them fresh, and they are
not required to be exact. The remaining inexactness is **store lag**: earlier events of the same entity
that Flink has not written yet when the gateway reads (`store_update_lag` metric).

TTL: `fs:*` 8 days (refreshed on write), `pf:*` none.
The scoring orchestrator adds the current event's own contribution to `count`/`sum` features when
`ScoringConfig.self_inclusion = true` (sync path), because the Flink write for that event cannot
have happened yet. Staleness of every blob is recorded in `DecisionRecord.features.freshness`.
