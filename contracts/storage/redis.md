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
u16  schema_version
u16  n                       number of float values that follow
i64  as_of_us                event time the values were computed at
i64  updated_at_us           wall time of the write (staleness = now - updated_at_us)
f32  values[n]               in feature_schema.yaml order, restricted to that entity's features
                             (profile blobs: profile features, then profile_attributes)
```

TTL: `fs:*` 8 days (refreshed on write), `pf:*` none.
The scoring orchestrator adds the current event's own contribution to `count`/`sum` features when
`ScoringConfig.self_inclusion = true` (sync path), because the Flink write for that event cannot
have happened yet. Staleness of every blob is recorded in `DecisionRecord.features.freshness`.
