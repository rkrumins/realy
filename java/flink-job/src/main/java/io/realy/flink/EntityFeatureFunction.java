package io.realy.flink;

import io.realy.common.BenchClock;
import io.realy.common.Env;
import io.realy.common.StageMetrics;
import io.realy.contracts.bench.v1.Stage;
import io.realy.scoring.features.EntityAggregator;
import io.realy.scoring.features.FeatureAssembler;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.store.RedisOnlineStore;
import io.realy.scoring.store.StoreBlob;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Keyed stage for one entity (customer, card, device, ...). Holds the entity's sparse window state
 * as one RocksDB value (real state I/O + serialization on every event), computes its features into
 * the envelope and, in `features` mode, writes the entity blob to the online store for the gateway.
 * With {@code storeState} the blob also carries the aggregator state itself (StoreBlob v2), so the
 * gateway re-windows it at each event's time and serves exact values instead of "as of the entity's
 * last event" ones.
 * Events without the entity are keyed by event id (no hot "0" key) and get defaults.
 *
 * <p>A stage also computes the entities it hosts ({@link #hosting}): owned entities that do not directly follow
 * their owner (an account inside its customer's stage, EntityRegistry.stageOf), each keyed by its own id in a
 * MapState under the owner's key, so their events keep the owner's order without a shuffle of their own.
 *
 * <p>Store writes go through a {@link StoreWriter}: bounded in flight (a slow store backpressures the
 * job), drained before every checkpoint completes (a completed checkpoint implies the store holds every
 * write before it) and failing the task when the store stays unavailable (restart, replay from the last
 * checkpoint). Spans are recorded only when decisions are produced (inline / both): in `features` mode
 * nothing reads them, so they would only cost allocation and serialization at every shuffle.
 */
public final class EntityFeatureFunction extends KeyedProcessFunction<Long, Envelope, Envelope>
        implements CheckpointedFunction {
    private final String entity;
    private final String schemaYaml;   // resolved once by the job's main (null: this process's schema)
    private final boolean writeStore;
    private final boolean storeState;
    private final boolean spans;
    private List<String> hosted = List.of();   // owned entities computed in this stage (keyed inside the owner's key)
    private List<String> storeStateOf = List.of();
    private transient Unit main;
    private transient List<Unit> units;        // this stage's entity, then the entities it hosts
    private transient RedisOnlineStore store;
    private transient StoreWriter writer;
    private transient long flushTimeoutMs;
    private transient long reportedErrors;
    private transient int entityEnum;
    private transient int schemaVersion;
    private transient StageMetrics metrics;
    private transient long ttlSeconds;
    private final List<String> later;          // the entities of the stages after this one
    private final String pipeline;             // the pipeline the job was planned for
    private transient boolean[] laterFolds;    // per event type ordinal: does a later stage fold it?
    private transient int defaultType;
    private transient java.util.Map<String, Integer> typeOrdinals;
    private transient float[] scratch;         // a fold-only event's values (for its store blob)

    public EntityFeatureFunction(String entity, boolean writeStore) {
        this(entity, writeStore, false);
    }

    public EntityFeatureFunction(String entity, boolean writeStore, boolean storeState) {
        this(entity, writeStore, storeState, true, null);
    }

    public EntityFeatureFunction(String entity, boolean writeStore, boolean storeState, boolean spans, String schemaYaml) {
        this(entity, writeStore, storeState, spans, schemaYaml, List.of());
    }

    public EntityFeatureFunction(String entity, boolean writeStore, boolean storeState, boolean spans, String schemaYaml,
                                 List<String> later) {
        this(entity, writeStore, storeState, spans, schemaYaml, later, null);
    }

    /**
     * @param later    the entities of the stages after this one (a fold-only event goes on only if one folds it)
     * @param pipeline the pipeline the job was planned for (null = this process's PIPELINE)
     */
    public EntityFeatureFunction(String entity, boolean writeStore, boolean storeState, boolean spans, String schemaYaml,
                                 List<String> later, String pipeline) {
        this.pipeline = pipeline;
        this.schemaYaml = schemaYaml;
        this.entity = entity;
        this.writeStore = writeStore;
        this.storeState = storeState;
        this.spans = spans;
        this.later = List.copyOf(later);
    }

    /**
     * Owned entities this stage also computes, each keyed by its own id inside this stage's key (EntityRegistry.stageOf),
     * and the hosted entities whose online-store blob carries the aggregator state.
     */
    public EntityFeatureFunction hosting(List<String> entities, List<String> withState) {
        this.hosted = List.copyOf(entities);
        this.storeStateOf = List.copyOf(withState);
        return this;
    }

    @Override
    public void open(OpenContext ctx) {
        FeatureSchema schema = FeatureSchema.of(schemaYaml);
        var pipe = io.realy.scoring.binding.PipelineBinding.of(pipeline);
        defaultType = pipe.defaultOrdinal();
        typeOrdinals = new java.util.HashMap<>();
        laterFolds = new boolean[64];
        for (String e : later) {
            if (schema.stream(e).isEmpty()) continue;
            EntityAggregator a = new EntityAggregator(schema, e, pipe);
            for (int t = 0; t < 64; t++) laterFolds[t] |= a.folds(t);
        }
        scratch = new float[schema.size()];
        schemaVersion = schema.version;
        entityEnum = FeatureAssembler.entityEnum(entity).getNumber();
        long ttlDays = Env.lng("STATE_TTL_DAYS", 8);
        ttlSeconds = ttlDays * 86400;
        StateTtlConfig ttl = StateTtlConfig.newBuilder(Duration.ofDays(ttlDays))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1000)
                .build();
        units = new java.util.ArrayList<>();
        main = new Unit(entity, new EntityAggregator(schema, entity, pipe), storeState);
        ValueStateDescriptor<byte[]> d = new ValueStateDescriptor<>("w-" + entity, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
        d.enableTimeToLive(ttl);
        main.value = getRuntimeContext().getState(d);
        // a status that holds until it changes (`latest` without a window: card_blocked) must outlive the TTL above:
        // a blocked card left dormant for longer still reads blocked. Its state is kept apart without expiry, and
        // only while it is not the default (cleared on an unblock), so this holds one entry per entity whose status
        // is not the default
        if (main.agg.hasSticky())
            main.stickyValue = getRuntimeContext().getState(new ValueStateDescriptor<>("s-" + entity, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
        units.add(main);
        for (String h : hosted) {
            Unit u = new Unit(h, new EntityAggregator(schema, h, pipe), storeStateOf.contains(h));
            MapStateDescriptor<Long, byte[]> md = new MapStateDescriptor<>("w-" + h, Types.LONG, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
            md.enableTimeToLive(ttl);
            u.map = getRuntimeContext().getMapState(md);
            if (u.agg.hasSticky())
                u.stickyMap = getRuntimeContext().getMapState(new MapStateDescriptor<>("s-" + h, Types.LONG, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
            units.add(u);
        }
        if (writeStore) {
            store = RedisOnlineStore.shared(Env.str("REDIS_URI", "redis://localhost:6379"), 4);
            // in flight per subtask: across all subtasks this bounds Redis's queue, so it must stay well
            // below (Redis ops/s x command timeout): 1000 x 14 subtasks ~ 0.3 s of queue at 50k ops/s
            writer = new StoreWriter(store, Env.integer("STORE_MAX_IN_FLIGHT", 1000), Env.lng("STORE_FAIL_AFTER_MS", 30_000));
            flushTimeoutMs = Env.lng("STORE_FLUSH_TIMEOUT_MS", 60_000);
        }
        metrics = FlinkMetrics.get();
    }

    @Override
    public void processElement(Envelope env, Context ctx, Collector<Envelope> out) throws Exception {
        long start = BenchClock.nowUs();
        int dom = BenchClock.domain();
        if (spans) env.span(Stage.SHUFFLE_VALUE, entityEnum, dom, env.lastEmitUs, start - env.lastEmitUs);
        int type = env.type == null ? defaultType : typeOrdinals.computeIfAbsent(env.type,
                t -> io.realy.scoring.binding.PipelineBinding.of(pipeline).typeOrdinal(t));
        // a fold-only type (a login, device telemetry) carries no feature vector: it updates state, nothing reads it
        boolean foldOnly = env.features == null;
        for (Unit u : units) u.process(env, type, foldOnly);
        long end = BenchClock.nowUs();
        if (spans) env.span(Stage.STATE_FEATURES_VALUE, entityEnum, dom, start, end - start);
        metrics.recordUs("state_" + entity, end - start);
        metrics.recordUs("shuffle_" + entity, start - env.lastEmitUs);
        env.lastEmitUs = end;
        // a fold-only event goes no further than the last stage that folds its type (a login never reaches
        // the merchant, terminal or payee shuffles)
        if (foldOnly && (type < 0 || !laterFolds[type])) {
            metrics.add("typed_done_" + entity, 1);
            return;
        }
        out.collect(env);
    }

    /**
     * One entity computed in this stage: the stage's own (its state is the keyed ValueState) or a hosted one (a
     * MapState entry by its id under the stage's key). Windows expire with the state TTL; sticky statuses are kept
     * apart without expiry while they are not the default.
     */
    private final class Unit {
        final String name;
        final EntityAggregator agg;
        final boolean withState;
        ValueState<byte[]> value, stickyValue;
        MapState<Long, byte[]> map, stickyMap;

        Unit(String name, EntityAggregator agg, boolean withState) {
            this.name = name;
            this.agg = agg;
            this.withState = withState;
        }

        void process(Envelope env, int type, boolean foldOnly) throws Exception {
            long key = agg.key(env.row);
            if (key == 0) {
                if (!foldOnly) agg.defaults(env.features);
                return;
            }
            if (env.readOnly) {
                // a re-scored stage of a transaction already folded in when it was assembled: read only
                agg.read(current(key), env.row, env.filterMask, env.features);
                return;
            }
            if (foldOnly && (type < 0 || !agg.folds(type))) return;   // nothing of this entity is fed by this type
            byte[] prev = current(key);
            float[] values = foldOnly ? scratch : env.features;
            byte[] next = agg.process(prev, env.row, env.filterMask, values, type);
            if (next == prev) return;
            put(key, next);
            if ((stickyValue != null || stickyMap != null) && type >= 0 && agg.foldsSticky(type)) {
                byte[] sticky = agg.stickyState(next);
                if (stickyValue != null) {
                    if (sticky == null) stickyValue.clear();
                    else stickyValue.update(sticky);
                } else if (sticky == null) {
                    stickyMap.remove(key);
                } else {
                    stickyMap.put(key, sticky);
                }
            }
            if (writeStore) write(env, key, next, values);
        }

        /** The entity's state; when the windowed state has expired (TTL), its sticky statuses alone. */
        private byte[] current(long key) throws Exception {
            byte[] b = value != null ? value.value() : map.get(key);
            if (b == null) {
                byte[] s = stickyValue != null ? stickyValue.value() : stickyMap != null ? stickyMap.get(key) : null;
                if (s != null) b = agg.restoreSticky(s);
            }
            return b;
        }

        private void put(long key, byte[] next) throws Exception {
            if (value != null) value.update(next);
            else map.put(key, next);
        }

        private void write(Envelope env, long key, byte[] next, float[] values) throws Exception {
            int[] idx = agg.featureIndices();
            float[] vals = new float[idx.length];
            for (int i = 0; i < idx.length; i++) vals[i] = values[idx[i]];
            long asOf = EntityAggregator.lastEventUs(next);
            long w0 = BenchClock.nowUs();
            long ingested = env.ingestUs;
            // history replay (x-backfill) is a bulk load measured on its own stages: live freshness and
            // write latency must describe live events only (object reuse: read the flag now, not in the callback)
            String sfx = env.backfill ? "_backfill" : "";
            StoreBlob blob = withState
                    ? new StoreBlob(schemaVersion, asOf, w0, vals, env.eventId, next)
                    : new StoreBlob(schemaVersion, asOf, w0, vals);
            byte[] bytes = blob.encode();
            metrics.add("store_write_bytes", bytes.length);
            // a non-default status keeps its online-store key too (no expiry), so the gateway reads it however
            // long the entity stays dormant; the next write after it returns to the default restores the expiry
            long ttl = agg.holdsSticky(values) ? 0 : ttlSeconds;
            writer.write(StoreBlob.streamKey(name, key), bytes, ttl, err -> {
                if (err != null) {
                    metrics.add("store_write_errors", 1);
                    return;
                }
                long done = BenchClock.nowUs();
                metrics.recordUs("feature_store_write" + sfx, done - w0);
                // Freshness of the online store: how long a new event stays invisible to the
                // gateway (Kafka pickup by Flink -> entity blob durable in Redis).
                metrics.recordUs("store_update_lag" + sfx, done - ingested);
            });
        }
    }

    /** Before the checkpoint barrier moves on: every store write so far must have landed. */
    @Override
    public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
        if (writer != null) writer.flush(flushTimeoutMs);
    }

    @Override
    public void initializeState(FunctionInitializationContext ctx) {}

    @Override
    public void close() throws Exception {
        try {
            if (writer != null) writer.flush(Math.min(flushTimeoutMs, 10_000));
        } catch (Exception e) {
            // closing after a failure or cancellation: the restart replays from the last checkpoint
        } finally {
            if (store != null) store.close();
        }
    }
}
