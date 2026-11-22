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
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
    private transient EntityAggregator agg;
    private transient ValueState<byte[]> state;
    private transient RedisOnlineStore store;
    private transient StoreWriter writer;
    private transient long flushTimeoutMs;
    private transient long reportedErrors;
    private transient int entityEnum;
    private transient int schemaVersion;
    private transient StageMetrics metrics;
    private transient long ttlSeconds;

    public EntityFeatureFunction(String entity, boolean writeStore) {
        this(entity, writeStore, false);
    }

    public EntityFeatureFunction(String entity, boolean writeStore, boolean storeState) {
        this(entity, writeStore, storeState, true, null);
    }

    public EntityFeatureFunction(String entity, boolean writeStore, boolean storeState, boolean spans, String schemaYaml) {
        this.schemaYaml = schemaYaml;
        this.entity = entity;
        this.writeStore = writeStore;
        this.storeState = storeState;
        this.spans = spans;
    }

    @Override
    public void open(OpenContext ctx) {
        FeatureSchema schema = FeatureSchema.of(schemaYaml);
        agg = new EntityAggregator(schema, entity);
        schemaVersion = schema.version;
        entityEnum = FeatureAssembler.entityEnum(entity).getNumber();
        ValueStateDescriptor<byte[]> d = new ValueStateDescriptor<>("w-" + entity, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
        long ttlDays = Env.lng("STATE_TTL_DAYS", 8);
        d.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofDays(ttlDays))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1000)
                .build());
        state = getRuntimeContext().getState(d);
        ttlSeconds = ttlDays * 86400;
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
        long key = agg.key(env.row);
        if (key == 0) {
            agg.defaults(env.features);
        } else {
            byte[] next = agg.process(state.value(), env.row, env.filterMask, env.features);
            state.update(next);
            if (writeStore) {
                int[] idx = agg.featureIndices();
                float[] vals = new float[idx.length];
                for (int i = 0; i < idx.length; i++) vals[i] = env.features[idx[i]];
                long asOf = EntityAggregator.lastEventUs(next);
                long w0 = BenchClock.nowUs();
                long ingested = env.ingestUs;
                // history replay (x-backfill) is a bulk load measured on its own stages: live freshness and
                // write latency must describe live events only (object reuse: read the flag now, not in the callback)
                String sfx = env.backfill ? "_backfill" : "";
                StoreBlob blob = storeState
                        ? new StoreBlob(schemaVersion, asOf, w0, vals, env.eventId, next)
                        : new StoreBlob(schemaVersion, asOf, w0, vals);
                byte[] bytes = blob.encode();
                metrics.add("store_write_bytes", bytes.length);
                writer.write(StoreBlob.streamKey(entity, key), bytes, ttlSeconds, err -> {
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
        long end = BenchClock.nowUs();
        if (spans) env.span(Stage.STATE_FEATURES_VALUE, entityEnum, dom, start, end - start);
        metrics.recordUs("state_" + entity, end - start);
        metrics.recordUs("shuffle_" + entity, start - env.lastEmitUs);
        env.lastEmitUs = end;
        out.collect(env);
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
