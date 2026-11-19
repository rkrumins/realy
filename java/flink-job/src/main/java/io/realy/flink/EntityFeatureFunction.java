package io.realy.flink;

import io.realy.common.BenchClock;
import io.realy.common.Env;
import io.realy.common.StageMetrics;
import io.realy.contracts.bench.v1.Stage;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.scoring.features.EntityAggregator;
import io.realy.scoring.features.FeatureAssembler;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.store.RedisOnlineStore;
import io.realy.scoring.store.StoreBlob;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.functions.OpenContext;
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
 */
public final class EntityFeatureFunction extends KeyedProcessFunction<Long, Envelope, Envelope> {
    private final String entity;
    private final boolean writeStore;
    private final boolean storeState;
    private transient EntityAggregator agg;
    private transient ValueState<byte[]> state;
    private transient RedisOnlineStore store;
    private transient int entityEnum;
    private transient int schemaVersion;
    private transient StageMetrics metrics;
    private transient long ttlSeconds;

    public EntityFeatureFunction(String entity, boolean writeStore) {
        this(entity, writeStore, false);
    }

    public EntityFeatureFunction(String entity, boolean writeStore, boolean storeState) {
        this.entity = entity;
        this.writeStore = writeStore;
        this.storeState = storeState;
    }

    @Override
    public void open(OpenContext ctx) {
        FeatureSchema schema = FeatureSchema.load();
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
        if (writeStore) store = RedisOnlineStore.shared(Env.str("REDIS_URI", "redis://localhost:6379"), 4);
        metrics = FlinkMetrics.get();
    }

    @Override
    public void processElement(Envelope env, Context ctx, Collector<Envelope> out) throws Exception {
        long start = BenchClock.nowUs();
        int dom = BenchClock.domain();
        env.span(Stage.SHUFFLE_VALUE, entityEnum, dom, env.lastEmitUs, start - env.lastEmitUs);
        long key = env.key(entity);
        if (key == 0) {
            agg.defaults(env.features);
        } else {
            TransactionEvent e = TransactionEvent.parseFrom(env.event);
            byte[] next = agg.process(state.value(), e, env.filterMask, env.features);
            state.update(next);
            if (writeStore) {
                int[] idx = agg.featureIndices();
                float[] vals = new float[idx.length];
                for (int i = 0; i < idx.length; i++) vals[i] = env.features[idx[i]];
                long asOf = EntityAggregator.lastEventUs(next);
                long w0 = BenchClock.nowUs();
                long ingested = env.ingestUs;
                StoreBlob blob = storeState
                        ? new StoreBlob(schemaVersion, asOf, w0, vals, e.getEventId(), next)
                        : new StoreBlob(schemaVersion, asOf, w0, vals);
                byte[] bytes = blob.encode();
                metrics.add("store_write_bytes", bytes.length);
                store.setAll(List.of(StoreBlob.streamKey(entity, key)), List.of(bytes), ttlSeconds)
                        .whenComplete((v, err) -> {
                            long done = BenchClock.nowUs();
                            metrics.recordUs("feature_store_write", done - w0);
                            // Freshness of the online store: how long a new event stays invisible to the
                            // gateway (Kafka pickup by Flink -> entity blob durable in Redis).
                            metrics.recordUs("store_update_lag", done - ingested);
                        });
            }
        }
        long end = BenchClock.nowUs();
        env.span(Stage.STATE_FEATURES_VALUE, entityEnum, dom, start, end - start);
        metrics.recordUs("state_" + entity, end - start);
        metrics.recordUs("shuffle_" + entity, start - env.lastEmitUs);
        env.lastEmitUs = end;
        out.collect(env);
    }

    @Override
    public void close() {
        if (store != null) store.close();
    }
}
