package io.realy.flink;

import io.realy.common.BenchClock;
import io.realy.common.Env;
import io.realy.contracts.bench.v1.Stage;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.contracts.scoring.v1.DecisionPath;
import io.realy.contracts.scoring.v1.DecisionRecord;
import io.realy.contracts.scoring.v1.IngressMode;
import io.realy.contracts.scoring.v1.Outcome;
import io.realy.scoring.features.FeatureSchema;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * realy Flink job.
 *
 * <pre>
 *   --mode features   stateful features -> online store (Topology B: gateway scores)
 *   --mode inline     stateful features -> async profile lookup + model -> decisions (Topology A)
 *   --mode both       inline + online-store writes
 *   --mode echo       source -> immediate decision (instrument calibration)
 *   --entities customer,card,...   keyed stages to run (default: all in feature_schema.yaml)
 *   --parallelism N  --max-parallelism 128  --checkpoint-ms 10000  --buffer-timeout-ms 1  --exactly-once false
 *   --tolerable-checkpoint-failures 3  --checkpoint-timeout-ms 120000
 *   --async-capacity 1000  --scoring-config '{...proto3 JSON ScoringConfig...}'
 * </pre>
 */
public final class FraudJob {
    public static void main(String[] args) throws Exception {
        ParameterTool p = ParameterTool.fromArgs(args);
        String mode = p.get("mode", Env.str("JOB_MODE", "features"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configure(env, p);
        String bootstrap = p.get("bootstrap", Env.str("KAFKA_BOOTSTRAP", "localhost:9092"));
        KafkaSource<Envelope> source = KafkaSource.<Envelope>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(p.get("events-topic", "payments.events.raw"))
                .setGroupId(p.get("group", "realy-flink-" + mode))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setDeserializer(new EventDeserializer())
                .setProperty("fetch.max.wait.ms", "100")
                .setProperty("fetch.min.bytes", "1")
                .setProperty("max.poll.records", "2000")
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setProperty("partition.discovery.interval.ms", "60000")
                .setProperties(io.realy.common.KafkaProps.security())
                .build();
        DataStream<Envelope> events = env.fromSource(source, WatermarkStrategy.noWatermarks(), "events.raw")
                .uid("source").name("payments.events.raw");
        DataStream<DecisionBytes> decisions = build(events, mode, p);
        if (decisions != null) {
            decisions.sinkTo(sink(bootstrap, p)).uid("decisions-sink").name("payments.decisions");
        }
        env.execute("realy-" + mode);
    }

    /**
     * Parallelism, checkpointing and fault tolerance. The job states its own guarantees here rather than
     * relying on cluster defaults:
     * <ul>
     *   <li>max parallelism is fixed (128 = Flink's default for small jobs, so existing savepoints keep
     *       restoring): it is the number of key groups, the upper bound for rescaling the keyed state;</li>
     *   <li>a few failed checkpoints are tolerated (one slow checkpoint under a load spike must not fail
     *       the job over and replay minutes of events); persistent failures still fail it;</li>
     *   <li>the latest checkpoints are retained on cancellation, so a job lost with its JobManager can be
     *       restored from them (runner/flink.py restores the newest one when no savepoint is given).</li>
     * </ul>
     */
    static void configure(StreamExecutionEnvironment env, ParameterTool p) {
        env.setParallelism(p.getInt("parallelism", Env.integer("PARALLELISM", 4)));
        env.setMaxParallelism(p.getInt("max-parallelism", Env.integer("MAX_PARALLELISM", 128)));
        env.setBufferTimeout(p.getLong("buffer-timeout-ms", 1));
        long cp = p.getLong("checkpoint-ms", 10_000);
        if (cp > 0) {
            boolean eos = p.getBoolean("exactly-once", false);
            // AT_LEAST_ONCE checkpoints need no barrier alignment -> no checkpoint-induced tail stalls.
            env.enableCheckpointing(cp, eos ? CheckpointingMode.EXACTLY_ONCE : CheckpointingMode.AT_LEAST_ONCE);
            var cc = env.getCheckpointConfig();
            cc.setMinPauseBetweenCheckpoints(cp / 2);
            cc.setCheckpointTimeout(p.getLong("checkpoint-timeout-ms", 120_000));
            cc.setMaxConcurrentCheckpoints(1);
            cc.setTolerableCheckpointFailureNumber(p.getInt("tolerable-checkpoint-failures", 3));
            cc.setExternalizedCheckpointRetention(
                    org.apache.flink.configuration.ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
            if (eos) cc.enableUnalignedCheckpoints();
        }
        env.getConfig().enableObjectReuse();
    }

    /** Builds the processing graph (also used by MiniCluster tests with a bounded source). */
    public static DataStream<DecisionBytes> build(DataStream<Envelope> events, String mode, ParameterTool p) {
        if ("echo".equals(mode)) {
            return events.map(FraudJob::echo).returns(TypeInformation.of(DecisionBytes.class)).name("echo");
        }
        boolean writeStore = p.getBoolean("write-store", "features".equals(mode) || "both".equals(mode));
        List<String> entities = new ArrayList<>(FeatureSchema.load().streamByEntity().keySet());
        String only = p.get("entities", "");
        if (!only.isBlank()) entities.retainAll(Arrays.asList(only.split(",")));
        DataStream<Envelope> s = events;
        if ("inline".equals(mode) || "both".equals(mode)) {
            s = s.filter(e -> !e.mirrored).name("skip-mirrored");
        }
        // entities whose online-store blob carries the aggregator state (exact re-windowing at read time);
        // the rest store values only. Customer/card: low per-entity rates -> small blobs, must be exact.
        List<String> stateEntities = Arrays.stream(p.get("store-state", Env.str("STORE_STATE", "customer,card")).split(","))
                .map(String::trim).filter(x -> !x.isEmpty()).toList();
        // spans travel with the envelope only when a decision will carry them (inline / both)
        s = features(s, entities, writeStore, stateEntities, !"features".equals(mode));
        if (!"features".equals(mode)) s = s.filter(e -> !e.backfill).name("skip-backfill");
        if ("features".equals(mode)) {
            s.addSink(new org.apache.flink.streaming.api.functions.sink.legacy.DiscardingSink<>()).name("discard");
            return null;
        }
        return AsyncDataStream.unorderedWait(s, new ScoringAsyncFunction(p.get("scoring-config", Env.str("SCORING_CONFIG", ""))),
                        p.getLong("async-timeout-ms", 1000), TimeUnit.MILLISECONDS, p.getInt("async-capacity", 1000))
                .uid("scoring").name("score (async I/O)");
    }

    /** Sequential keyed stages, one per entity (each keyBy = one network shuffle, measured). */
    public static DataStream<Envelope> features(DataStream<Envelope> in, List<String> entities, boolean writeStore) {
        return features(in, entities, writeStore, List.of());
    }

    public static DataStream<Envelope> features(DataStream<Envelope> in, List<String> entities, boolean writeStore,
                                                List<String> stateEntities) {
        return features(in, entities, writeStore, stateEntities, true);
    }

    public static DataStream<Envelope> features(DataStream<Envelope> in, List<String> entities, boolean writeStore,
                                                List<String> stateEntities, boolean spans) {
        DataStream<Envelope> s = in;
        for (String entity : entities) {
            s = s.keyBy(new EntityKey(entity))
                    .process(new EntityFeatureFunction(entity, writeStore, stateEntities.contains(entity), spans))
                    .uid("features-" + entity).name("features:" + entity);
        }
        return s;
    }

    /** Entity key; events lacking the entity are spread by event id (no hot zero key). */
    public static final class EntityKey implements KeySelector<Envelope, Long> {
        private final String entity;

        public EntityKey(String entity) {
            this.entity = entity;
        }

        @Override
        public Long getKey(Envelope e) {
            long k = e.key(entity);
            return k != 0 ? k : -1L - (e.eventId & 0xffff);
        }
    }

    static DecisionBytes echo(Envelope env) throws Exception {
        TransactionEvent e = TransactionEvent.parseFrom(env.event);
        long now = BenchClock.nowUs();
        env.span(Stage.DECISION_VALUE, Outcome.APPROVE_VALUE, BenchClock.domain(), now, 0);
        DecisionRecord r = DecisionRecord.newBuilder().setEventId(e.getEventId()).setOutcome(Outcome.APPROVE)
                .setPath(DecisionPath.PATH_ECHO).setIngressMode(IngressMode.ASYNC_FLINK_INLINE).setDecidedAtUs(now)
                .setEvent(e).setTrace(ScoringAsyncFunction.trace(env)).build();
        return new DecisionBytes(ScoringAsyncFunction.key(e.getCustomerId()), r.toByteArray());
    }

    static KafkaSink<DecisionBytes> sink(String bootstrap, ParameterTool p) {
        Properties props = new Properties();
        for (var kv : Map.of("linger.ms", "1", "batch.size", "131072", "compression.type", "lz4", "acks", "all",
                "enable.idempotence", "true", "max.in.flight.requests.per.connection", "5",
                "transaction.timeout.ms", "600000").entrySet()) {
            props.setProperty(kv.getKey(), kv.getValue());
        }
        props.putAll(io.realy.common.KafkaProps.security());
        String topic = p.get("decisions-topic", "payments.decisions");
        boolean eos = p.getBoolean("exactly-once", false);
        var b = KafkaSink.<DecisionBytes>builder()
                .setBootstrapServers(bootstrap)
                .setKafkaProducerConfig(props)
                .setRecordSerializer((KafkaRecordSerializationSchema<DecisionBytes>) (d, ctx, ts) ->
                        new ProducerRecord<>(topic, d.key, d.value))
                .setDeliveryGuarantee(eos ? DeliveryGuarantee.EXACTLY_ONCE : DeliveryGuarantee.AT_LEAST_ONCE);
        if (eos) b.setTransactionalIdPrefix("realy-decisions");
        return b.build();
    }

    static Configuration unused() {
        return new Configuration();
    }

    static Duration d(long ms) {
        return Duration.ofMillis(ms);
    }
}
