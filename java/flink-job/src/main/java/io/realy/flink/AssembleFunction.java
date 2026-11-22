package io.realy.flink;

import io.realy.common.BenchClock;
import io.realy.common.Env;
import io.realy.contracts.bench.v1.Stage;
import io.realy.contracts.flows.v1.TxnState;
import io.realy.scoring.binding.AssemblerCore;
import io.realy.scoring.binding.Assembly;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.rules.Cel;
import io.realy.scoring.rules.Filters;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Assembles multi-message transactions (contracts/transactions/*.yaml) into the unified event, keyed by
 * the pipeline's partition entity (one customer's messages arrive in order, so assembly is deterministic
 * and the Python twin reproduces it). Open transactions live in keyed MapState (TxnState bytes, TTL'd);
 * the wall-clock wait for required parts is a processing-time timer. Unified events pass straight through
 * (after any wait they close). Each assembled transaction is a new envelope timed from its trigger's
 * ingest, with a TXN_ASSEMBLY span for the wait, carrying its TxnContext (CEL event.txn).
 */
public final class AssembleFunction extends KeyedProcessFunction<Long, Envelope, Envelope> {
    private final List<String> contracts;
    private final String schemaYaml;
    private final boolean scoreStages;
    private transient AssemblerCore core;
    private transient MapState<String, byte[]> open;
    private transient FeatureSchema schema;
    private transient Filters filters;
    private transient io.realy.common.StageMetrics metrics;
    private static final Map<String, Cel.Predicate> WHEN = new ConcurrentHashMap<>();

    /**
     * @param scoreStages emit re-scored lifecycle stages (read-only decisions of their own); off: stages
     *                    only move the transaction's context
     */
    public AssembleFunction(List<String> contracts, String schemaYaml, boolean scoreStages) {
        this.contracts = List.copyOf(contracts);
        this.schemaYaml = schemaYaml;
        this.scoreStages = scoreStages;
    }

    @Override
    public void open(OpenContext ctx) {
        schema = FeatureSchema.of(schemaYaml);
        filters = new Filters(schema);
        core = new AssemblerCore(contracts, (expr, view) -> WHEN.computeIfAbsent(expr, Cel::compile).test(Map.of("event", view)));
        MapStateDescriptor<String, byte[]> d = new MapStateDescriptor<>("txn", Types.STRING,
                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
        d.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofDays(Env.lng("TXN_TTL_DAYS", 2)))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1000).build());
        open = getRuntimeContext().getMapState(d);
        store = newStore();
        metrics = FlinkMetrics.get();
    }

    private transient AssemblerCore.Store store;

    private AssemblerCore.Store newStore() {
        return new AssemblerCore.Store() {
        @Override
        public Iterable<String> keys() throws Exception {
            return open.keys();
        }

        @Override
        public TxnState get(String key) throws Exception {
            byte[] b = open.get(key);
            return b == null ? null : TxnState.parseFrom(b);
        }

        @Override
        public void put(String key, TxnState state) throws Exception {
            open.put(key, state.toByteArray());
        }

        @Override
        public void remove(String key) throws Exception {
            open.remove(key);
        }
        };
    }

    @Override
    public void processElement(Envelope env, Context ctx, Collector<Envelope> out) throws Exception {
        long now = BenchClock.nowUs();
        if (env.type == null) {
            emit(core.closeOverdue(store, env.eventTimeUs), env, out);
            out.collect(env);                       // a one-message payment: through, after any wait it closed
            return;
        }
        List<Assembly.Out> outs = core.process(store, env.type, env.event, env.eventTimeUs, env.trace, env.ingestUs, now,
                env.backfill);
        metrics.add("txn_parts", 1);
        emit(outs, env, out);
        // the wall-clock wait of anything still waiting on this key (one timer per deadline; duplicates merge)
        for (String k : open.keys()) {
            TxnState s = TxnState.parseFrom(open.get(k));
            if (!s.getAssembled() && s.getDeadlineWallUs() > 0) {
                ctx.timerService().registerProcessingTimeTimer(s.getDeadlineWallUs() / 1000 + 1);
            }
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Envelope> out) throws Exception {
        emit(core.onTimer(store, timestamp * 1000), null, out);
    }

    private void emit(List<Assembly.Out> outs, Envelope cause, Collector<Envelope> out) {
        for (Assembly.Out o : outs) {
            if ("stage".equals(o.kind()) && !scoreStages) continue;
            long now = BenchClock.nowUs();
            long ingest = o.ingestUs() > 0 ? o.ingestUs() : now;
            byte[] raw = o.event().toByteArray();
            Envelope e = EventDeserializer.envelope(o.event(), raw, o.trace(), false, schema, filters, ingest, o.ctx());
            e.backfill = o.backfill();
            e.readOnly = o.readOnly();
            e.runId = cause != null ? cause.runId : null;
            e.span(Stage.TXN_ASSEMBLY_VALUE, 0, BenchClock.domain(), ingest, now - ingest);
            e.lastEmitUs = now;
            metrics.add(o.ctx().getPartial() ? "txn_assembled_partial" : "txn_assembled", 1);
            metrics.recordUs("txn_assembly_wait", now - ingest);
            out.collect(e);
        }
    }
}
