package io.realy.flink;

import io.realy.common.BenchClock;
import io.realy.common.ControlSubscriber;
import io.realy.common.Env;
import io.realy.common.ProtoJson;
import io.realy.common.StageMetrics;
import io.realy.contracts.bench.v1.BenchTrace;
import io.realy.contracts.bench.v1.ScoringConfig;
import io.realy.contracts.bench.v1.Span;
import io.realy.contracts.bench.v1.Stage;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.contracts.scoring.v1.DecisionPath;
import io.realy.contracts.scoring.v1.DecisionRecord;
import io.realy.contracts.scoring.v1.IngressMode;
import io.realy.contracts.scoring.v1.Outcome;
import io.realy.scoring.ScoringConfigHolder;
import io.realy.scoring.ScoringOrchestrator;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.model.KServeModelClient;
import io.realy.scoring.store.MockOnlineStore;
import io.realy.scoring.store.OnlineStore;
import io.realy.scoring.store.RedisOnlineStore;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

/**
 * Inline scoring inside Flink (Topology A): stream features come from the upstream keyed stages;
 * profile features are fetched from the online store; then model + rules + policy via the shared
 * ScoringOrchestrator. Live config arrives per subtask from bench.control. An async timeout never
 * fails the job — it yields a degraded REVIEW decision.
 */
public final class ScoringAsyncFunction extends RichAsyncFunction<Envelope, DecisionBytes> {
    private final String initialConfigJson;
    private final String schemaYaml;
    private transient ScoringOrchestrator orchestrator;
    private transient KServeModelClient model;
    private transient OnlineStore store;
    private transient ControlSubscriber control;
    private transient StageMetrics metrics;
    private transient String instance;

    public ScoringAsyncFunction(String initialConfigJson) {
        this(initialConfigJson, null);
    }

    public ScoringAsyncFunction(String initialConfigJson, String schemaYaml) {
        this.schemaYaml = schemaYaml;
        this.initialConfigJson = initialConfigJson;
    }

    @Override
    public void open(OpenContext ctx) {
        FeatureSchema schema = FeatureSchema.of(schemaYaml);
        ScoringConfig cfg = ScoringConfigHolder.defaults();
        if (initialConfigJson != null && !initialConfigJson.isBlank()) cfg = ProtoJson.overlay(cfg, initialConfigJson);
        ScoringConfigHolder holder = new ScoringConfigHolder(cfg);
        store = "mock".equals(holder.get().cfg().getFeatureSource())
                ? new MockOnlineStore() : RedisOnlineStore.shared(Env.str("REDIS_URI", "redis://localhost:6379"), 4);
        model = KServeModelClient.forServedModel(Env.str("MODEL_TARGET", "localhost:8001"), Env.str("MODEL_NAME", "fraud_mlp"),
                schema.size(), 2);
        model.configure(holder.get().cfg());
        holder.onChange(a -> model.configure(a.cfg()));
        instance = Env.str("HOSTNAME", "flink") + "/" + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        orchestrator = new ScoringOrchestrator(schema, store, model, holder, instance, Env.lng("RUN_SEED", 42));
        metrics = FlinkMetrics.get();
        control = new ControlSubscriber("flink", (msg, raw) -> {
            if (msg.hasScoring()) holder.update(raw.get("scoring").toString());
            if (!msg.getRunId().isEmpty()) metrics.setRunId(msg.getRunId());
        });
    }

    static BenchTrace.Builder trace(Envelope env) throws Exception {
        BenchTrace.Builder t = env.trace != null ? BenchTrace.parseFrom(env.trace).toBuilder() : BenchTrace.newBuilder();
        for (int i = 0; i + 2 < env.spans.length; i += 3) {
            long h = env.spans[i];
            t.addSpans(Span.newBuilder().setStage(Stage.forNumber((int) (h >>> 40)))
                    .setAttr((int) ((h >>> 32) & 0xff)).setDomain((int) h)
                    .setStartUs(env.spans[i + 1]).setDurUs((int) env.spans[i + 2]));
        }
        return t;
    }

    static byte[] key(long customerId) {
        return Long.toUnsignedString(customerId).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void asyncInvoke(Envelope env, ResultFuture<DecisionBytes> result) throws Exception {
        long start = BenchClock.nowUs();
        env.span(Stage.ASYNC_WAIT_VALUE, 0, BenchClock.domain(), env.lastEmitUs, start - env.lastEmitUs);
        TransactionEvent e = TransactionEvent.parseFrom(env.event);
        BenchTrace.Builder t = trace(env);
        long budgetUs = orchestrator.config().get().cfg().getBudgetMs() * 1000L;
        long remaining = Math.max(1000, budgetUs - (start - env.ingestUs));
        var txn = env.txn == null ? null : io.realy.contracts.flows.v1.TxnContext.parseFrom(env.txn);
        var req = new ScoringOrchestrator.Request(e, t, 0, IngressMode.ASYNC_FLINK_INLINE, env.features, env.filterMask,
                remaining, null, txn);
        orchestrator.score(req).whenComplete((rec, err) -> {
            DecisionRecord out = err == null ? rec : errorRecord(e, t, "PIPELINE_ERROR");
            metrics.inc("decisions");
            metrics.recordUs("hot_path", BenchClock.nowUs() - env.ingestUs);
            result.complete(List.of(new DecisionBytes(key(e.getCustomerId()), out.toByteArray())));
        });
    }

    @Override
    public void timeout(Envelope env, ResultFuture<DecisionBytes> result) throws Exception {
        TransactionEvent e = TransactionEvent.parseFrom(env.event);
        metrics.inc("async_timeouts");
        result.complete(List.of(new DecisionBytes(key(e.getCustomerId()), errorRecord(e, trace(env), "ASYNC_TIMEOUT").toByteArray())));
    }

    private DecisionRecord errorRecord(TransactionEvent e, BenchTrace.Builder t, String reason) {
        long now = BenchClock.nowUs();
        t.addSpans(Span.newBuilder().setStage(Stage.DECISION).setAttr(Outcome.REVIEW_VALUE).setDomain(BenchClock.domain()).setStartUs(now));
        return DecisionRecord.newBuilder().setEventId(e.getEventId()).setOutcome(Outcome.REVIEW).addReasonCodes(reason)
                .setPath(DecisionPath.PATH_RULES_ONLY_FALLBACK).setDegraded(true).setIngressMode(IngressMode.ASYNC_FLINK_INLINE)
                .setInstanceId(instance).setDecidedAtUs(now).setEvent(e).setTrace(t).build();
    }

    @Override
    public void close() {
        if (control != null) control.close();
        if (model != null) model.close();
        if (store != null) store.close();
    }
}
