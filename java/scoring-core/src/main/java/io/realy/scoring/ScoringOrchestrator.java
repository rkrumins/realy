package io.realy.scoring;

import io.realy.common.BenchClock;
import io.realy.common.Delays;
import io.realy.common.Hashing;
import io.realy.common.LatencyModel;
import io.realy.common.SplitMix64;
import io.realy.contracts.bench.v1.BenchTrace;
import io.realy.contracts.bench.v1.OnDeadline;
import io.realy.contracts.bench.v1.Stage;
import io.realy.contracts.features.v1.FeatureVector;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.contracts.scoring.v1.DecisionPath;
import io.realy.contracts.scoring.v1.DecisionRecord;
import io.realy.contracts.scoring.v1.IngressMode;
import io.realy.contracts.scoring.v1.ModelResult;
import io.realy.contracts.scoring.v1.Outcome;
import io.realy.contracts.scoring.v1.RuleHit;
import io.realy.scoring.features.EventView;
import io.realy.scoring.features.FeatureAssembler;
import io.realy.scoring.features.FeatureMap;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.model.ModelClient;
import io.realy.scoring.rules.Filters;
import io.realy.scoring.rules.RuleSet;
import io.realy.scoring.store.OnlineStore;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * The hot path, shared by the gateway (sync gRPC/HTTP and async Kafka ingress) and the Flink inline
 * job: feature lookup -> assemble -> model -> rules -> policy, under an absolute deadline with
 * per-stage deadlines and a configurable fallback. Every stage is timed into the event's BenchTrace.
 */
public final class ScoringOrchestrator {
    /** Fallback reasons (FALLBACK span attr). */
    public static final int FB_FEATURE_TIMEOUT = 1;
    public static final int FB_FEATURE_ERROR = 2;
    public static final int FB_MODEL_TIMEOUT = 3;
    public static final int FB_MODEL_ERROR = 4;

    /**
     * @param txn the context of the multi-message transaction the event was assembled from (CEL
     *            {@code event.txn}, recorded on the decision); null for a one-message payment
     */
    public record Request(TransactionEvent event, BenchTrace.Builder trace, long ingestUs, IngressMode mode,
                          float[] precomputed, long filterMask, long deadlineOverrideUs,
                          Consumer<DecisionRecord> onFinal, io.realy.contracts.flows.v1.TxnContext txn) {
        public Request(TransactionEvent event, BenchTrace.Builder trace, long ingestUs, IngressMode mode,
                       float[] precomputed, long filterMask, long deadlineOverrideUs, Consumer<DecisionRecord> onFinal) {
            this(event, trace, ingestUs, mode, precomputed, filterMask, deadlineOverrideUs, onFinal, null);
        }

        public static Request of(TransactionEvent e, BenchTrace.Builder trace, long ingestUs, IngressMode mode) {
            return new Request(e, trace, ingestUs, mode, null, -1, 0, null, null);
        }
    }

    private final FeatureSchema schema;
    private final FeatureAssembler assembler;
    private final Filters filters;
    private final OnlineStore store;
    private final ModelClient model;
    private final ScoringConfigHolder config;
    private final String instanceId;
    private final long seed;
    private volatile io.realy.common.StageMetrics metrics;

    public ScoringOrchestrator(FeatureSchema schema, OnlineStore store, ModelClient model, ScoringConfigHolder config,
                               String instanceId, long seed) {
        this.schema = schema;
        this.assembler = new FeatureAssembler(schema);
        this.filters = new Filters(schema);
        this.store = store;
        this.model = model;
        this.config = config;
        this.instanceId = instanceId;
        this.seed = seed;
    }

    /** Optional: count online-store bytes read and exact state-blob reads per event (sizing inputs). */
    public ScoringOrchestrator metrics(io.realy.common.StageMetrics m) {
        this.metrics = m;
        return this;
    }

    public FeatureSchema schema() {
        return schema;
    }

    public Filters filters() {
        return filters;
    }

    public ScoringConfigHolder config() {
        return config;
    }

    private static final class Ctx {
        Request r;
        ScoringConfigHolder.Active a;
        Map<String, Object> view;
        long mask;
        long deadlineUs;
        FeatureAssembler.Assembled features;
        int fallbackReason;
    }

    public CompletableFuture<DecisionRecord> score(Request r) {
        Ctx c = new Ctx();
        c.r = r;
        c.a = config.get();
        long start = BenchClock.nowUs();
        if (r.ingestUs() > 0) Spans.add(r.trace(), Stage.QUEUE_WAIT, 0, r.ingestUs(), start - r.ingestUs());
        if (c.a.cfg().getEcho()) {
            return CompletableFuture.completedFuture(build(c, Outcome.APPROVE, Float.NaN, List.of(), null,
                    DecisionPath.PATH_ECHO, false, 0, List.of()));
        }
        long budgetUs = r.deadlineOverrideUs() > 0 ? r.deadlineOverrideUs() : c.a.cfg().getBudgetMs() * 1000L;
        c.deadlineUs = (r.ingestUs() > 0 ? r.ingestUs() : start) + budgetUs;
        c.view = EventView.of(r.event(), io.realy.scoring.binding.TxnContexts.view(r.txn()));
        c.mask = r.filterMask() >= 0 ? r.filterMask() : filters.mask(c.view);

        // ---------------------------------------------------------------- 1. feature lookup
        FeatureAssembler.Lookup lk = assembler.lookupKeys(r.event(), r.precomputed() == null);
        long fStart = BenchClock.nowUs();
        LatencyModel.Sample fd = c.a.featureDelay().sample(SplitMix64.of(seed, r.event().getEventId(), 1));
        CompletableFuture<byte[][]> fetch = switch (fd.kind()) {
            case ERROR -> CompletableFuture.failedFuture(new IllegalStateException("injected feature-store error"));
            case TIMEOUT -> new CompletableFuture<>();
            case OK -> (lk.keys().isEmpty() ? CompletableFuture.completedFuture(new byte[0][]) : store.getAll(lk.keys()))
                    .thenCompose(b -> Delays.after(fd.delayUs()).thenApply(v -> b));
        };
        long featTimeout = Math.min(deadlineStage(c.a.cfg().getFeatureDeadlineMs()), c.deadlineUs - fStart);
        return Delays.withDeadline(fetch, featTimeout).handle((blobs, err) -> {
            long fEnd = BenchClock.nowUs();
            Spans.add(r.trace(), Stage.FEATURE_LOOKUP, lk.keys().size(), fStart, fEnd - fStart);
            if (err != null) {
                c.fallbackReason = isTimeout(err) ? FB_FEATURE_TIMEOUT : FB_FEATURE_ERROR;
                blobs = null;
            }
            c.features = assembler.assemble(r.event(), c.mask, r.precomputed(), lk, blobs, c.a.cfg().getSelfInclusion(), c.view);
            var m = metrics;
            if (m != null) {
                m.add("store_read_bytes", c.features.storeBytes());
                m.add("state_reads", c.features.stateReads());
            }
            Spans.add(r.trace(), Stage.FEATURE_ASSEMBLE, 0, fEnd, BenchClock.nowUs() - fEnd);
            return c;
        }).thenCompose(this::modelStage);
    }

    private static long deadlineStage(int ms) {
        return ms > 0 ? ms * 1000L : Long.MAX_VALUE;
    }

    // -------------------------------------------------------------------- 2. model + rules + policy
    private CompletableFuture<DecisionRecord> modelStage(Ctx c) {
        Request r = c.r;
        if (c.fallbackReason != 0) {
            return CompletableFuture.completedFuture(fallback(c, null));
        }
        long mStart = BenchClock.nowUs();
        long remaining = c.deadlineUs - mStart;
        long modelTimeout = Math.min(deadlineStage(c.a.cfg().getModelDeadlineMs()), remaining);
        LatencyModel.Sample md = c.a.modelDelay().sample(SplitMix64.of(seed, r.event().getEventId(), 2));
        boolean provisional = c.a.cfg().getOnDeadline() == OnDeadline.PROVISIONAL_THEN_ASYNC && r.onFinal() != null;
        long callTimeout = provisional ? Math.max(modelTimeout, 10L * c.a.cfg().getBudgetMs() * 1000L) : modelTimeout;
        CompletableFuture<ModelClient.Result> call = switch (md.kind()) {
            case ERROR -> CompletableFuture.failedFuture(new IllegalStateException("injected model error"));
            case TIMEOUT -> new CompletableFuture<>();
            case OK -> Delays.after(md.delayUs()).thenCompose(v -> model.infer(c.features.values(),
                    Math.max(1, callTimeout - md.delayUs())));
        };
        return Delays.withDeadline(call, Math.max(1, modelTimeout)).handle((res, err) -> {
            long mEnd = BenchClock.nowUs();
            Spans.add(r.trace(), Stage.MODEL_CALL, 0, mStart, mEnd - mStart);
            if (err == null) {
                if (res.clientBatchWaitUs() > 0) Spans.add(r.trace(), Stage.MODEL_BATCH_WAIT, res.batchSize(), mStart, res.clientBatchWaitUs());
                if (res.serverQueueUs() > 0 || res.serverComputeUs() > 0) {
                    Spans.add(r.trace(), Stage.MODEL_SERVER_QUEUE, res.batchSize(), mStart, res.serverQueueUs());
                    Spans.add(r.trace(), Stage.MODEL_SERVER_COMPUTE, res.batchSize(), mStart, res.serverComputeUs());
                }
                return full(c, res);
            }
            c.fallbackReason = isTimeout(err) ? FB_MODEL_TIMEOUT : FB_MODEL_ERROR;
            if (provisional) {
                DecisionRecord prov = fallback(c, DecisionPath.PATH_PROVISIONAL);
                call.whenComplete((late, lateErr) -> {
                    if (lateErr == null) {
                        Ctx c2 = new Ctx();
                        c2.r = c.r; c2.a = c.a; c2.view = c.view; c2.mask = c.mask; c2.features = c.features;
                        DecisionRecord fin = full(c2, late).toBuilder().setRevision(1)
                                .setPath(DecisionPath.PATH_FINAL_AFTER_PROVISIONAL).build();
                        r.onFinal().accept(fin);
                    }
                });
                return prov;
            }
            return fallback(c, null);
        });
    }

    private DecisionRecord full(Ctx c, ModelClient.Result res) {
        long rStart = BenchClock.nowUs();
        RuleSet rules = c.a.rules();
        List<RuleSet.Hit> hits = rules.evaluate(new FeatureMap(schema, c.features.values()), c.view);
        long pStart = BenchClock.nowUs();
        Spans.add(c.r.trace(), Stage.RULES, hits.size(), rStart, pStart - rStart);
        boolean canStepUp = rules.usesChallenge() && rules.challengeable(c.view);
        Outcome o = rules.decide(hits, res.score(), canStepUp);
        Spans.add(c.r.trace(), Stage.POLICY, o.getNumber(), pStart, BenchClock.nowUs() - pStart);
        List<String> reasons = reasons(hits, o, res.score(), rules, 0, canStepUp);
        return build(c, o, res.score(), hits, res, DecisionPath.PATH_FULL, false, 0, reasons);
    }

    private DecisionRecord fallback(Ctx c, DecisionPath pathOverride) {
        long at = BenchClock.nowUs();
        Spans.point(c.r.trace(), Stage.FALLBACK, c.fallbackReason, at);
        RuleSet rules = c.a.rules();
        List<RuleSet.Hit> hits = rules.evaluate(new FeatureMap(schema, c.features.values()), c.view);
        Spans.add(c.r.trace(), Stage.RULES, hits.size(), at, BenchClock.nowUs() - at);
        boolean canStepUp = rules.usesChallenge() && rules.challengeable(c.view);
        Outcome o = rules.decide(hits, Float.NaN, canStepUp);
        boolean timeoutMode = c.a.cfg().getOnDeadline() == OnDeadline.TIMEOUT;
        if (timeoutMode && o == Outcome.APPROVE) o = Outcome.REVIEW;
        List<String> reasons = reasons(hits, o, Float.NaN, rules, c.fallbackReason, canStepUp);
        if (timeoutMode) reasons = append(reasons, "DEADLINE_EXCEEDED");
        DecisionPath path = pathOverride != null ? pathOverride : DecisionPath.PATH_RULES_ONLY_FALLBACK;
        return build(c, o, Float.NaN, hits, null, path, true, 0, reasons);
    }

    private static List<String> append(List<String> l, String s) {
        var out = new java.util.ArrayList<>(l);
        out.add(s);
        return out;
    }

    private static List<String> reasons(List<RuleSet.Hit> hits, Outcome o, float score, RuleSet rules, int fb,
                                        boolean canStepUp) {
        var out = new java.util.ArrayList<String>(hits.size() + 2);
        for (RuleSet.Hit h : hits) out.add(h.id());
        if (!Float.isNaN(score)) {
            if (score >= rules.declineThreshold) out.add("SCORE_DECLINE");
            else if (canStepUp && !Float.isNaN(rules.challengeThreshold) && score >= rules.challengeThreshold) out.add("SCORE_CHALLENGE");
            else if (score >= rules.reviewThreshold) out.add("SCORE_REVIEW");
        }
        switch (fb) {
            case FB_FEATURE_TIMEOUT -> out.add("FEATURE_TIMEOUT");
            case FB_FEATURE_ERROR -> out.add("FEATURE_ERROR");
            case FB_MODEL_TIMEOUT -> out.add("MODEL_TIMEOUT");
            case FB_MODEL_ERROR -> out.add("MODEL_ERROR");
            default -> { }
        }
        return out;
    }

    private DecisionRecord build(Ctx c, Outcome o, float score, List<RuleSet.Hit> hits, ModelClient.Result res,
                                 DecisionPath path, boolean degraded, int revision, List<String> reasons) {
        Request r = c.r;
        long now = BenchClock.nowUs();
        Spans.point(r.trace(), Stage.DECISION, o.getNumber(), now);
        DecisionRecord.Builder b = DecisionRecord.newBuilder()
                .setEventId(r.event().getEventId())
                .setOutcome(o)
                .setScore(Float.isNaN(score) ? 0f : score)
                .addAllReasonCodes(reasons)
                .setPolicyVersion(c.a.cfg().getVersion())
                .setRuleSetVersion(c.a.rules().version)
                .setPath(path)
                .setDegraded(degraded)
                .setIngressMode(r.mode())
                .setInstanceId(instanceId)
                .setDecidedAtUs(now)
                .setRevision(revision)
                .setEvent(r.event())
                .setFeatureSchemaVersion(schema.version);
        if (r.txn() != null) b.setTxn(r.txn());
        for (RuleSet.Hit h : hits) b.addRuleHits(RuleHit.newBuilder().setRuleId(h.id()).setAction(h.action()).setReason(h.reason()));
        if (res != null) {
            b.setModel(ModelResult.newBuilder().setModelName(res.modelName()).setModelVersion(res.modelVersion())
                    .setScore(res.score()).addRawOutputs(res.score())
                    .setInputHash(Hashing.fnv1a64(c.features.values())).setBatchSize(res.batchSize()));
        } else if (c.fallbackReason == FB_MODEL_TIMEOUT || c.fallbackReason == FB_MODEL_ERROR) {
            b.setModel(ModelResult.newBuilder().setTimedOut(c.fallbackReason == FB_MODEL_TIMEOUT)
                    .setErrored(c.fallbackReason == FB_MODEL_ERROR));
        }
        if (c.features != null && sampled(r)) {
            FeatureVector.Builder fv = FeatureVector.newBuilder().setSchemaVersion(schema.version)
                    .setMissingBitmap(ByteString.copyFrom(c.features.missing())).setComputedAtUs(now)
                    .addAllFreshness(c.features.freshness());
            for (float v : c.features.values()) fv.addValues(v);
            b.setFeatures(fv);
        }
        if (r.trace() != null) b.setTrace(r.trace());
        return b.build();
    }

    private boolean sampled(Request r) {
        int permille = config.get().cfg().getFeatureSamplePermille();
        if (r.trace() != null && (r.trace().getFlags() & 1) != 0) return true;
        if (permille >= 1000) return true;
        if (permille <= 0) return false;
        return Long.remainderUnsigned(SplitMix64.mix(r.event().getEventId()), 1000) < permille;
    }

    private static boolean isTimeout(Throwable t) {
        Throwable x = t instanceof CompletionException && t.getCause() != null ? t.getCause() : t;
        if (x instanceof TimeoutException) return true;
        if (x instanceof io.grpc.StatusRuntimeException s) return s.getStatus().getCode() == io.grpc.Status.Code.DEADLINE_EXCEEDED;
        return false;
    }
}
