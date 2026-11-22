package io.realy.gateway;

import io.realy.common.BenchClock;
import io.realy.common.StageMetrics;
import io.realy.contracts.bench.v1.AuditFailure;
import io.realy.contracts.bench.v1.BenchTrace;
import io.realy.contracts.bench.v1.Span;
import io.realy.contracts.bench.v1.Stage;
import io.realy.contracts.scoring.v1.DecisionRecord;
import io.realy.contracts.scoring.v1.Outcome;
import io.realy.scoring.ScoringOrchestrator;
import io.realy.scoring.Spans;
import java.util.concurrent.CompletableFuture;

/** Score -> durable audit write -> (caller replies). Shared by sync and async ingress. */
public final class Pipeline {
    private final ScoringOrchestrator orchestrator;
    private final DecisionSink sink;
    private final StageMetrics metrics;

    public Pipeline(ScoringOrchestrator orchestrator, DecisionSink sink, StageMetrics metrics) {
        this.orchestrator = orchestrator;
        this.sink = sink;
        this.metrics = metrics;
    }

    public ScoringOrchestrator orchestrator() {
        return orchestrator;
    }

    public DecisionSink sink() {
        return sink;
    }

    /**
     * Score, then make the decision durable; completes with the record once it is (on the broker, or fsynced
     * to the spool), with AUDIT_ACK in its trace. Every request gets a decision: a scoring error, thrown or
     * asynchronous, becomes a fail-safe decision (ON_PIPELINE_ERROR, default REVIEW, reason PIPELINE_ERROR)
     * that is made durable like any other, so no event is ever consumed without one.
     */
    public CompletableFuture<DecisionRecord> process(ScoringOrchestrator.Request req) {
        CompletableFuture<DecisionRecord> scored;
        try {
            scored = orchestrator.score(req);
        } catch (RuntimeException e) {
            scored = CompletableFuture.failedFuture(e);
        }
        return scored.exceptionally(err -> errorDecision(req, err)).thenCompose(rec -> {
            long aStart = BenchClock.nowUs();
            return sink.write(rec).handle((ok, err) -> err).thenCompose(err -> {
                if (err == null) return CompletableFuture.completedFuture(finish(rec, req, aStart, false));
                metrics.inc("audit_failures");
                DecisionRecord out = rec;
                if (orchestrator.config().get().cfg().getAuditOnFailure() == AuditFailure.FAIL_CLOSED) {
                    out = rec.toBuilder().setOutcome(Outcome.DECLINE).addReasonCodes("AUDIT_UNAVAILABLE").build();
                }
                DecisionRecord durable = out;
                return sink.persist(durable).thenApply(v -> finish(durable, req, aStart, true));
            });
        });
    }

    private DecisionRecord finish(DecisionRecord out, ScoringOrchestrator.Request req, long aStart, boolean spooled) {
        long aEnd = BenchClock.nowUs();
        metrics.recordUs("audit_ack", aEnd - aStart);
        BenchTrace.Builder t = out.getTrace().toBuilder();
        Spans.add(t, Stage.AUDIT_ACK, spooled ? 1 : 0, aStart, aEnd - aStart);
        record(out, req.ingestUs(), aEnd);
        return out.toBuilder().setTrace(t).build();
    }

    /** The decision for a request whose scoring failed: fail-safe, flagged degraded, with the error's kind. */
    DecisionRecord errorDecision(ScoringOrchestrator.Request req, Throwable err) {
        Throwable cause = err instanceof java.util.concurrent.CompletionException && err.getCause() != null ? err.getCause() : err;
        metrics.inc("pipeline_errors");
        org.slf4j.LoggerFactory.getLogger(Pipeline.class).warn("scoring failed for event {}: {}",
                req.event().getEventId(), cause.toString());
        long now = BenchClock.nowUs();
        BenchTrace.Builder t = req.trace().clone();
        t.addSpans(Span.newBuilder().setStage(Stage.DECISION).setAttr(ON_ERROR.getNumber()).setDomain(BenchClock.domain()).setStartUs(now));
        return DecisionRecord.newBuilder().setEventId(req.event().getEventId()).setOutcome(ON_ERROR)
                .addReasonCodes("PIPELINE_ERROR").addReasonCodes("ERROR_" + cause.getClass().getSimpleName().toUpperCase())
                .setPath(io.realy.contracts.scoring.v1.DecisionPath.PATH_RULES_ONLY_FALLBACK).setDegraded(true)
                .setIngressMode(req.mode()).setDecidedAtUs(now).setEvent(req.event()).setTrace(t).build();
    }

    /** The outcome of a decision whose scoring failed (ON_PIPELINE_ERROR: REVIEW, DECLINE or APPROVE). */
    static final Outcome ON_ERROR = Outcome.valueOf(io.realy.common.Env.str("ON_PIPELINE_ERROR", "REVIEW"));

    /** Async final decision after a provisional answer (on_deadline = PROVISIONAL_THEN_ASYNC). */
    public void publishFinal(DecisionRecord fin) {
        sink.write(fin).exceptionally(e -> {
            sink.persist(fin);
            return null;
        });
        metrics.inc("final_after_provisional");
    }

    private void record(DecisionRecord r, long ingestUs, long nowUs) {
        metrics.inc("decisions");
        metrics.inc("outcome_" + r.getOutcome().name().toLowerCase());
        if (r.getDegraded()) metrics.inc("degraded");
        if (ingestUs > 0) metrics.recordUs("hot_path", nowUs - ingestUs);
        for (Span s : r.getTrace().getSpansList()) {
            if (s.getDurUs() > 0 || s.getStage() == Stage.FEATURE_LOOKUP || s.getStage() == Stage.MODEL_CALL) {
                metrics.recordUs(s.getStage().name().toLowerCase(), s.getDurUs());
            }
        }
    }
}
