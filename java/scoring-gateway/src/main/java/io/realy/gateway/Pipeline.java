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

    /** Completes with the record once it is durable (or spooled); the trace includes AUDIT_ACK. */
    public CompletableFuture<DecisionRecord> process(ScoringOrchestrator.Request req) {
        return orchestrator.score(req).thenCompose(rec -> {
            long aStart = BenchClock.nowUs();
            return sink.write(rec).handle((ok, err) -> {
                long aEnd = BenchClock.nowUs();
                metrics.recordUs("audit_ack", aEnd - aStart);
                DecisionRecord out = rec;
                if (err != null) {
                    metrics.inc("audit_failures");
                    if (orchestrator.config().get().cfg().getAuditOnFailure() == AuditFailure.FAIL_CLOSED) {
                        out = rec.toBuilder().setOutcome(Outcome.DECLINE).addReasonCodes("AUDIT_UNAVAILABLE").build();
                    }
                    sink.spool(out);
                }
                BenchTrace.Builder t = out.getTrace().toBuilder();
                Spans.add(t, Stage.AUDIT_ACK, err == null ? 0 : 1, aStart, aEnd - aStart);
                record(out, req.ingestUs(), aEnd);
                return out.toBuilder().setTrace(t).build();
            });
        });
    }

    /** Async final decision after a provisional answer (on_deadline = PROVISIONAL_THEN_ASYNC). */
    public void publishFinal(DecisionRecord fin) {
        sink.write(fin).exceptionally(e -> {
            sink.spool(fin);
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
