package io.realy.gateway;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.realy.common.BenchClock;
import io.realy.contracts.bench.v1.BenchTrace;
import io.realy.contracts.bench.v1.Stage;
import io.realy.contracts.scoring.v1.DecisionRecord;
import io.realy.contracts.scoring.v1.IngressMode;
import io.realy.contracts.scoring.v1.ScoreRequest;
import io.realy.contracts.scoring.v1.ScoreResponse;
import io.realy.contracts.scoring.v1.ScoringServiceGrpc;
import io.realy.scoring.ScoringOrchestrator;
import io.realy.scoring.Spans;

/** Synchronous ingress: gRPC unary/bidi (and unframed JSON via Armeria on the same service). */
public final class ScoringServiceImpl extends ScoringServiceGrpc.ScoringServiceImplBase {
    private final Pipeline pipeline;
    private final boolean mirror;

    public ScoringServiceImpl(Pipeline pipeline, boolean mirror) {
        this.pipeline = pipeline;
        this.mirror = mirror;
    }

    private java.util.concurrent.CompletableFuture<DecisionRecord> handle(ScoreRequest req, IngressMode mode) {
        long ingest = BenchClock.nowUs();
        BenchTrace.Builder trace = req.hasTrace() ? req.getTrace().toBuilder() : BenchTrace.newBuilder();
        Spans.point(trace, Stage.INGEST, mode.getNumber(), ingest);
        if (mirror) pipeline.sink().mirror(req.getEvent(), req.hasTrace() ? req.getTrace().toByteArray() : null);
        var r = new ScoringOrchestrator.Request(req.getEvent(), trace, ingest, mode, null, -1,
                req.getDeadlineMs() > 0 ? req.getDeadlineMs() * 1000L : 0, pipeline::publishFinal);
        return pipeline.process(r);
    }

    static ScoreResponse toResponse(DecisionRecord d) {
        BenchTrace.Builder t = d.getTrace().toBuilder();
        Spans.point(t, Stage.RESPONSE, 0, BenchClock.nowUs());
        return ScoreResponse.newBuilder().setEventId(d.getEventId()).setOutcome(d.getOutcome()).setScore(d.getScore())
                .addAllReasonCodes(d.getReasonCodesList()).setPath(d.getPath()).setDegraded(d.getDegraded())
                .setDecidedAtUs(d.getDecidedAtUs()).setTrace(t).build();
    }

    /** Armeria serves this gRPC service as plain HTTP/JSON too (unframed requests): label which rail it was. */
    private static IngressMode unaryMode() {
        var ctx = com.linecorp.armeria.server.ServiceRequestContext.currentOrNull();
        if (ctx == null) return IngressMode.SYNC_GRPC;
        var ct = ctx.request().contentType();
        return ct != null && ct.subtype().contains("json") && !ct.subtype().startsWith("grpc")
                ? IngressMode.SYNC_HTTP : IngressMode.SYNC_GRPC;
    }

    @Override
    public void score(ScoreRequest req, StreamObserver<ScoreResponse> obs) {
        handle(req, unaryMode()).whenComplete((d, err) -> {
            if (err != null) {
                // the detail stays in the gateway's log; the caller gets a generic status with an id to quote
                String ref = Long.toHexString(System.nanoTime());
                org.slf4j.LoggerFactory.getLogger(ScoringServiceImpl.class).error("score failed (ref {}) for event {}",
                        ref, req.getEvent().getEventId(), err);
                obs.onError(Status.INTERNAL.withDescription("scoring failed (ref " + ref + ")").asRuntimeException());
            } else if (d.getReasonCodesList().contains("DEADLINE_EXCEEDED")) {
                obs.onError(Status.DEADLINE_EXCEEDED.withDescription("scoring budget exceeded").asRuntimeException());
            } else {
                obs.onNext(toResponse(d));
                obs.onCompleted();
            }
        });
    }

    @Override
    public StreamObserver<ScoreRequest> scoreStream(StreamObserver<ScoreResponse> out) {
        return new StreamObserver<>() {
            private final java.util.concurrent.atomic.AtomicInteger inflight = new java.util.concurrent.atomic.AtomicInteger();
            private volatile boolean halfClosed;

            @Override
            public void onNext(ScoreRequest req) {
                inflight.incrementAndGet();
                handle(req, IngressMode.SYNC_GRPC).whenComplete((d, err) -> {
                    synchronized (out) {
                        if (err == null) out.onNext(toResponse(d));
                        if (inflight.decrementAndGet() == 0 && halfClosed) out.onCompleted();
                    }
                });
            }

            @Override
            public void onError(Throwable t) {
                halfClosed = true;
            }

            @Override
            public void onCompleted() {
                synchronized (out) {
                    halfClosed = true;
                    if (inflight.get() == 0) out.onCompleted();
                }
            }
        };
    }
}
