package io.realy.scoring;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.realy.contracts.bench.v1.ScoringConfig;
import io.realy.contracts.kserve.v2.GRPCInferenceServiceGrpc;
import io.realy.contracts.kserve.v2.ModelInferRequest;
import io.realy.contracts.kserve.v2.ModelInferResponse;
import io.realy.contracts.scoring.v1.Outcome;
import io.realy.scoring.model.KServeModelClient;
import io.realy.scoring.rules.RuleSet;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Model-call hedging against a KServe v2 server whose first call is slow, and live threshold tuning. */
class HedgingTest {
    private Server server;
    private final AtomicInteger calls = new AtomicInteger();
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
    private volatile long slowFirstMs = 400;
    private volatile boolean warming;

    @BeforeEach
    void start() throws Exception {
        server = ServerBuilder.forPort(0).addService(new GRPCInferenceServiceGrpc.GRPCInferenceServiceImplBase() {
            @Override
            public void modelInfer(ModelInferRequest req, StreamObserver<ModelInferResponse> obs) {
                int n = calls.incrementAndGet();
                int b = (int) req.getInputs(0).getShape(0);
                ByteBuffer out = ByteBuffer.allocate(4 * b).order(ByteOrder.LITTLE_ENDIAN);
                for (int i = 0; i < b; i++) out.putFloat(n == 1 ? 0.1f : 0.2f);   // tells which call answered
                out.flip();
                ModelInferResponse resp = ModelInferResponse.newBuilder().setModelName("m").setModelVersion("1")
                        .addRawOutputContents(ByteString.copyFrom(out)).build();
                long delay = warming ? 1 : n == 1 ? slowFirstMs : 5;
                timer.schedule(() -> {
                    obs.onNext(resp);
                    obs.onCompleted();
                }, delay, TimeUnit.MILLISECONDS);
            }
        }).build().start();
    }

    @AfterEach
    void stop() {
        server.shutdownNow();
        timer.shutdownNow();
    }

    private KServeModelClient client(int hedgeMs) throws Exception {
        var c = new KServeModelClient("localhost:" + server.getPort(), "m", 4, 2);
        // connect and JIT both channels first: a cold channel's first call measures setup, not hedging
        warming = true;
        for (int i = 0; i < 6; i++) c.infer(new float[] {1, 2, 3, 4}, 2_000_000).get(2, TimeUnit.SECONDS);
        warming = false;
        calls.set(0);
        c.configure(ScoringConfig.newBuilder().setHedgeAfterMs(hedgeMs).build());
        return c;
    }

    @Test
    void hedgeAnswersWhenTheFirstCallIsSlow() throws Exception {
        try (var c = client(20)) {
            long t0 = System.nanoTime();
            var r = c.infer(new float[] {1, 2, 3, 4}, 2_000_000).get(2, TimeUnit.SECONDS);
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertThat(r.score()).isEqualTo(0.2f);          // the duplicate won
            assertThat(ms).isLessThan(200);
            assertThat(calls.get()).isEqualTo(2);
        }
    }

    @Test
    void noHedgeWaitsForTheSlowCall() throws Exception {
        try (var c = client(0)) {
            long t0 = System.nanoTime();
            var r = c.infer(new float[] {1, 2, 3, 4}, 2_000_000).get(2, TimeUnit.SECONDS);
            assertThat(r.score()).isEqualTo(0.1f);
            assertThat((System.nanoTime() - t0) / 1_000_000).isGreaterThanOrEqualTo(380);
            assertThat(calls.get()).isEqualTo(1);
        }
    }

    @Test
    void fastFirstCallSendsNoDuplicate() throws Exception {
        slowFirstMs = 1;
        try (var c = client(100)) {
            assertThat(c.infer(new float[] {1, 2, 3, 4}, 2_000_000).get(2, TimeUnit.SECONDS).score()).isEqualTo(0.1f);
            Thread.sleep(150);
            assertThat(calls.get()).isEqualTo(1);
        }
    }

    @Test
    void thresholdOverride() {
        var base = RuleSet.bundled("baseline");
        var tuned = base.withThresholds(0.3f, 0.7f);
        assertThat(tuned.decide(List.of(), 0.5f)).isEqualTo(Outcome.REVIEW);
        assertThat(base.decide(List.of(), 0.5f)).isEqualTo(Outcome.APPROVE);
        assertThat(tuned.decide(List.of(), 0.75f)).isEqualTo(Outcome.DECLINE);
        assertThat(base.withThresholds(0, 0)).isSameAs(base);              // 0 = keep the rule set's
        var holder = new ScoringConfigHolder(ScoringConfigHolder.defaults().toBuilder().setReviewThreshold(0.3f).build());
        assertThat(holder.get().rules().reviewThreshold).isEqualTo(0.3f);
        assertThat(holder.get().rules().declineThreshold).isEqualTo(base.declineThreshold);
    }
}
