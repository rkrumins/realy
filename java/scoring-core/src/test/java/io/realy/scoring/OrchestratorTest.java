package io.realy.scoring;

import static org.assertj.core.api.Assertions.assertThat;

import io.realy.contracts.bench.v1.BenchTrace;
import io.realy.contracts.bench.v1.Fixed;
import io.realy.contracts.bench.v1.LatencyDist;
import io.realy.contracts.bench.v1.Stage;
import io.realy.contracts.payments.v1.PaymentType;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.contracts.scoring.v1.DecisionPath;
import io.realy.contracts.scoring.v1.IngressMode;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.model.ModelClient;
import io.realy.scoring.store.MockOnlineStore;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class OrchestratorTest {
    static TransactionEvent event() {
        return TransactionEvent.newBuilder().setEventId(42).setEventTimeUs(1_800_000_000_000_000L)
                .setPaymentType(PaymentType.CARD_PURCHASE).setCustomerId(1).setCardId(2).setMerchantId(3)
                .setAmountUsdMinor(1234).setMerchantCountry("GB").build();
    }

    static ModelClient constant(float score, long delayUs) {
        return (f, timeout) -> io.realy.common.Delays.after(delayUs)
                .thenApply(v -> new ModelClient.Result(score, "m", "1", 10, 20, 1, 0));
    }

    @Test
    void fullPathRecordsStagesAndFeatures() throws Exception {
        var holder = new ScoringConfigHolder(ScoringConfigHolder.defaults());
        var o = new ScoringOrchestrator(FeatureSchema.load(), new MockOnlineStore(), constant(0.95f, 1000), holder, "t", 1);
        var trace = BenchTrace.newBuilder();
        var d = o.score(ScoringOrchestrator.Request.of(event(), trace, io.realy.common.BenchClock.nowUs(), IngressMode.SYNC_GRPC)).get();
        assertThat(d.getPath()).isEqualTo(DecisionPath.PATH_FULL);
        assertThat(d.getReasonCodesList()).contains("SCORE_DECLINE");
        assertThat(d.getFeatures().getValuesCount()).isEqualTo(FeatureSchema.load().size());
        assertThat(d.getTrace().getSpansList()).extracting(s -> s.getStage())
                .contains(Stage.FEATURE_LOOKUP, Stage.MODEL_CALL, Stage.MODEL_SERVER_QUEUE, Stage.RULES, Stage.DECISION);
    }

    @Test
    void modelDeadlineFallsBackToRulesOnly() throws Exception {
        var cfg = ScoringConfigHolder.defaults().toBuilder().setModelDeadlineMs(20).build();
        var holder = new ScoringConfigHolder(cfg);
        var o = new ScoringOrchestrator(FeatureSchema.load(), new MockOnlineStore(), constant(0.1f, 200_000), holder, "t", 1);
        long t0 = System.nanoTime();
        var d = o.score(ScoringOrchestrator.Request.of(event(), BenchTrace.newBuilder(), io.realy.common.BenchClock.nowUs(), IngressMode.SYNC_GRPC)).get();
        assertThat((System.nanoTime() - t0) / 1e6).isLessThan(150);
        assertThat(d.getPath()).isEqualTo(DecisionPath.PATH_RULES_ONLY_FALLBACK);
        assertThat(d.getDegraded()).isTrue();
        assertThat(d.getReasonCodesList()).contains("MODEL_TIMEOUT");
    }

    @Test
    void provisionalThenAsyncPublishesFinal() throws Exception {
        var cfg = ScoringConfigHolder.defaults().toBuilder().setModelDeadlineMs(10)
                .setOnDeadline(io.realy.contracts.bench.v1.OnDeadline.PROVISIONAL_THEN_ASYNC).build();
        var o = new ScoringOrchestrator(FeatureSchema.load(), new MockOnlineStore(), constant(0.2f, 50_000),
                new ScoringConfigHolder(cfg), "t", 1);
        var fin = new CompletableFuture<io.realy.contracts.scoring.v1.DecisionRecord>();
        var req = new ScoringOrchestrator.Request(event(), BenchTrace.newBuilder(), io.realy.common.BenchClock.nowUs(),
                IngressMode.SYNC_GRPC, null, -1, 0, fin::complete);
        var d = o.score(req).get();
        assertThat(d.getPath()).isEqualTo(DecisionPath.PATH_PROVISIONAL);
        var f = fin.get(2, java.util.concurrent.TimeUnit.SECONDS);
        assertThat(f.getPath()).isEqualTo(DecisionPath.PATH_FINAL_AFTER_PROVISIONAL);
        assertThat(f.getRevision()).isEqualTo(1);
    }

    @Test
    void injectedFeatureDelayIsApplied() throws Exception {
        var cfg = ScoringConfigHolder.defaults().toBuilder()
                .setFeatureLookupDelay(LatencyDist.newBuilder().setFixed(Fixed.newBuilder().setMs(15))).build();
        var o = new ScoringOrchestrator(FeatureSchema.load(), new MockOnlineStore(), constant(0.1f, 0),
                new ScoringConfigHolder(cfg), "t", 1);
        var d = o.score(ScoringOrchestrator.Request.of(event(), BenchTrace.newBuilder(), io.realy.common.BenchClock.nowUs(), IngressMode.SYNC_GRPC)).get();
        var lookup = d.getTrace().getSpansList().stream().filter(s -> s.getStage() == Stage.FEATURE_LOOKUP).findFirst().orElseThrow();
        assertThat(lookup.getDurUs()).isBetween(14_000, 40_000);
    }
}
