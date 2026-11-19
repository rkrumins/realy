package io.realy.scoring.model;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.realy.common.BenchClock;
import io.realy.contracts.kserve.v2.GRPCInferenceServiceGrpc;
import io.realy.contracts.kserve.v2.InferParameter;
import io.realy.contracts.kserve.v2.ModelInferRequest;
import io.realy.contracts.kserve.v2.ModelInferResponse;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * KServe v2 / Triton gRPC client (identical against mock-model-server and real Triton). FP32 input
 * {@code input__0 [B, N]} as raw_input_contents, FP32 output {@code output__0 [B, 1]}. Optional
 * client-side micro-batching coalesces concurrent requests arriving within {@code windowUs}.
 *
 * <p>Hedging ({@code hedge_after_ms > 0}): when a call (a row or a whole micro-batch) has not answered
 * after that long, the identical request is sent once more on the next channel; the first response
 * wins and the other call is cancelled. It trades a bounded amount of extra model load (only the
 * slowest calls are duplicated) for a lower tail, and only helps when slowness is per-request
 * (queueing on one replica, GC, a slow node), not when the whole model tier is saturated.
 */
public final class KServeModelClient implements ModelClient {
    private final ManagedChannel[] channels;
    private final GRPCInferenceServiceGrpc.GRPCInferenceServiceFutureStub[] stubs;
    private final AtomicInteger rr = new AtomicInteger();
    private final String modelName;
    private final int nFeatures;
    private final String inputName;
    private final String outputName;
    private volatile int windowUs;
    private volatile int maxBatch;
    private volatile long hedgeAfterUs;
    private volatile io.realy.common.StageMetrics metrics;
    private final Object lock = new Object();
    private List<Pending> pending = new ArrayList<>();
    private long pendingDeadlineUs = Long.MAX_VALUE;

    private record Pending(float[] row, CompletableFuture<Result> future, long enqueuedUs, long timeoutUs) {}

    public KServeModelClient(String target, String modelName, int nFeatures, int channelCount) {
        this.modelName = modelName;
        this.nFeatures = nFeatures;
        this.inputName = io.realy.common.Env.str("MODEL_INPUT_NAME", "input__0");
        this.outputName = io.realy.common.Env.str("MODEL_OUTPUT_NAME", "output__0");
        channels = new ManagedChannel[Math.max(1, channelCount)];
        stubs = new GRPCInferenceServiceGrpc.GRPCInferenceServiceFutureStub[channels.length];
        for (int i = 0; i < channels.length; i++) {
            channels[i] = ManagedChannelBuilder.forTarget(target).usePlaintext()
                    .maxInboundMessageSize(16 << 20).build();
            stubs[i] = GRPCInferenceServiceGrpc.newFutureStub(channels[i]);
        }
    }

    public void configureBatching(int windowUs, int maxBatch) {
        this.windowUs = Math.max(0, windowUs);
        this.maxBatch = Math.max(1, maxBatch);
    }

    public void configureHedging(int hedgeAfterMs) {
        this.hedgeAfterUs = Math.max(0, hedgeAfterMs) * 1000L;
    }

    /** Micro-batching + hedging from the live ScoringConfig (hot reload). */
    public void configure(io.realy.contracts.bench.v1.ScoringConfig c) {
        configureBatching(c.getMicrobatchWindowUs(), c.getMicrobatchMax());
        configureHedging(c.getHedgeAfterMs());
    }

    /** Counters model_hedges (duplicates sent) and model_hedge_wins (the duplicate answered first). */
    public KServeModelClient metrics(io.realy.common.StageMetrics m) {
        this.metrics = m;
        return this;
    }

    private void count(String counter) {
        var m = metrics;
        if (m != null) m.inc(counter);
    }

    @Override
    public CompletableFuture<Result> infer(float[] row, long timeoutUs) {
        CompletableFuture<Result> f = new CompletableFuture<>();
        if (windowUs <= 0 || maxBatch <= 1) {
            send(List.of(new Pending(row, f, BenchClock.nowUs(), timeoutUs)), timeoutUs);
            return f;
        }
        List<Pending> flush = null;
        long now = BenchClock.nowUs();
        synchronized (lock) {
            boolean first = pending.isEmpty();
            pending.add(new Pending(row, f, now, timeoutUs));
            pendingDeadlineUs = Math.min(pendingDeadlineUs, now + timeoutUs);
            if (pending.size() >= maxBatch) {
                flush = pending;
                pending = new ArrayList<>();
                pendingDeadlineUs = Long.MAX_VALUE;
            } else if (first) {
                List<Pending> batchRef = pending;
                io.realy.common.Delays.after(windowUs).thenRun(() -> flushIfSame(batchRef));
            }
        }
        if (flush != null) send(flush, remaining(flush));
        return f;
    }

    private void flushIfSame(List<Pending> ref) {
        List<Pending> flush = null;
        synchronized (lock) {
            if (pending == ref && !pending.isEmpty()) {
                flush = pending;
                pending = new ArrayList<>();
                pendingDeadlineUs = Long.MAX_VALUE;
            }
        }
        if (flush != null) send(flush, remaining(flush));
    }

    private static long remaining(List<Pending> batch) {
        long now = BenchClock.nowUs();
        long min = Long.MAX_VALUE;
        for (Pending p : batch) min = Math.min(min, p.enqueuedUs() + p.timeoutUs() - now);
        return Math.max(1, min);
    }

    private void send(List<Pending> batch, long timeoutUs) {
        int b = batch.size();
        ByteBuffer buf = ByteBuffer.allocate(4 * b * nFeatures).order(ByteOrder.LITTLE_ENDIAN);
        for (Pending p : batch) {
            for (int i = 0; i < nFeatures; i++) buf.putFloat(i < p.row().length ? p.row()[i] : 0f);
        }
        buf.flip();
        ModelInferRequest req = ModelInferRequest.newBuilder()
                .setModelName(modelName)
                .addInputs(ModelInferRequest.InferInputTensor.newBuilder()
                        .setName(inputName).setDatatype("FP32").addShape(b).addShape(nFeatures))
                .addOutputs(ModelInferRequest.InferRequestedOutputTensor.newBuilder().setName(outputName))
                .addRawInputContents(ByteString.copyFrom(buf))
                .build();
        long sentUs = BenchClock.nowUs();
        int first = Math.floorMod(rr.getAndIncrement(), stubs.length);
        AtomicBoolean done = new AtomicBoolean();
        AtomicInteger outstanding = new AtomicInteger(1);
        AtomicReferenceArray<com.google.common.util.concurrent.ListenableFuture<ModelInferResponse>> calls =
                new AtomicReferenceArray<>(2);
        attempt(req, batch, first, timeoutUs, sentUs, done, outstanding, calls, 0);
        long hedge = hedgeAfterUs;
        if (hedge > 0 && hedge < timeoutUs) {
            io.realy.common.Delays.after(hedge).thenRun(() -> {
                if (done.get()) return;
                long left = timeoutUs - (BenchClock.nowUs() - sentUs);
                if (left <= 1_000) return;              // not worth a duplicate this close to the deadline
                outstanding.incrementAndGet();
                count("model_hedges");
                attempt(req, batch, (first + 1) % stubs.length, left, sentUs, done, outstanding, calls, 1);
            });
        }
    }

    private void attempt(ModelInferRequest req, List<Pending> batch, int channel, long timeoutUs, long sentUs,
                         AtomicBoolean done, AtomicInteger outstanding,
                         AtomicReferenceArray<com.google.common.util.concurrent.ListenableFuture<ModelInferResponse>> calls,
                         int slot) {
        int b = batch.size();
        var stub = stubs[channel].withDeadlineAfter(timeoutUs, TimeUnit.MICROSECONDS);
        var call = stub.modelInfer(req);
        calls.set(slot, call);
        Futures.addCallback(call, new FutureCallback<>() {
            @Override
            public void onSuccess(ModelInferResponse resp) {
                if (!done.compareAndSet(false, true)) return;     // the other attempt answered first
                var other = calls.get(1 - slot);
                if (other != null) other.cancel(true);
                if (slot == 1) count("model_hedge_wins");
                try {
                    float[] scores = decode(resp, b);
                    long q = param(resp, "queue_us");
                    long c = param(resp, "compute_us");
                    int bs = (int) Math.max(param(resp, "batch_size"), b);
                    for (int i = 0; i < b; i++) {
                        Pending p = batch.get(i);
                        p.future().complete(new Result(scores[i], resp.getModelName(), resp.getModelVersion(), q, c, bs,
                                sentUs - p.enqueuedUs()));
                    }
                } catch (RuntimeException e) {
                    for (Pending p : batch) p.future().completeExceptionally(e);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                if (outstanding.decrementAndGet() > 0) return;    // the hedge may still answer
                if (!done.compareAndSet(false, true)) return;
                for (Pending p : batch) p.future().completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor());
    }

    private float[] decode(ModelInferResponse resp, int b) {
        float[] out = new float[b];
        if (resp.getRawOutputContentsCount() > 0) {
            ByteBuffer bb = resp.getRawOutputContents(0).asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < b && bb.remaining() >= 4; i++) out[i] = bb.getFloat();
        } else if (resp.getOutputsCount() > 0) {
            var contents = resp.getOutputs(0).getContents();
            for (int i = 0; i < b && i < contents.getFp32ContentsCount(); i++) out[i] = contents.getFp32Contents(i);
        } else {
            throw new IllegalStateException("model returned no outputs");
        }
        return out;
    }

    private static long param(ModelInferResponse r, String k) {
        InferParameter p = r.getParametersMap().get(k);
        return p == null ? 0 : p.getInt64Param();
    }

    @Override
    public void close() {
        for (ManagedChannel c : channels) c.shutdownNow();
    }
}
