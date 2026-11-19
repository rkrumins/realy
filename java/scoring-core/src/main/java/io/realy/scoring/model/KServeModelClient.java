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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * KServe v2 / Triton gRPC client (identical against mock-model-server and real Triton). FP32 input
 * {@code input__0 [B, N]} as raw_input_contents, FP32 output {@code output__0 [B, 1]}. Optional
 * client-side micro-batching coalesces concurrent requests arriving within {@code windowUs}.
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
        var stub = stubs[Math.floorMod(rr.getAndIncrement(), stubs.length)].withDeadlineAfter(timeoutUs, TimeUnit.MICROSECONDS);
        Futures.addCallback(stub.modelInfer(req), new FutureCallback<>() {
            @Override
            public void onSuccess(ModelInferResponse resp) {
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
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable t) {
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
