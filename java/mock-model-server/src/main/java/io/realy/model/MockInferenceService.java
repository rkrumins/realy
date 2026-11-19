package io.realy.model;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.realy.common.BenchClock;
import io.realy.common.Delays;
import io.realy.common.Hashing;
import io.realy.common.LatencyModel;
import io.realy.common.SplitMix64;
import io.realy.common.StageMetrics;
import io.realy.contracts.bench.v1.ModelServerConfig;
import io.realy.contracts.kserve.v2.GRPCInferenceServiceGrpc;
import io.realy.contracts.kserve.v2.InferParameter;
import io.realy.contracts.kserve.v2.ModelInferRequest;
import io.realy.contracts.kserve.v2.ModelInferResponse;
import io.realy.contracts.kserve.v2.ModelMetadataRequest;
import io.realy.contracts.kserve.v2.ModelMetadataResponse;
import io.realy.contracts.kserve.v2.ModelReadyRequest;
import io.realy.contracts.kserve.v2.ModelReadyResponse;
import io.realy.contracts.kserve.v2.ServerLiveRequest;
import io.realy.contracts.kserve.v2.ServerLiveResponse;
import io.realy.contracts.kserve.v2.ServerReadyRequest;
import io.realy.contracts.kserve.v2.ServerReadyResponse;
import io.realy.scoring.model.ProxyModel;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * KServe v2 inference service that behaves like a GPU model server under load: requests queue for
 * one of {@code workers} execution slots, are coalesced by dynamic batching ({@code max_batch},
 * {@code max_queue_delay_us}), and each batch takes {@code service_time + per_item_us * rows}. The
 * score itself is real math (the proxy MLP), so outputs are verifiable. Errors/timeouts/overload are
 * injectable. Without the capacity model a delay-only mock can never saturate — this one does.
 */
public final class MockInferenceService extends GRPCInferenceServiceGrpc.GRPCInferenceServiceImplBase {
    private volatile ProxyModel model;
    private volatile String weightsPath = "";
    private final StageMetrics metrics;
    private final long seed;
    private volatile ModelServerConfig cfg;
    private volatile LatencyModel serviceTime;

    private final Object lock = new Object();
    private final ArrayDeque<Batch> waiting = new ArrayDeque<>();
    private int busy;
    private Batch forming;
    private final AtomicInteger queued = new AtomicInteger();

    private static final class Req {
        final float[] rows;
        final int n;
        final StreamObserver<ModelInferResponse> obs;
        final long enqueuedUs;
        final String outputName;
        final String id;

        Req(float[] rows, int n, StreamObserver<ModelInferResponse> obs, String outputName, String id) {
            this.rows = rows;
            this.n = n;
            this.obs = obs;
            this.outputName = outputName;
            this.id = id;
            this.enqueuedUs = BenchClock.nowUs();
        }
    }

    private static final class Batch {
        final List<Req> reqs = new ArrayList<>();
        int rows;
        long firstUs;
    }

    public MockInferenceService(ProxyModel model, ModelServerConfig cfg, StageMetrics metrics, long seed) {
        this.model = model;
        this.metrics = metrics;
        this.seed = seed;
        configure(cfg);
    }

    /**
     * Apply a (complete) config. A new {@code weights_path} hot-swaps the model: the new weights are
     * loaded first and swapped in atomically, so in-flight batches finish on the old model and a bad
     * file leaves the old model serving (the update is rejected).
     */
    public void configure(ModelServerConfig c) {
        String wp = c.getWeightsPath();
        if (!wp.isEmpty() && !wp.equals(weightsPath)) {
            ProxyModel m;
            try {
                m = ProxyModel.load(wp);
            } catch (java.io.IOException | RuntimeException e) {
                throw new IllegalArgumentException("cannot load model weights " + wp + ": " + e.getMessage(), e);
            }
            if (model != null && m.nFeatures != model.nFeatures) {
                throw new IllegalArgumentException("model " + m.version + " takes " + m.nFeatures + " features, serving "
                        + model.nFeatures);
            }
            model = m;
            weightsPath = wp;
        }
        this.cfg = c.toBuilder().setModelVersion(model.version).setWeightsPath(weightsPath).build();
        this.serviceTime = new LatencyModel(c.getServiceTime());
    }

    public ModelServerConfig config() {
        return cfg;
    }

    @Override
    public void serverLive(ServerLiveRequest r, StreamObserver<ServerLiveResponse> o) {
        o.onNext(ServerLiveResponse.newBuilder().setLive(true).build());
        o.onCompleted();
    }

    @Override
    public void serverReady(ServerReadyRequest r, StreamObserver<ServerReadyResponse> o) {
        o.onNext(ServerReadyResponse.newBuilder().setReady(true).build());
        o.onCompleted();
    }

    @Override
    public void modelReady(ModelReadyRequest r, StreamObserver<ModelReadyResponse> o) {
        o.onNext(ModelReadyResponse.newBuilder().setReady(r.getName().equals(model.name)).build());
        o.onCompleted();
    }

    @Override
    public void modelMetadata(ModelMetadataRequest r, StreamObserver<ModelMetadataResponse> o) {
        o.onNext(ModelMetadataResponse.newBuilder().setName(model.name).addVersions(model.version).setPlatform("realy_mock")
                .addInputs(ModelMetadataResponse.TensorMetadata.newBuilder().setName("input__0").setDatatype("FP32")
                        .addShape(-1).addShape(model.nFeatures))
                .addOutputs(ModelMetadataResponse.TensorMetadata.newBuilder().setName("output__0").setDatatype("FP32")
                        .addShape(-1).addShape(1))
                .build());
        o.onCompleted();
    }

    @Override
    public void modelInfer(ModelInferRequest req, StreamObserver<ModelInferResponse> obs) {
        metrics.inc("requests");
        if (req.getInputsCount() == 0) {
            obs.onError(Status.INVALID_ARGUMENT.withDescription("no inputs").asRuntimeException());
            return;
        }
        var in = req.getInputs(0);
        int n = in.getShapeCount() > 0 ? (int) in.getShape(0) : 1;
        int f = in.getShapeCount() > 1 ? (int) in.getShape(1) : model.nFeatures;
        float[] rows = new float[n * model.nFeatures];
        if (req.getRawInputContentsCount() > 0) {
            ByteBuffer bb = req.getRawInputContents(0).asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
            for (int r = 0; r < n; r++) {
                for (int i = 0; i < f; i++) {
                    float v = bb.getFloat();
                    if (i < model.nFeatures) rows[r * model.nFeatures + i] = v;
                }
            }
        } else {
            var c = in.getContents();
            for (int r = 0; r < n; r++) {
                for (int i = 0; i < f && i < model.nFeatures; i++) rows[r * model.nFeatures + i] = c.getFp32Contents(r * f + i);
            }
        }
        // Deterministic fault injection keyed by the input tensor.
        var s = serviceTime.sample(SplitMix64.of(seed, Hashing.fnv1a64(rows), 3));
        if (s.kind() == LatencyModel.Kind.ERROR) {
            metrics.inc("errors_injected");
            obs.onError(Status.UNAVAILABLE.withDescription("injected model error").asRuntimeException());
            return;
        }
        if (s.kind() == LatencyModel.Kind.TIMEOUT) {
            metrics.inc("timeouts_injected");
            return; // never answer; the client deadline fires
        }
        ModelServerConfig c = cfg;
        if (c.getMaxQueue() > 0 && queued.get() >= c.getMaxQueue()) {
            metrics.inc("rejected");
            obs.onError(Status.RESOURCE_EXHAUSTED.withDescription("queue full").asRuntimeException());
            return;
        }
        String outName = req.getOutputsCount() > 0 ? req.getOutputs(0).getName() : "output__0";
        enqueue(new Req(rows, n, obs, outName, req.getId()));
    }

    private void enqueue(Req r) {
        queued.incrementAndGet();
        ModelServerConfig c = cfg;
        int maxBatch = Math.max(1, c.getMaxBatch());
        Batch ready = null;
        boolean armTimer = false;
        Batch armed = null;
        synchronized (lock) {
            if (maxBatch <= 1 || r.n >= maxBatch) {
                ready = new Batch();
                ready.reqs.add(r);
                ready.rows = r.n;
            } else {
                if (forming == null) {
                    forming = new Batch();
                    forming.firstUs = r.enqueuedUs;
                    armTimer = true;
                    armed = forming;
                }
                forming.reqs.add(r);
                forming.rows += r.n;
                if (forming.rows >= maxBatch) {
                    ready = forming;
                    forming = null;
                    armTimer = false;
                }
            }
        }
        if (ready != null) dispatch(ready);
        if (armTimer) {
            Batch ref = armed;
            Delays.after(Math.max(1, c.getMaxQueueDelayUs())).thenRun(() -> {
                Batch b = null;
                synchronized (lock) {
                    if (forming == ref) {
                        b = forming;
                        forming = null;
                    }
                }
                if (b != null) dispatch(b);
            });
        }
    }

    private void dispatch(Batch b) {
        int workers = cfg.getWorkers();
        synchronized (lock) {
            if (workers > 0 && busy >= workers) {
                waiting.addLast(b);
                return;
            }
            busy++;
        }
        execute(b);
    }

    private void execute(Batch b) {
        long startUs = BenchClock.nowUs();
        var s = serviceTime.sample(SplitMix64.of(seed, startUs ^ b.rows, 4));
        long serviceUs = s.delayUs() + Math.round(cfg.getPerItemUs() * b.rows);
        Delays.after(serviceUs).thenRun(() -> complete(b, startUs, serviceUs));
    }

    private void complete(Batch b, long startUs, long serviceUs) {
        long now = BenchClock.nowUs();
        metrics.recordUs("model_server_service", serviceUs);
        metrics.add("batch_rows", b.rows);
        metrics.inc("batches");
        for (Req r : b.reqs) {
            ByteBuffer out = ByteBuffer.allocate(4 * r.n).order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < r.n; i++) out.putFloat(model.score(r.rows, i * model.nFeatures));
            out.flip();
            long queueUs = startUs - r.enqueuedUs;
            metrics.recordUs("model_server_queue", queueUs);
            metrics.recordUs("model_server_total", now - r.enqueuedUs);
            ModelInferResponse resp = ModelInferResponse.newBuilder()
                    .setModelName(model.name).setModelVersion(model.version).setId(r.id)
                    .addOutputs(ModelInferResponse.InferOutputTensor.newBuilder().setName(r.outputName)
                            .setDatatype("FP32").addShape(r.n).addShape(1))
                    .addRawOutputContents(ByteString.copyFrom(out))
                    .putParameters("queue_us", InferParameter.newBuilder().setInt64Param(queueUs).build())
                    .putParameters("compute_us", InferParameter.newBuilder().setInt64Param(serviceUs).build())
                    .putParameters("batch_size", InferParameter.newBuilder().setInt64Param(b.rows).build())
                    .build();
            try {
                r.obs.onNext(resp);
                r.obs.onCompleted();
            } catch (RuntimeException e) {
                metrics.inc("respond_failed");
            }
            queued.decrementAndGet();
        }
        Batch next;
        synchronized (lock) {
            next = waiting.pollFirst();
            if (next == null) busy--;
        }
        if (next != null) execute(next);
    }
}
