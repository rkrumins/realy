package io.realy.model;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.realy.common.StageMetrics;
import io.realy.contracts.bench.v1.Fixed;
import io.realy.contracts.bench.v1.LatencyDist;
import io.realy.contracts.bench.v1.ModelServerConfig;
import io.realy.scoring.model.ProxyModel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class MockInferenceServiceTest {
    @Test
    void capacityLimitCausesQueueingAndBatchingCoalesces() throws Exception {
        ProxyModel model = ProxyModel.load(null);
        var cfg = ModelServerConfig.newBuilder().setServiceTime(LatencyDist.newBuilder().setFixed(Fixed.newBuilder().setMs(20)))
                .setWorkers(1).setMaxBatch(1).build();
        var svc = new MockInferenceService(model, cfg, new StageMetrics("model", "t"), 1);
        String name = InProcessServerBuilder.generateName();
        var server = InProcessServerBuilder.forName(name).addService(svc).directExecutor().build().start();
        // Use a raw stub over in-process transport.
        var ch = InProcessChannelBuilder.forName(name).directExecutor().build();
        var stub = io.realy.contracts.kserve.v2.GRPCInferenceServiceGrpc.newFutureStub(ch);
        var req = io.realy.contracts.kserve.v2.ModelInferRequest.newBuilder().setModelName(model.name)
                .addInputs(io.realy.contracts.kserve.v2.ModelInferRequest.InferInputTensor.newBuilder().setName("input__0")
                        .setDatatype("FP32").addShape(1).addShape(model.nFeatures))
                .addRawInputContents(com.google.protobuf.ByteString.copyFrom(new byte[4 * model.nFeatures])).build();
        long t0 = System.nanoTime();
        List<com.google.common.util.concurrent.ListenableFuture<io.realy.contracts.kserve.v2.ModelInferResponse>> fs = new ArrayList<>();
        for (int i = 0; i < 5; i++) fs.add(stub.modelInfer(req));
        long maxQueue = 0;
        for (var f : fs) maxQueue = Math.max(maxQueue, f.get().getParametersMap().get("queue_us").getInt64Param());
        double elapsedMs = (System.nanoTime() - t0) / 1e6;
        assertThat(elapsedMs).isGreaterThan(95);           // 5 x 20ms serialized on one worker
        assertThat(maxQueue).isGreaterThan(70_000);          // last one waited for 4 others

        svc.configure(cfg.toBuilder().setWorkers(1).setMaxBatch(8).setMaxQueueDelayUs(5_000).build());
        t0 = System.nanoTime();
        fs.clear();
        for (int i = 0; i < 5; i++) fs.add(stub.modelInfer(req));
        long batch = 0;
        for (var f : fs) batch = Math.max(batch, f.get().getParametersMap().get("batch_size").getInt64Param());
        assertThat(batch).isEqualTo(5);                       // coalesced into one batch
        assertThat((System.nanoTime() - t0) / 1e6).isLessThan(80);
        ch.shutdownNow();
        server.shutdownNow();
    }
}
