package io.realy.scoring;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.realy.contracts.kserve.v2.GRPCInferenceServiceGrpc;
import io.realy.contracts.kserve.v2.ModelMetadataRequest;
import io.realy.contracts.kserve.v2.ModelMetadataResponse;
import io.realy.scoring.model.KServeModelClient;
import java.net.ServerSocket;
import org.junit.jupiter.api.Test;

/**
 * The model server was not up when the client started (both restarted together): the client keeps asking
 * for the model's metadata and, once it answers, reports it and adopts the served input width, instead
 * of reporting a stale error and sending the schema width until it is restarted.
 */
class ModelMetadataRetryTest {
    @Test
    void metadataIsReadOnceTheServerComesUp() throws Exception {
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        System.setProperty("MODEL_METADATA_RETRY_MS", "200");
        KServeModelClient c = KServeModelClient.forServedModel("localhost:" + port, "fraud_mlp", 62, 1);
        try {
            assertThat(c.served().error()).isNotEmpty();
            assertThat(c.width()).isEqualTo(62);
            Server server = ServerBuilder.forPort(port).addService(new GRPCInferenceServiceGrpc.GRPCInferenceServiceImplBase() {
                @Override
                public void modelMetadata(ModelMetadataRequest req, StreamObserver<ModelMetadataResponse> obs) {
                    obs.onNext(ModelMetadataResponse.newBuilder().setName(req.getName()).setPlatform("onnxruntime_onnx")
                            .addInputs(ModelMetadataResponse.TensorMetadata.newBuilder().setName("input__0").setDatatype("FP32")
                                    .addShape(-1).addShape(54))
                            .addOutputs(ModelMetadataResponse.TensorMetadata.newBuilder().setName("output__0")).build());
                    obs.onCompleted();
                }
            }).build().start();
            try {
                long end = System.currentTimeMillis() + 15_000;
                while (!c.served().error().isEmpty() && System.currentTimeMillis() < end) Thread.sleep(50);
                assertThat(c.served().error()).isEmpty();
                assertThat(c.served().platform()).isEqualTo("onnxruntime_onnx");
                assertThat(c.width()).isEqualTo(54);     // a positional model now gets exactly its inputs
            } finally {
                server.shutdownNow();
            }
        } finally {
            c.close();
            System.clearProperty("MODEL_METADATA_RETRY_MS");
        }
    }
}
