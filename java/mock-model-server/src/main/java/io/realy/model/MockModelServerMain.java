package io.realy.model;

import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.realy.common.AdminHttp;
import io.realy.common.ControlSubscriber;
import io.realy.common.Env;
import io.realy.common.ProtoJson;
import io.realy.common.StageMetrics;
import io.realy.contracts.bench.v1.LatencyDist;
import io.realy.contracts.bench.v1.ModelServerConfig;
import io.realy.contracts.bench.v1.Uniform;
import io.realy.scoring.model.ProxyModel;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Entry point: KServe v2 gRPC on GRPC_PORT (8001), admin/metrics on ADMIN_PORT (8002). */
public final class MockModelServerMain {
    private static final Logger LOG = LoggerFactory.getLogger(MockModelServerMain.class);

    public static void main(String[] args) throws Exception {
        ProxyModel model = ProxyModel.load(Env.str("MODEL_WEIGHTS", ""));
        ModelServerConfig.Builder cfg = ModelServerConfig.newBuilder()
                .setServiceTime(LatencyDist.newBuilder().setUniform(Uniform.newBuilder().setMinMs(30).setMaxMs(100)))
                .setWorkers(0).setMaxBatch(1).setModelVersion(model.version);
        String initial = Env.str("MODEL_SERVER_CONFIG", "");
        if (!initial.isEmpty()) ProtoJson.mergeJson(initial, cfg);
        String instance = Env.str("HOSTNAME", "mock-model");
        StageMetrics metrics = new StageMetrics("model", instance).startPublishing();
        MockInferenceService svc = new MockInferenceService(model, cfg.build(), metrics, Env.lng("RUN_SEED", 42));

        int port = Env.integer("GRPC_PORT", 8001);
        var server = NettyServerBuilder.forAddress(new InetSocketAddress(port))
                .addService(svc)
                .executor(Executors.newFixedThreadPool(Math.max(2, Runtime.getRuntime().availableProcessors())))
                .maxInboundMessageSize(16 << 20)
                .build().start();
        new AdminHttp(Env.integer("ADMIN_PORT", 8002), metrics.registry(),
                () -> ProtoJson.print(svc.config()),
                body -> {
                    ModelServerConfig.Builder b = svc.config().toBuilder();
                    ProtoJson.mergeJson(body, b);
                    svc.configure(b.build());
                    LOG.info("model server config updated: {}", ProtoJson.print(b));
                    return ProtoJson.print(b);
                }).start();
        new ControlSubscriber("model", msg -> {
            if (msg.hasModelServer()) {
                svc.configure(svc.config().toBuilder().mergeFrom(msg.getModelServer()).build());
                LOG.info("model server config from control topic v{}", msg.getVersion());
            }
            if (!msg.getRunId().isEmpty()) metrics.setRunId(msg.getRunId());
        });
        LOG.info("mock model server '{}' v{} ({} features) on :{} config {}", model.name, model.version, model.nFeatures, port,
                ProtoJson.print(svc.config()));
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
        server.awaitTermination();
    }
}
