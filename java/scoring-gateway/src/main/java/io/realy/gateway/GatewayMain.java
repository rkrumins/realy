package io.realy.gateway;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.grpc.GrpcService;
import io.realy.common.ControlSubscriber;
import io.realy.common.Env;
import io.realy.common.ProtoJson;
import io.realy.common.StageMetrics;
import io.realy.contracts.bench.v1.ScoringConfig;
import io.realy.scoring.ScoringConfigHolder;
import io.realy.scoring.ScoringOrchestrator;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.model.KServeModelClient;
import io.realy.scoring.store.MockOnlineStore;
import io.realy.scoring.store.OnlineStore;
import io.realy.scoring.store.RedisOnlineStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real-time scoring gateway (stateless; scale horizontally).
 *   :8080  gRPC ScoringService (+ unframed JSON: POST /realy.scoring.v1.ScoringService/Score),
 *          GET /health, GET /metrics, GET|PUT /admin/config
 *   INGRESS_KAFKA=true additionally consumes payments.events.raw (async rail).
 */
public final class GatewayMain {
    private static final Logger LOG = LoggerFactory.getLogger(GatewayMain.class);

    public static void main(String[] args) throws Exception {
        String instance = Env.str("HOSTNAME", "gateway");
        FeatureSchema schema = FeatureSchema.load();
        ScoringConfig cfg = ScoringConfigHolder.defaults();
        String initial = Env.str("SCORING_CONFIG", "");
        if (!initial.isEmpty()) cfg = ProtoJson.overlay(cfg, initial);
        ScoringConfigHolder holder = new ScoringConfigHolder(cfg);

        OnlineStore store = "mock".equals(holder.get().cfg().getFeatureSource())
                ? new MockOnlineStore()
                : new RedisOnlineStore(Env.str("REDIS_URI", "redis://localhost:6379"), Env.integer("REDIS_CONNECTIONS", 4));
        KServeModelClient model = new KServeModelClient(Env.str("MODEL_TARGET", "localhost:8001"),
                Env.str("MODEL_NAME", "fraud_mlp"), Env.integer("MODEL_INPUT_DIM", schema.size()), Env.integer("MODEL_CHANNELS", 4));
        model.configure(holder.get().cfg());
        holder.onChange(a -> model.configure(a.cfg()));

        StageMetrics metrics = new StageMetrics("gateway", instance).startPublishing();
        model.metrics(metrics);
        var orchestrator = new ScoringOrchestrator(schema, store, model, holder, instance, Env.lng("RUN_SEED", 42))
                .metrics(metrics);
        var sink = new DecisionSink("gateway-" + instance);
        var pipeline = new Pipeline(orchestrator, sink, metrics);

        Server server = Server.builder()
                .http(Env.integer("PORT", 8080))
                .maxNumConnections(Env.integer("MAX_CONNECTIONS", 10_000))
                .service(GrpcService.builder()
                        .addService(new ScoringServiceImpl(pipeline, Env.bool("MIRROR_TO_KAFKA", true)))
                        .enableUnframedRequests(true)
                        .useBlockingTaskExecutor(false)
                        .build())
                .service("/health", (ctx, req) -> HttpResponse.of("ok"))
                .service("/metrics", (ctx, req) -> HttpResponse.of(HttpStatus.OK, MediaType.PLAIN_TEXT_UTF_8,
                        metrics.registry().scrape()))
                .service("/admin/config", (ctx, req) -> HttpResponse.of(req.aggregate().thenApply(agg -> {
                    try {
                        String body = agg.contentUtf8();
                        String out = body.isBlank() ? ProtoJson.print(holder.get().cfg()) : holder.update(body);
                        return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, out);
                    } catch (IllegalArgumentException e) {
                        return HttpResponse.of(HttpStatus.BAD_REQUEST, MediaType.PLAIN_TEXT_UTF_8, e.getMessage());
                    }
                })))
                .build();
        server.start().join();

        KafkaIngress ingress = new KafkaIngress(pipeline, metrics).start();
        new ControlSubscriber("gateway", (msg, raw) -> {
            if (msg.hasScoring()) holder.update(raw.get("scoring").toString());
            if (msg.hasIngress()) ingress.setEnabled(msg.getIngress().getKafkaEnabled());
            if (!msg.getRunId().isEmpty()) metrics.setRunId(msg.getRunId());
        });
        LOG.info("gateway {} up on :{} (kafka ingress: {}, store: {}, model: {})", instance, server.activeLocalPort(),
                ingress.enabled(), holder.get().cfg().getFeatureSource(), Env.str("MODEL_TARGET", "localhost:8001"));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            ingress.close();
            server.stop().join();
            sink.close();
        }));
    }
}
