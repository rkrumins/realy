package io.realy.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.realy.common.Env;
import io.realy.contracts.bench.v1.ScoringConfig;
import io.realy.scoring.binding.PipelineBinding;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.model.KServeModelClient;
import io.realy.scoring.model.ModelChannels;
import io.realy.scoring.store.MockOnlineStore;
import io.realy.scoring.store.OnlineStore;
import io.realy.scoring.store.RedisOnlineStore;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * GET /admin/info: what this gateway is connected to, as it sees it (its own network, its own settings):
 * the pipeline and feature schema it runs, Kafka (endpoint, security, the topic it consumes), the online
 * store (endpoint, standalone or cluster, server version), the model server (target, TLS, the served
 * model's platform, version and input width), the audit sink, and every simulated setting in effect
 * (injected delays, echo, the mock store). The preflight (python/realy/infra.py) compares it with the
 * scenario. No credentials: endpoints are reported without user info, tokens never.
 */
final class AdminInfo {
    private static final ObjectMapper OM = new ObjectMapper();

    private AdminInfo() {}

    static String json(String instance, FeatureSchema schema, OnlineStore store, KServeModelClient model,
                       KafkaIngress ingress, ScoringConfig cfg) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("service", "gateway");
        out.put("instance", instance);
        String v = AdminInfo.class.getPackage().getImplementationVersion();
        out.put("version", v != null ? v : Env.str("REALY_VERSION", "dev"));
        out.put("pipeline", PipelineBinding.current().name);
        out.put("schema", Map.of("version", schema.version, "features", schema.size(),
                "sha256", sha256(FeatureSchema.loadText())));
        Map<String, Object> ext = new LinkedHashMap<>();
        ext.put("aggregations", io.realy.scoring.features.Aggregations.extensions());
        ext.put("plugins_dir", Env.str("PLUGINS_DIR", ""));
        ext.put("jars", io.realy.scoring.Plugins.jars().stream().map(x -> x.getFileName().toString()).toList());
        out.put("extensions", ext);
        Map<String, Object> kafka = new LinkedHashMap<>();
        kafka.put("bootstrap", Env.str("KAFKA_BOOTSTRAP", "localhost:9092"));
        kafka.put("security_protocol", Env.str("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"));
        kafka.put("sasl_mechanism", Env.str("KAFKA_SASL_MECHANISM", ""));
        kafka.put("ingress_enabled", ingress.enabled());
        kafka.put("ingress_topic", ingress.topic());
        kafka.put("consumers", Env.integer("KAFKA_CONSUMERS", 2));
        kafka.put("group", Env.str("GATEWAY_GROUP", "realy-gateway"));
        out.put("kafka", kafka);
        Map<String, Object> redis = new LinkedHashMap<>();
        if (store instanceof RedisOnlineStore r) {
            String uri = Env.str("REDIS_URI", "redis://localhost:6379");
            redis.put("endpoint", redact(uri));
            redis.put("tls", uri.startsWith("rediss://"));
            redis.put("mode", r.mode());
            redis.put("server_version", r.serverVersion());
        } else {
            redis.put("mode", store instanceof MockOnlineStore ? "mock" : store.getClass().getSimpleName());
        }
        out.put("redis", redis);
        Map<String, Object> m = new LinkedHashMap<>();
        var sv = model.served();
        m.put("target", model.target());
        m.put("name", Env.str("MODEL_NAME", "fraud_mlp"));
        m.put("tls", ModelChannels.tls());
        m.put("auth_header", !Env.str("MODEL_AUTH_HEADER", "").isBlank());
        m.put("platform", sv.platform());
        m.put("mock", sv.mock());
        m.put("versions", sv.versions());
        m.put("input", model.inputName());
        m.put("served_input_width", sv.inputWidth());
        m.put("request_width", model.width());
        m.put("schema_width", schema.size());
        m.put("output", sv.outputName());
        m.put("metadata_error", sv.error());
        out.put("model", m);
        out.put("audit", Map.of("decisions_topic", Env.str("DECISIONS_TOPIC", "payments.decisions"),
                "on_failure", cfg.getAuditOnFailure().name(), "spool", Env.str("AUDIT_SPOOL", "/tmp/realy-audit.spool")));
        Map<String, Object> sim = new LinkedHashMap<>();
        sim.put("echo", cfg.getEcho());
        sim.put("feature_source", cfg.getFeatureSource().isEmpty() ? "redis" : cfg.getFeatureSource());
        sim.put("feature_lookup_delay", cfg.hasFeatureLookupDelay());
        sim.put("model_extra_delay", cfg.hasModelExtraDelay());
        out.put("simulated", sim);
        try {
            return OM.writeValueAsString(out);
        } catch (Exception e) {
            return "{\"error\":\"" + e.getMessage() + "\"}";
        }
    }

    /** host:port and scheme only (no user, password or query). */
    static String redact(String uri) {
        try {
            URI u = URI.create(uri);
            return u.getScheme() + "://" + u.getHost() + (u.getPort() > 0 ? ":" + u.getPort() : "");
        } catch (RuntimeException e) {
            return "(unparseable)";
        }
    }

    static String sha256(String s) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder b = new StringBuilder();
            for (int i = 0; i < 6; i++) b.append(String.format("%02x", d[i]));
            return b.toString();
        } catch (Exception e) {
            return "";
        }
    }
}
