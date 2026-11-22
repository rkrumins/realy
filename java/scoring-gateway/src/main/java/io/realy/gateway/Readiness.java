package io.realy.gateway;

import io.realy.common.Env;
import io.realy.scoring.model.KServeModelClient;
import io.realy.scoring.store.OnlineStore;
import io.realy.scoring.store.RedisOnlineStore;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * GET /ready: whether this gateway can make decisions now. Ready when every enabled ingress loop polled in
 * the last READY_POLL_MS (default 10 s; a dead or wedged loop is not ready), the audit broker is not failing
 * (it acknowledged more recently than it failed), the online store's connections are open and the model
 * server's channel is not failing. /health stays the liveness probe (the process answers). A not-ready
 * gateway is taken out of the load balancer (sync rail) while it recovers; nothing is lost on the async
 * rail, whose offsets wait for durable decisions.
 */
final class Readiness {
    private Readiness() {}

    record Result(boolean ready, Map<String, Object> checks) {
        String json() {
            try {
                return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(
                        Map.of("ready", ready, "checks", checks));
            } catch (Exception e) {
                return "{\"ready\":" + ready + "}";
            }
        }
    }

    static Result check(KafkaIngress ingress, DecisionSink sink, OnlineStore store, KServeModelClient model) {
        Map<String, Object> c = new LinkedHashMap<>();
        boolean ok = true;
        boolean ing = ingress.healthy(Env.lng("READY_POLL_MS", 10_000));
        c.put("ingress", ing ? "polling" : "a consumer has not polled recently (restarts: " + ingress.restarts() + ")");
        ok &= ing;
        boolean audit = !sink.brokerFailing();
        c.put("audit", audit ? "broker acknowledging" : "broker failing (decisions are being spooled)");
        ok &= audit;
        if (store instanceof RedisOnlineStore r) {
            boolean s = r.connected();
            c.put("store", s ? r.mode() + " connected" : "disconnected");
            ok &= s;
        }
        boolean m = model.reachable();
        c.put("model", m ? "channel ok" : "channel failing");
        ok &= m;
        c.put("spool", sink.status());
        return new Result(ok, c);
    }
}
