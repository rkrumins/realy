package io.realy.common;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;

/** Minimal admin endpoint (JDK HttpServer): /health, /metrics, GET|PUT /admin/config. */
public final class AdminHttp {
    private final HttpServer server;

    public AdminHttp(int port, PrometheusMeterRegistry registry, Supplier<String> getConfig,
                     Function<String, String> putConfig) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 64);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/health", ex -> respond(ex, 200, "text/plain", "ok"));
        server.createContext("/metrics", ex -> respond(ex, 200, "text/plain; version=0.0.4", registry.scrape()));
        server.createContext("/admin/config", ex -> {
            try {
                if ("PUT".equals(ex.getRequestMethod()) || "POST".equals(ex.getRequestMethod())) {
                    String refused = Profile.adminWriteRefusal(ex.getRequestHeaders().getFirst("Authorization"));
                    if (refused != null) {
                        respond(ex, 403, "text/plain", refused);
                        return;
                    }
                    String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                    respond(ex, 200, "application/json", putConfig.apply(body));
                } else {
                    respond(ex, 200, "application/json", getConfig.get());
                }
            } catch (IllegalArgumentException e) {
                respond(ex, 400, "text/plain", e.getMessage());
            }
        });
    }

    public AdminHttp start() {
        server.start();
        return this;
    }

    public void stop() {
        server.stop(0);
    }

    private static void respond(HttpExchange ex, int code, String type, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", type);
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }
}
