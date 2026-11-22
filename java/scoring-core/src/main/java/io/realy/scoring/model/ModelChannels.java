package io.realy.scoring.model;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ChannelCredentials;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.TlsChannelCredentials;
import io.realy.common.Env;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * gRPC channels to the model server, plaintext (the bundled mock, an in-cluster Triton) or secured (a
 * managed endpoint, KServe behind an ingress gateway):
 *
 * <pre>
 *   MODEL_TLS=true                  TLS with the JVM's trust store
 *   MODEL_CA_FILE=/path/ca.pem      ... trusting this CA instead
 *   MODEL_CLIENT_CERT / _KEY        mutual TLS (PEM files)
 *   MODEL_AUTH_HEADER=authorization a header on every call, its value from
 *   MODEL_AUTH_TOKEN_FILE / MODEL_AUTH_TOKEN   (a mounted secret file is preferred; e.g. "Bearer ...")
 *   MODEL_AUTHORITY=fraud.models.example.com   the :authority (Host) to send, when it differs from the target
 * </pre>
 *
 * The token is read once and never logged.
 */
public final class ModelChannels {
    private ModelChannels() {}

    public static boolean tls() {
        return Env.bool("MODEL_TLS", false);
    }

    public static ManagedChannel build(String target) {
        ManagedChannelBuilder<?> b = Grpc.newChannelBuilder(target, credentials());
        String authority = Env.str("MODEL_AUTHORITY", "");
        if (!authority.isBlank()) b.overrideAuthority(authority);
        ClientInterceptor auth = authInterceptor();
        if (auth != null) b.intercept(auth);
        return b.maxInboundMessageSize(16 << 20).build();
    }

    static ChannelCredentials credentials() {
        if (!tls()) return InsecureChannelCredentials.create();
        try {
            TlsChannelCredentials.Builder t = TlsChannelCredentials.newBuilder();
            String ca = Env.str("MODEL_CA_FILE", "");
            if (!ca.isBlank()) t.trustManager(new File(ca));
            String cert = Env.str("MODEL_CLIENT_CERT", ""), key = Env.str("MODEL_CLIENT_KEY", "");
            if (!cert.isBlank() && !key.isBlank()) t.keyManager(new File(cert), new File(key));
            return t.build();
        } catch (IOException e) {
            throw new UncheckedIOException("model TLS material", e);
        }
    }

    /** The auth header's value: MODEL_AUTH_TOKEN_FILE (trimmed) or MODEL_AUTH_TOKEN; "" when none. */
    static String token() {
        String f = Env.str("MODEL_AUTH_TOKEN_FILE", "");
        if (!f.isBlank()) {
            try {
                return Files.readString(Path.of(f)).trim();
            } catch (IOException e) {
                throw new UncheckedIOException("MODEL_AUTH_TOKEN_FILE", e);
            }
        }
        return Env.str("MODEL_AUTH_TOKEN", "");
    }

    static ClientInterceptor authInterceptor() {
        String header = Env.str("MODEL_AUTH_HEADER", "");
        String value = header.isBlank() ? "" : token();
        if (header.isBlank() || value.isBlank()) return null;
        Metadata.Key<String> k = Metadata.Key.of(header.toLowerCase(), Metadata.ASCII_STRING_MARSHALLER);
        return new ClientInterceptor() {
            @Override
            public <Q, R> ClientCall<Q, R> interceptCall(MethodDescriptor<Q, R> m, CallOptions o, Channel next) {
                return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(m, o)) {
                    @Override
                    public void start(Listener<R> l, Metadata h) {
                        h.put(k, value);
                        super.start(l, h);
                    }
                };
            }
        };
    }
}
