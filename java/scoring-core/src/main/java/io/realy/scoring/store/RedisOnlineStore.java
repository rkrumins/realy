package io.realy.scoring.store;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Lettuce async client for a standalone Redis or a Redis Cluster (ElastiCache cluster mode, Memorystore
 * Cluster, Redis Enterprise, a self-managed cluster). Commands issued back-to-back are pipelined, so all
 * entity blobs of one event cost a single network round trip per node they live on. Several connections
 * are used round-robin to spread load across Lettuce's event loops.
 *
 * <p>Mode: REDIS_MODE = auto (default: ask the server, INFO cluster) | standalone | cluster. TLS and auth
 * come from the URI (rediss://user:password@host:port); a private CA through the JVM trust store. In
 * cluster mode the topology is refreshed periodically and on MOVED / ASK / reconnects, so a failover or a
 * resharding is followed without a restart.
 */
public final class RedisOnlineStore implements OnlineStore {
    private final AutoCloseable client;
    private final StatefulConnection<byte[], byte[]>[] conns;
    private final Supplier<RedisStringAsyncCommands<byte[], byte[]>>[] cmds;
    private final AtomicInteger rr = new AtomicInteger();
    private final String mode;
    private final String server;
    private final ClientResources resources;

    private static final java.util.Map<String, RedisOnlineStore> SHARED = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicInteger refs = new java.util.concurrent.atomic.AtomicInteger();

    /**
     * One client per JVM and URI (Flink runs many operator instances per TaskManager: a client each
     * would mean dozens of Netty event loops competing for the same cores). Reference counted.
     */
    public static RedisOnlineStore shared(String uri, int connections) {
        RedisOnlineStore s = SHARED.computeIfAbsent(uri, u -> new RedisOnlineStore(u, connections,
                Math.max(2, Runtime.getRuntime().availableProcessors() / 2)));
        s.refs.incrementAndGet();
        return s;
    }

    public RedisOnlineStore(String uri, int connections) {
        this(uri, connections, 0);
    }

    @SuppressWarnings("unchecked")
    public RedisOnlineStore(String uri, int connections, int ioThreads) {
        ClientResources res = ioThreads > 0
                ? DefaultClientResources.builder().ioThreadPoolSize(ioThreads).computationThreadPoolSize(ioThreads).build()
                : DefaultClientResources.create();
        resources = res;
        RedisURI ruri = RedisURI.create(uri);
        String want = io.realy.common.Env.str("REDIS_MODE", "auto").toLowerCase();
        RedisClient standalone = RedisClient.create(res, ruri);
        standalone.setOptions(clientOptions());
        boolean cluster = "cluster".equals(want);
        String info = "";
        if ("auto".equals(want) || cluster) {
            try (StatefulRedisConnection<String, String> probe = standalone.connect()) {
                info = probe.sync().info("server") + probe.sync().info("cluster");
                cluster = cluster || info.contains("cluster_enabled:1");
            } catch (RuntimeException e) {
                if (cluster) throw e;           // asked for a cluster and cannot reach it
            }
        }
        int n = Math.max(1, connections);
        conns = new StatefulConnection[n];
        cmds = new Supplier[n];
        if (cluster) {
            standalone.shutdown(Duration.ZERO, Duration.ZERO);
            RedisClusterClient c = RedisClusterClient.create(res, ruri);
            c.setOptions(clusterOptions());
            client = c;
            for (int i = 0; i < n; i++) {
                StatefulRedisClusterConnection<byte[], byte[]> conn = c.connect(ByteArrayCodec.INSTANCE);
                warmNodeConnections(c, conn);
                conns[i] = conn;
                cmds[i] = conn::async;
            }
        } else {
            client = standalone;
            for (int i = 0; i < n; i++) {
                StatefulRedisConnection<byte[], byte[]> conn = standalone.connect(ByteArrayCodec.INSTANCE);
                conns[i] = conn;
                cmds[i] = conn::async;
            }
        }
        mode = cluster ? "cluster" : "standalone";
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("redis_version:(\\S+)").matcher(info);
        server = m.find() ? m.group(1) : "";
    }

    /**
     * Open every primary's connection now, for reads and for writes. Lettuce opens a cluster node's
     * connection on the first command routed to it, and with commands rejected while disconnected (the
     * fail-fast choice below) the first burst after start-up - a Flink job's first writes, a gateway's
     * first lookups - would race those connections and fail. Measured: a fresh features job failed its
     * first checkpoints with "Currently not connected" until this was done up front.
     */
    static void warmNodeConnections(RedisClusterClient client, StatefulRedisClusterConnection<byte[], byte[]> conn) {
        for (io.lettuce.core.cluster.models.partitions.RedisClusterNode node : client.getPartitions()) {
            if (!node.is(io.lettuce.core.cluster.models.partitions.RedisClusterNode.NodeFlag.UPSTREAM)) continue;
            conn.getConnection(node.getNodeId(), io.lettuce.core.protocol.ConnectionIntent.WRITE);
            conn.getConnection(node.getNodeId(), io.lettuce.core.protocol.ConnectionIntent.READ);
        }
    }

    /** Every connection is open (Lettuce reconnects on its own; a closed one means the store is unreachable). */
    public boolean connected() {
        for (var c : conns) if (!c.isOpen()) return false;
        return true;
    }

    /** "standalone" or "cluster": what the client connected as (reported by /admin/info). */
    public String mode() {
        return mode;
    }

    /** The server's redis_version, when the client could ask for it ("" otherwise). */
    public String serverVersion() {
        return server;
    }

    static ClusterClientOptions clusterOptions() {
        io.lettuce.core.ClientOptions base = clientOptions();
        return ClusterClientOptions.builder(base)
                .topologyRefreshOptions(ClusterTopologyRefreshOptions.builder()
                        .enablePeriodicRefresh(Duration.ofSeconds(io.realy.common.Env.lng("REDIS_TOPOLOGY_REFRESH_S", 30)))
                        .enableAllAdaptiveRefreshTriggers()
                        .build())
                .build();
    }

    /**
     * Fail fast instead of buffering without bound. Lettuce's defaults queue every command while the
     * connection is down (an unbounded request queue): a Redis outage then turns into heap growth until
     * the JVM dies (measured: a TaskManager OutOfMemoryError ~3 min after Redis was OOM-killed). Here a
     * disconnected client rejects commands, the queue is bounded, and a command that gets no answer
     * times out, so callers see errors and apply their own policy (Flink: backpressure, then fail the
     * task and restart from the last checkpoint; gateway: the feature deadline and its fallback).
     */
    static io.lettuce.core.ClientOptions clientOptions() {
        long timeoutMs = io.realy.common.Env.lng("REDIS_COMMAND_TIMEOUT_MS", 2000);
        return io.lettuce.core.ClientOptions.builder()
                .autoReconnect(true)
                .disconnectedBehavior(io.lettuce.core.ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .requestQueueSize(io.realy.common.Env.integer("REDIS_REQUEST_QUEUE", 100_000))
                .timeoutOptions(io.lettuce.core.TimeoutOptions.enabled(java.time.Duration.ofMillis(timeoutMs)))
                .socketOptions(io.lettuce.core.SocketOptions.builder()
                        .connectTimeout(java.time.Duration.ofMillis(io.realy.common.Env.lng("REDIS_CONNECT_TIMEOUT_MS", 2000)))
                        .keepAlive(true).build())
                .build();
    }

    private RedisStringAsyncCommands<byte[], byte[]> cmd() {
        return cmds[Math.floorMod(rr.getAndIncrement(), cmds.length)].get();
    }

    @Override
    public CompletableFuture<byte[][]> getAll(List<String> keys) {
        try {
            return getAll0(keys);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<byte[][]> getAll0(List<String> keys) {
        var c = cmd();
        @SuppressWarnings("unchecked")
        RedisFuture<byte[]>[] fs = new RedisFuture[keys.size()];
        for (int i = 0; i < fs.length; i++) fs[i] = c.get(keys.get(i).getBytes(StandardCharsets.UTF_8));
        CompletableFuture<?>[] cfs = new CompletableFuture[fs.length];
        for (int i = 0; i < fs.length; i++) cfs[i] = fs[i].toCompletableFuture();
        return CompletableFuture.allOf(cfs).thenApply(v -> {
            byte[][] out = new byte[fs.length][];
            for (int i = 0; i < fs.length; i++) out[i] = (byte[]) cfs[i].join();
            return out;
        });
    }

    @Override
    public CompletableFuture<Void> setAll(List<String> keys, List<byte[]> values, long ttlSeconds) {
        try {
            return setAll0(keys, values, ttlSeconds);
        } catch (RuntimeException e) {          // rejected (disconnected / queue full): an error, not a throw
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<Void> setAll0(List<String> keys, List<byte[]> values, long ttlSeconds) {
        var c = cmd();
        CompletableFuture<?>[] cfs = new CompletableFuture[keys.size()];
        SetArgs args = ttlSeconds > 0 ? SetArgs.Builder.ex(ttlSeconds) : null;
        for (int i = 0; i < cfs.length; i++) {
            byte[] k = keys.get(i).getBytes(StandardCharsets.UTF_8);
            cfs[i] = (args == null ? c.set(k, values.get(i)) : c.set(k, values.get(i), args)).toCompletableFuture();
        }
        return CompletableFuture.allOf(cfs);
    }

    @Override
    public void close() {
        if (SHARED.containsValue(this) && refs.decrementAndGet() > 0) return;
        SHARED.values().remove(this);
        for (var c : conns) c.close();
        if (client instanceof RedisClusterClient c) c.shutdown();
        else ((RedisClient) client).shutdown();
        resources.shutdown();
    }
}
