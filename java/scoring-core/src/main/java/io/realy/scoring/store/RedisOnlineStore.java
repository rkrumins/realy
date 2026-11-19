package io.realy.scoring.store;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lettuce async client. Commands issued back-to-back are pipelined on a shared connection, so all
 * entity blobs of one event cost a single network round trip. Several connections are used
 * round-robin to spread load across Lettuce's event loops.
 */
public final class RedisOnlineStore implements OnlineStore {
    private final RedisClient client;
    private final StatefulRedisConnection<byte[], byte[]>[] conns;
    private final AtomicInteger rr = new AtomicInteger();

    @SuppressWarnings("unchecked")
    public RedisOnlineStore(String uri, int connections) {
        client = RedisClient.create(uri);
        conns = new StatefulRedisConnection[Math.max(1, connections)];
        for (int i = 0; i < conns.length; i++) conns[i] = client.connect(ByteArrayCodec.INSTANCE);
    }

    private RedisAsyncCommands<byte[], byte[]> cmd() {
        return conns[Math.floorMod(rr.getAndIncrement(), conns.length)].async();
    }

    @Override
    public CompletableFuture<byte[][]> getAll(List<String> keys) {
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
        for (var c : conns) c.close();
        client.shutdown();
    }
}
