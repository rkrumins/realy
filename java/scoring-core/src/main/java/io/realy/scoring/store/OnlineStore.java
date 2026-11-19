package io.realy.scoring.store;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Multi-key binary GET against the online feature store. Missing keys map to null entries. */
public interface OnlineStore extends AutoCloseable {
    CompletableFuture<byte[][]> getAll(List<String> keys);

    CompletableFuture<Void> setAll(List<String> keys, List<byte[]> values, long ttlSeconds);

    @Override
    default void close() {}
}
