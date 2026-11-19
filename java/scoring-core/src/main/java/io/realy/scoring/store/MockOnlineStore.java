package io.realy.scoring.store;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/** No I/O: every key is missing (features default). Isolates orchestrator cost from the store. */
public final class MockOnlineStore implements OnlineStore {
    @Override
    public CompletableFuture<byte[][]> getAll(List<String> keys) {
        return CompletableFuture.completedFuture(new byte[keys.size()][]);
    }

    @Override
    public CompletableFuture<Void> setAll(List<String> keys, List<byte[]> values, long ttlSeconds) {
        return CompletableFuture.completedFuture(null);
    }
}
