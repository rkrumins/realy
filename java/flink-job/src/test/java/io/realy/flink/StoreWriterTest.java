package io.realy.flink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.realy.scoring.store.OnlineStore;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Checkpoint-aligned store writes: bounded, drained at flush, failures decline the checkpoint. */
class StoreWriterTest {
    /** Completes writes only when told to; can fail them. */
    static final class ManualStore implements OnlineStore {
        final ConcurrentLinkedQueue<CompletableFuture<Void>> pending = new ConcurrentLinkedQueue<>();
        final AtomicInteger sets = new AtomicInteger();

        @Override
        public CompletableFuture<byte[][]> getAll(List<String> keys) {
            return CompletableFuture.completedFuture(new byte[keys.size()][]);
        }

        @Override
        public CompletableFuture<Void> setAll(List<String> keys, List<byte[]> values, long ttl) {
            sets.incrementAndGet();
            var f = new CompletableFuture<Void>();
            pending.add(f);
            return f;
        }

        void completeAll() {
            for (CompletableFuture<Void> f; (f = pending.poll()) != null; ) f.complete(null);
        }

        void failOne(Throwable t) {
            pending.poll().completeExceptionally(t);
        }

        void failAll(Throwable t) {
            for (CompletableFuture<Void> f; (f = pending.poll()) != null; ) f.completeExceptionally(t);
        }
    }

    @Test
    void flushWaitsForEveryWriteAndDeclinesAfterAFailure() throws Exception {
        var store = new ManualStore();
        var w = new StoreWriter(store, 10, 30_000);
        for (int i = 0; i < 5; i++) w.write("k" + i, new byte[] {1}, 60, null);
        assertThat(w.inFlight()).isEqualTo(5);
        assertThatThrownBy(() -> w.flush(50)).isInstanceOf(IOException.class).hasMessageContaining("did not complete");
        store.completeAll();
        w.flush(1000);                                   // all landed: the checkpoint may complete
        w.write("x", new byte[] {1}, 60, null);
        store.failAll(new IllegalStateException("OOM command not allowed when used memory > 'maxmemory'"));
        assertThatThrownBy(() -> w.flush(1000)).hasMessageContaining("1 online-store write(s) failed").hasMessageContaining("maxmemory");
        w.flush(1000);                                   // reported once: the next checkpoint starts clean
        assertThat(w.errors()).isEqualTo(1);
    }

    @Test
    void aTimedOutWriteIsRetriedOnceBeforeItCounts() throws Exception {
        var store = new ManualStore();
        var w = new StoreWriter(store, 10, 30_000);
        w.write("k", new byte[] {1}, 60, null);
        store.failOne(new RuntimeException(new TimeoutException("SET timed out")));   // retried
        assertThat(store.sets.get()).isEqualTo(2);
        store.completeAll();
        w.flush(1000);
        assertThat(w.errors()).isZero();
        assertThat(w.retries()).isEqualTo(1);
    }

    @Test
    void writesBlockWhenTheStoreIsSlow() throws Exception {
        var store = new ManualStore();
        var w = new StoreWriter(store, 2, 30_000);
        w.write("a", new byte[] {1}, 60, null);
        w.write("b", new byte[] {1}, 60, null);
        Thread t = new Thread(() -> {
            try {
                w.write("c", new byte[] {1}, 60, null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        t.start();
        t.join(200);
        assertThat(t.isAlive()).isTrue();                // backpressure: the third write waits
        store.completeAll();
        t.join(2000);
        assertThat(t.isAlive()).isFalse();
        store.completeAll();
        w.flush(1000);
    }

    @Test
    void aStoreDownForLongFailsTheTask() throws Exception {
        var store = new ManualStore();
        var w = new StoreWriter(store, 10, 50);
        w.write("k", new byte[] {1}, 60, null);
        store.failAll(new IllegalStateException("connection refused"));
        Thread.sleep(80);
        assertThatThrownBy(w::check).isInstanceOf(IOException.class).hasMessageContaining("online store unavailable");
    }
}
