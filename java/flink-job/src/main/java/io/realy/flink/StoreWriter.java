package io.realy.flink;

import io.realy.scoring.store.OnlineStore;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Asynchronous online-store writes with the guarantees of a checkpointed sink.
 *
 * <ul>
 *   <li><b>Bounded in-flight writes.</b> At most {@code maxInFlight} writes per operator instance are
 *       outstanding; the next one waits for a free slot, so a slow store backpressures the job instead
 *       of growing the heap.</li>
 *   <li><b>Aligned with checkpoints.</b> {@link #flush} (called from {@code snapshotState}, before the
 *       checkpoint barrier is passed on) waits until every write issued so far has completed. A completed
 *       checkpoint therefore means the store holds every write for the events before it; a write that
 *       failed since the previous checkpoint declines this one, so it is never acknowledged.</li>
 *   <li><b>Fails loudly.</b> When the store has failed every write for {@code failAfterMs}, the task
 *       fails: Flink restarts the job (restart strategy) and replays from the last completed checkpoint
 *       once the store is back, rewriting what was lost. Errors are counted ({@code store_write_errors}).</li>
 * </ul>
 * Writes are idempotent (the latest blob per key), so replays after a restore are safe.
 */
final class StoreWriter {
    private final OnlineStore store;
    private final int maxInFlight;
    private final Semaphore permits;
    private final long failAfterMs;
    private final AtomicLong failedSinceFlush = new AtomicLong();
    private final AtomicLong errors = new AtomicLong();
    private final AtomicLong retries = new AtomicLong();
    private final AtomicReference<Throwable> lastError = new AtomicReference<>();
    private volatile long lastOkMs;
    private volatile long firstFailMs;

    StoreWriter(OnlineStore store, int maxInFlight, long failAfterMs) {
        this.store = store;
        this.maxInFlight = Math.max(1, maxInFlight);
        this.permits = new Semaphore(this.maxInFlight);
        this.failAfterMs = failAfterMs;
        this.lastOkMs = System.currentTimeMillis();
    }

    /** Issue one write; blocks while {@code maxInFlight} writes are outstanding (backpressure). */
    void write(String key, byte[] value, long ttlSeconds, Consumer<Throwable> onDone) throws IOException, InterruptedException {
        check();
        permits.acquire();
        issue(key, value, ttlSeconds, onDone, 0);
    }

    /** A write that timed out is retried once (SET is idempotent) before it counts as failed. */
    private void issue(String key, byte[] value, long ttlSeconds, Consumer<Throwable> onDone, int attempt) {
        CompletableFuture<Void> f;
        try {
            f = store.setAll(List.of(key), List.of(value), ttlSeconds);
        } catch (RuntimeException e) {
            f = CompletableFuture.failedFuture(e);
        }
        f.whenComplete((v, err) -> {
            if (err != null && attempt == 0 && isTimeout(err)) {
                retries.incrementAndGet();
                issue(key, value, ttlSeconds, onDone, 1);
                return;
            }
            permits.release();
            if (err == null) {
                lastOkMs = System.currentTimeMillis();
                firstFailMs = 0;
            } else {
                errors.incrementAndGet();
                failedSinceFlush.incrementAndGet();
                lastError.set(err);
                if (firstFailMs == 0) firstFailMs = System.currentTimeMillis();
            }
            if (onDone != null) onDone.accept(err);
        });
    }

    /** Fail the task once the store has been failing continuously for longer than {@code failAfterMs}. */
    void check() throws IOException {
        long since = firstFailMs;
        if (since != 0 && lastOkMs < since && System.currentTimeMillis() - since > failAfterMs) {
            throw new IOException("online store unavailable for " + (System.currentTimeMillis() - since) / 1000
                    + " s (" + errors.get() + " failed writes): " + describe(lastError.get()));
        }
    }

    /**
     * Wait for every outstanding write, then fail if any write failed since the last flush. Called before
     * a checkpoint is acknowledged.
     */
    void flush(long timeoutMs) throws Exception {
        if (!permits.tryAcquire(maxInFlight, timeoutMs, TimeUnit.MILLISECONDS)) {
            throw new IOException("online store writes did not complete within " + timeoutMs + " ms ("
                    + (maxInFlight - permits.availablePermits()) + " outstanding)");
        }
        permits.release(maxInFlight);
        long failed = failedSinceFlush.getAndSet(0);
        if (failed > 0) {
            throw new IOException(failed + " online-store write(s) failed since the last checkpoint: "
                    + describe(lastError.get()));
        }
    }

    long errors() {
        return errors.get();
    }

    long retries() {
        return retries.get();
    }

    private static boolean isTimeout(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
            String n = c.getClass().getSimpleName();
            if (n.contains("Timeout")) return true;
        }
        return false;
    }

    int inFlight() {
        return maxInFlight - permits.availablePermits();
    }

    private static String describe(Throwable t) {
        if (t == null) return "unknown";
        Throwable c = t;
        while (c.getCause() != null && c.getCause() != c) c = c.getCause();
        return c.getClass().getSimpleName() + ": " + c.getMessage();
    }
}
