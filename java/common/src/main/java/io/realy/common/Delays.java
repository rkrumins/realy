package io.realy.common;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Non-blocking delay injection. Thousands of requests can be "in" a synthetic 100 ms stage
 * concurrently (Little's law: 10k TPS x 100 ms = 1,000 in flight) without holding threads.
 */
public final class Delays {
    private static final ScheduledThreadPoolExecutor TIMER;

    static {
        TIMER = new ScheduledThreadPoolExecutor(Math.max(2, Runtime.getRuntime().availableProcessors() / 2), r -> {
            Thread t = new Thread(r, "realy-delay-timer");
            t.setDaemon(true);
            return t;
        });
        TIMER.setRemoveOnCancelPolicy(true);
    }

    private Delays() {}

    /** Completes after {@code delayUs} microseconds on the timer thread. */
    public static CompletableFuture<Void> after(long delayUs) {
        if (delayUs <= 0) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> f = new CompletableFuture<>();
        TIMER.schedule(() -> f.complete(null), delayUs, TimeUnit.MICROSECONDS);
        return f;
    }

    public static Executor delayedExecutor(long delayUs) {
        return command -> TIMER.schedule(command, Math.max(0, delayUs), TimeUnit.MICROSECONDS);
    }

    /** Fails the returned future with {@link java.util.concurrent.TimeoutException} at the deadline. */
    public static <T> CompletableFuture<T> withDeadline(CompletableFuture<T> f, long timeoutUs) {
        if (timeoutUs <= 0) {
            CompletableFuture<T> failed = new CompletableFuture<>();
            failed.completeExceptionally(new java.util.concurrent.TimeoutException("deadline already passed"));
            f.cancel(false);
            return failed;
        }
        CompletableFuture<T> out = new CompletableFuture<>();
        var timer = TIMER.schedule(() -> out.completeExceptionally(
                new java.util.concurrent.TimeoutException("deadline " + timeoutUs + "us")), timeoutUs, TimeUnit.MICROSECONDS);
        f.whenComplete((v, e) -> {
            timer.cancel(false);
            if (e != null) out.completeExceptionally(e); else out.complete(v);
        });
        return out;
    }
}
