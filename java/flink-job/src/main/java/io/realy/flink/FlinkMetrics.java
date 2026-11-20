package io.realy.flink;

import io.realy.common.Env;
import io.realy.common.StageMetrics;

/** One bench.metrics publisher per TaskManager JVM, shared by all subtasks. */
public final class FlinkMetrics {
    private static volatile StageMetrics INSTANCE;

    private FlinkMetrics() {}

    public static StageMetrics get() {
        if (INSTANCE == null) {
            synchronized (FlinkMetrics.class) {
                if (INSTANCE == null) {
                    INSTANCE = new StageMetrics("flink", Env.str("HOSTNAME", "flink-tm")).startPublishing();
                }
            }
        }
        return INSTANCE;
    }
}
