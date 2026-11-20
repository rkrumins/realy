package io.realy.scoring.model;

import java.util.concurrent.CompletableFuture;

/** Scores one feature row against a model server. */
public interface ModelClient extends AutoCloseable {
    /** Result of one inference; server timings come from KServe v2 response parameters. */
    record Result(float score, String modelName, String modelVersion, long serverQueueUs, long serverComputeUs,
                  int batchSize, long clientBatchWaitUs) {}

    CompletableFuture<Result> infer(float[] features, long timeoutUs);

    @Override
    default void close() {}
}
