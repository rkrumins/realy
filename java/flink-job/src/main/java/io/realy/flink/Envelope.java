package io.realy.flink;

import io.realy.scoring.features.EventRow;
import java.util.Arrays;

/**
 * Flink POJO carried between operators: the raw event bytes, the event decoded once ({@link EventRow}:
 * time, location, every entity's key and the values the stream features read), the filter mask (CEL
 * filters evaluated once at the source), the feature vector being filled stage by stage, and the benchmark
 * spans (3 longs each: stage<<40 | attr<<32 | domain, start_us, dur_us).
 */
public class Envelope {
    public byte[] event;
    public byte[] trace;           // original x-bench-trace header (may be null)
    public long eventId;
    public long eventTimeUs;
    public EventRow row;
    public long filterMask;
    public float[] features;
    public long ingestUs;
    public long lastEmitUs;
    public long[] spans = new long[0];
    public boolean mirrored;
    public boolean backfill;       // warm-start history: update features, never score
    /** A part of a multi-message transaction before assembly: its event type (null = the unified event). */
    public String type;
    /** The TxnContext of an assembled transaction (serialized; null = a one-message payment). */
    public byte[] txn;
    /** A re-scored lifecycle stage: the features read it, nothing folds it into state again. */
    public boolean readOnly;
    /** The x-run-id header, carried to what the job publishes (the assembled transactions topic). */
    public byte[] runId;

    public Envelope() {}

    public void span(int stage, int attr, int domain, long startUs, long durUs) {
        int n = spans.length;
        spans = Arrays.copyOf(spans, n + 3);
        spans[n] = ((long) stage << 40) | ((long) (attr & 0xff) << 32) | (domain & 0xffffffffL);
        spans[n + 1] = startUs;
        spans[n + 2] = durUs;
    }
}
