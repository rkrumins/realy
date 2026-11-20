package io.realy.flink;

import java.util.Arrays;

/**
 * Flink POJO carried between operators: the raw event bytes, the keys of every entity, the filter
 * mask (CEL filters evaluated once at the source), the feature vector being filled stage by stage,
 * and the benchmark spans (3 longs each: stage<<40 | attr<<32 | domain, start_us, dur_us).
 */
public class Envelope {
    public byte[] event;
    public byte[] trace;           // original x-bench-trace header (may be null)
    public long eventId;
    public long eventTimeUs;
    public long customerId;
    public long cardId;
    public long deviceId;
    public long ip;
    public long merchantId;
    public long terminalId;
    public long payeeId;
    public long filterMask;
    public float[] features;
    public long ingestUs;
    public long lastEmitUs;
    public long[] spans = new long[0];
    public boolean mirrored;
    public boolean backfill;       // warm-start history: update features, never score

    public Envelope() {}

    public long key(String entity) {
        return switch (entity) {
            case "customer" -> customerId;
            case "card" -> cardId;
            case "device" -> deviceId;
            case "ip" -> ip;
            case "merchant" -> merchantId;
            case "terminal" -> terminalId;
            case "payee" -> payeeId;
            default -> 0;
        };
    }

    public void span(int stage, int attr, int domain, long startUs, long durUs) {
        int n = spans.length;
        spans = Arrays.copyOf(spans, n + 3);
        spans[n] = ((long) stage << 40) | ((long) (attr & 0xff) << 32) | (domain & 0xffffffffL);
        spans[n + 1] = startUs;
        spans[n + 2] = durUs;
    }
}
