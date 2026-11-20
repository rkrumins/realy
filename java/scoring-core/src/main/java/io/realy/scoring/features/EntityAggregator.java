package io.realy.scoring.features;

import io.realy.contracts.payments.v1.TransactionEvent;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Windowed aggregates for all stream features of one entity, over a compact sparse state blob.
 * Semantics are documented in contracts/features/feature_schema.yaml and mirrored exactly by
 * python/realy/features/engine.py. The Flink job keeps the blob in RocksDB ValueState<byte[]>.
 */
public final class EntityAggregator {
    private static final byte STATE_VERSION = 1;
    private static final long NONE = Long.MIN_VALUE;
    private static final long MIN_GEO_DT_US = 60_000_000L;

    private final String entity;
    private final List<FeatureSpec> specs;
    private final int[] filterBits;
    private final int[] indices;

    public EntityAggregator(FeatureSchema schema, String entity) {
        this.entity = entity;
        this.specs = schema.stream(entity);
        this.filterBits = new int[specs.size()];
        this.indices = new int[specs.size()];
        for (int i = 0; i < specs.size(); i++) {
            FeatureSpec s = specs.get(i);
            filterBits[i] = s.filter() == null ? -1 : schema.filterBit(s.filter());
            indices[i] = s.index();
        }
    }

    public String entity() {
        return entity;
    }

    public List<FeatureSpec> specs() {
        return specs;
    }

    /** Schema indices of this entity's features, in blob order. */
    public int[] featureIndices() {
        return indices;
    }

    /** Write defaults for an event that does not carry the entity. */
    public void defaults(float[] out) {
        for (int i = 0; i < specs.size(); i++) out[indices[i]] = (float) specs.get(i).defaultValue();
    }

    /**
     * Update state with {@code e} and write the entity's feature values into {@code out} at their
     * schema indices. Returns the new state blob.
     */
    public byte[] process(byte[] stateBytes, TransactionEvent e, long filterMask, float[] out) {
        State st = State.decode(stateBytes, specs);
        long t = e.getEventTimeUs();
        for (int i = 0; i < specs.size(); i++) {
            FeatureSpec s = specs.get(i);
            boolean match = filterBits[i] < 0 || (filterMask & (1L << filterBits[i])) != 0;
            out[indices[i]] = (float) apply(s, st.feats[i], e, t, match);
        }
        st.lastEventUs = Math.max(st.lastEventUs, t);
        return st.encode(specs);
    }

    /**
     * Read-only variant of {@link #process}: the feature values {@code e} would get if it were applied
     * to {@code stateBytes} now (update-then-read, exactly the inline semantics), without producing a
     * new state. {@code stateBytes} is never modified. Used by the gateway to re-window an online-store
     * state blob at the event's own time; {@code null} state = an entity never seen before.
     */
    public void evaluate(byte[] stateBytes, TransactionEvent e, long filterMask, float[] out) {
        State st = State.decode(stateBytes, specs);
        long t = e.getEventTimeUs();
        for (int i = 0; i < specs.size(); i++) {
            FeatureSpec s = specs.get(i);
            boolean match = filterBits[i] < 0 || (filterMask & (1L << filterBits[i])) != 0;
            out[indices[i]] = (float) apply(s, st.feats[i], e, t, match);
        }
    }

    private static double apply(FeatureSpec s, Feat f, TransactionEvent e, long t, boolean match) {
        long g = s.granularityUs();
        long k = Math.floorDiv(t, g);
        long n = s.buckets();
        switch (s.agg()) {
            case "count", "sum", "max", "avg" -> {
                f.maxK = Math.max(f.maxK, k);
                if (match) {
                    double v = "count".equals(s.agg()) ? 1.0 : EventView.numeric(e, s.value());
                    f.add(k, v, s.agg());
                }
                f.evictBuckets(f.maxK - n);
                double a = 0;
                double b = 0;
                boolean any = false;
                double max = 0;
                for (int j = 0; j < f.size; j++) {
                    long idx = f.idx[j];
                    if (idx > k - n && idx <= k) {
                        if ("max".equals(s.agg())) {
                            max = any ? Math.max(max, f.a[j]) : f.a[j];
                        } else {
                            a += f.a[j];
                            b += f.b[j];
                        }
                        any = true;
                    }
                }
                return switch (s.agg()) {
                    case "max" -> any ? max : s.defaultValue();
                    case "avg" -> b > 0 ? a / b : s.defaultValue();
                    default -> a;
                };
            }
            case "distinct" -> {
                f.maxK = Math.max(f.maxK, k);
                if (match) f.seen.merge(EventView.valueKey(e, s.value()), k, Math::max);
                f.evictSeen(f.maxK - n);
                int c = 0;
                for (long last : f.seen.values()) if (last > k - n) c++;
                return c;
            }
            case "novel" -> {
                f.maxK = Math.max(f.maxK, k);
                f.evictSeen(f.maxK - n);
                if (!match) return 0.0;
                long key = EventView.valueKey(e, s.value());
                Long last = f.seen.get(key);
                boolean seen = last != null && last > k - n;
                f.seen.merge(key, k, Math::max);
                return seen ? 0.0 : 1.0;
            }
            case "time_since_last" -> {
                double v = (f.lastTs != NONE && t >= f.lastTs && t - f.lastTs <= s.windowUs())
                        ? (t - f.lastTs) / 1e6 : s.defaultValue();
                if (match && (f.lastTs == NONE || t >= f.lastTs)) f.lastTs = t;
                return v;
            }
            case "geo_speed_kmh" -> {
                double v = s.defaultValue();
                if (match && f.lastTs != NONE && t >= f.lastTs && t - f.lastTs <= s.windowUs()) {
                    double km = Geo.haversineKm(f.lat, f.lon, e.getLat(), e.getLon());
                    long dt = Math.max(t - f.lastTs, MIN_GEO_DT_US);
                    v = km / (dt / 3.6e9);
                }
                if (match && (f.lastTs == NONE || t >= f.lastTs)) {
                    f.lastTs = t;
                    f.lat = e.getLat();
                    f.lon = e.getLon();
                }
                return v;
            }
            default -> throw new IllegalArgumentException("unknown agg " + s.agg());
        }
    }

    /** Last event time seen in a state blob (the online-store `as_of`). */
    public static long lastEventUs(byte[] stateBytes) {
        if (stateBytes == null || stateBytes.length < 9) return NONE;
        return java.nio.ByteBuffer.wrap(stateBytes, 1, 8).getLong();
    }

    // ------------------------------------------------------------------------------------- state
    static final class Feat {
        long maxK = NONE;
        int size;
        long[] idx = new long[4];
        double[] a = new double[4];
        double[] b = new double[4];
        Map<Long, Long> seen;
        long lastTs = NONE;
        double lat;
        double lon;

        void add(long k, double v, String agg) {
            int pos = size;
            while (pos > 0 && idx[pos - 1] > k) pos--;
            if (pos > 0 && idx[pos - 1] == k) {
                int j = pos - 1;
                if ("max".equals(agg)) {
                    a[j] = Math.max(a[j], v);
                } else {
                    a[j] += v;
                    b[j] += 1;
                }
                return;
            }
            if (size == idx.length) {
                idx = java.util.Arrays.copyOf(idx, size * 2);
                a = java.util.Arrays.copyOf(a, size * 2);
                b = java.util.Arrays.copyOf(b, size * 2);
            }
            System.arraycopy(idx, pos, idx, pos + 1, size - pos);
            System.arraycopy(a, pos, a, pos + 1, size - pos);
            System.arraycopy(b, pos, b, pos + 1, size - pos);
            idx[pos] = k;
            a[pos] = v;
            b[pos] = 1;
            size++;
        }

        void evictBuckets(long upTo) {
            int drop = 0;
            while (drop < size && idx[drop] <= upTo) drop++;
            if (drop == 0) return;
            System.arraycopy(idx, drop, idx, 0, size - drop);
            System.arraycopy(a, drop, a, 0, size - drop);
            System.arraycopy(b, drop, b, 0, size - drop);
            size -= drop;
        }

        void evictSeen(long upTo) {
            for (Iterator<Long> it = seen.values().iterator(); it.hasNext(); ) {
                if (it.next() <= upTo) it.remove();
            }
        }
    }

    static final class State {
        Feat[] feats;
        long lastEventUs = NONE;

        static State decode(byte[] bytes, List<FeatureSpec> specs) {
            State st = new State();
            st.feats = new Feat[specs.size()];
            for (int i = 0; i < specs.size(); i++) {
                st.feats[i] = new Feat();
                String agg = specs.get(i).agg();
                if ("distinct".equals(agg) || "novel".equals(agg)) st.feats[i].seen = new HashMap<>();
            }
            if (bytes == null || bytes.length == 0) return st;
            try (var in = new DataInputStream(new ByteArrayInputStream(bytes))) {
                byte ver = in.readByte();
                if (ver != STATE_VERSION) return new State().fresh(specs);
                st.lastEventUs = in.readLong();
                int nf = in.readUnsignedShort();
                if (nf != specs.size()) return new State().fresh(specs); // schema changed: reset state
                for (int i = 0; i < nf; i++) {
                    Feat f = st.feats[i];
                    f.maxK = in.readLong();
                    switch (specs.get(i).agg()) {
                        case "count", "sum", "max", "avg" -> {
                            int n = in.readInt();
                            f.idx = new long[Math.max(4, n)];
                            f.a = new double[Math.max(4, n)];
                            f.b = new double[Math.max(4, n)];
                            for (int j = 0; j < n; j++) {
                                f.idx[j] = in.readLong();
                                f.a[j] = in.readDouble();
                                f.b[j] = in.readDouble();
                            }
                            f.size = n;
                        }
                        case "distinct", "novel" -> {
                            int n = in.readInt();
                            for (int j = 0; j < n; j++) f.seen.put(in.readLong(), in.readLong());
                        }
                        case "time_since_last" -> f.lastTs = in.readLong();
                        case "geo_speed_kmh" -> {
                            f.lastTs = in.readLong();
                            f.lat = in.readDouble();
                            f.lon = in.readDouble();
                        }
                        default -> throw new IllegalStateException("unknown agg");
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return st;
        }

        private State fresh(List<FeatureSpec> specs) {
            return decode(null, specs);
        }

        byte[] encode(List<FeatureSpec> specs) {
            var bos = new ByteArrayOutputStream(64);
            try (var out = new DataOutputStream(bos)) {
                out.writeByte(STATE_VERSION);
                out.writeLong(lastEventUs);
                out.writeShort(specs.size());
                for (int i = 0; i < specs.size(); i++) {
                    Feat f = feats[i];
                    out.writeLong(f.maxK);
                    switch (specs.get(i).agg()) {
                        case "count", "sum", "max", "avg" -> {
                            out.writeInt(f.size);
                            for (int j = 0; j < f.size; j++) {
                                out.writeLong(f.idx[j]);
                                out.writeDouble(f.a[j]);
                                out.writeDouble(f.b[j]);
                            }
                        }
                        case "distinct", "novel" -> {
                            out.writeInt(f.seen.size());
                            for (var en : f.seen.entrySet()) {
                                out.writeLong(en.getKey());
                                out.writeLong(en.getValue());
                            }
                        }
                        case "time_since_last" -> out.writeLong(f.lastTs);
                        case "geo_speed_kmh" -> {
                            out.writeLong(f.lastTs);
                            out.writeDouble(f.lat);
                            out.writeDouble(f.lon);
                        }
                        default -> throw new IllegalStateException("unknown agg");
                    }
                }
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
            return bos.toByteArray();
        }
    }
}
