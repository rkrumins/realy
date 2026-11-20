package io.realy.scoring.features;

import io.realy.contracts.payments.v1.TransactionEvent;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
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
    /**
     * State encodings. v1: fixed-width (8-byte bucket index + two doubles per bucket, 16 bytes per
     * distinct entry). v2: varints, delta-coded bucket indices and keys, integral values as varints and
     * other values as raw doubles, so every value round-trips exactly (about 2-3x smaller). Both decode;
     * v2 is written. The 8-byte last-event time stays at offset 1 in both.
     */
    private static final byte STATE_V1 = 1;
    private static final byte STATE_V2 = 2;
    private static final byte STATE_VERSION = STATE_V2;
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

    /** Re-encode a state blob in the v1 layout (tests: state written by earlier releases). */
    public byte[] toV1(byte[] stateBytes) {
        return State.decode(stateBytes, specs).encodeV1(specs);
    }

    /** Decode any supported layout and re-encode in the current one. */
    public byte[] reencode(byte[] stateBytes) {
        return State.decode(stateBytes, specs).encode(specs);
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
            if (bytes[0] == STATE_V2) return decodeV2(st, bytes, specs);
            if (bytes[0] != STATE_V1) return new State().fresh(specs);
            try (var in = new DataInputStream(new ByteArrayInputStream(bytes))) {
                in.readByte();
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

        private static State decodeV2(State st, byte[] bytes, List<FeatureSpec> specs) {
            var in = new Reader(bytes, 1);
            st.lastEventUs = in.fixedLong();
            int nf = (int) in.varLong();
            if (nf != specs.size()) return new State().fresh(specs);
            long base = st.lastEventUs;
            for (int i = 0; i < nf; i++) {
                Feat f = st.feats[i];
                long mk = in.varLong();
                f.maxK = mk == 0 ? NONE : unzig(mk - 1);
                switch (specs.get(i).agg()) {
                    case "count", "sum", "max", "avg" -> {
                        int n = (int) in.varLong();
                        f.idx = new long[Math.max(4, n)];
                        f.a = new double[Math.max(4, n)];
                        f.b = new double[Math.max(4, n)];
                        long prev = 0;
                        for (int j = 0; j < n; j++) {
                            prev = j == 0 ? (f.maxK == NONE ? 0 : f.maxK) + unzig(in.varLong()) : prev + in.varLong();
                            f.idx[j] = prev;
                            f.a[j] = in.num();
                            f.b[j] = in.num();
                        }
                        f.size = n;
                    }
                    case "distinct", "novel" -> {
                        int n = (int) in.varLong();
                        long key = 0;
                        for (int j = 0; j < n; j++) {
                            key = j == 0 ? unzig(in.varLong()) : key + in.varLong();
                            f.seen.put(key, base - unzig(in.varLong()));
                        }
                    }
                    case "time_since_last" -> f.lastTs = in.optTime(base);
                    case "geo_speed_kmh" -> {
                        f.lastTs = in.optTime(base);
                        f.lat = in.rawDouble();
                        f.lon = in.rawDouble();
                    }
                    default -> throw new IllegalStateException("unknown agg");
                }
            }
            return st;
        }

        private State fresh(List<FeatureSpec> specs) {
            return decode(null, specs);
        }

        /** The v1 layout (kept to test that state written by earlier releases still decodes identically). */
        byte[] encodeV1(List<FeatureSpec> specs) {
            var bos = new java.io.ByteArrayOutputStream(64);
            try (var out = new java.io.DataOutputStream(bos)) {
                out.writeByte(STATE_V1);
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

        byte[] encode(List<FeatureSpec> specs) {
            var out = new Writer(48 + specs.size() * 6);
            out.u8(STATE_VERSION);
            out.fixedLong(lastEventUs);
            out.varLong(specs.size());
            long base = lastEventUs;
            for (int i = 0; i < specs.size(); i++) {
                Feat f = feats[i];
                out.varLong(f.maxK == NONE ? 0 : zig(f.maxK) + 1);
                switch (specs.get(i).agg()) {
                    case "count", "sum", "max", "avg" -> {
                        out.varLong(f.size);
                        for (int j = 0; j < f.size; j++) {
                            out.varLong(j == 0 ? zig(f.idx[0] - (f.maxK == NONE ? 0 : f.maxK)) : f.idx[j] - f.idx[j - 1]);
                            out.num(f.a[j]);
                            out.num(f.b[j]);
                        }
                    }
                    case "distinct", "novel" -> {
                        int n = f.seen.size();
                        out.varLong(n);
                        long[] keys = new long[n];
                        int k = 0;
                        for (long key : f.seen.keySet()) keys[k++] = key;
                        java.util.Arrays.sort(keys);
                        for (int j = 0; j < n; j++) {
                            out.varLong(j == 0 ? zig(keys[0]) : keys[j] - keys[j - 1]);
                            out.varLong(zig(base - f.seen.get(keys[j])));
                        }
                    }
                    case "time_since_last" -> out.optTime(f.lastTs, base);
                    case "geo_speed_kmh" -> {
                        out.optTime(f.lastTs, base);
                        out.rawDouble(f.lat);
                        out.rawDouble(f.lon);
                    }
                    default -> throw new IllegalStateException("unknown agg");
                }
            }
            return out.toByteArray();
        }
    }

    // ------------------------------------------------------------------------------------- codec
    static long zig(long v) {
        return (v << 1) ^ (v >> 63);
    }

    static long unzig(long v) {
        return (v >>> 1) ^ -(v & 1);
    }

    /** Growable little buffer: varints, fixed longs and exact numbers (no stream / synchronisation). */
    static final class Writer {
        private byte[] buf;
        private int n;

        Writer(int cap) {
            buf = new byte[Math.max(16, cap)];
        }

        private void ensure(int k) {
            if (n + k > buf.length) buf = java.util.Arrays.copyOf(buf, Math.max(buf.length * 2, n + k));
        }

        void u8(int b) {
            ensure(1);
            buf[n++] = (byte) b;
        }

        void fixedLong(long v) {       // big-endian, as v1 (lastEventUs() reads it at offset 1)
            ensure(8);
            for (int i = 7; i >= 0; i--) buf[n++] = (byte) (v >>> (8 * i));
        }

        void varLong(long v) {         // unsigned LEB128
            ensure(10);
            while ((v & ~0x7FL) != 0) {
                buf[n++] = (byte) ((v & 0x7F) | 0x80);
                v >>>= 7;
            }
            buf[n++] = (byte) v;
        }

        void rawDouble(double d) {
            fixedLong(Double.doubleToRawLongBits(d));
        }

        /** Integral values (counts, sums of minor units) as a varint, anything else as a raw double. */
        void num(double d) {
            long l = (long) d;
            if (l == d && Math.abs(l) < (1L << 52) && !(d == 0.0 && Double.doubleToRawLongBits(d) != 0L)) {
                varLong(zig(l) << 1);
            } else {
                varLong(1);
                rawDouble(d);
            }
        }

        void optTime(long t, long base) {
            varLong(t == NONE ? 0 : zig(base - t) + 1);
        }

        byte[] toByteArray() {
            return java.util.Arrays.copyOf(buf, n);
        }
    }

    static final class Reader {
        private final byte[] b;
        private int p;

        Reader(byte[] b, int p) {
            this.b = b;
            this.p = p;
        }

        long fixedLong() {
            long v = 0;
            for (int i = 0; i < 8; i++) v = (v << 8) | (b[p++] & 0xFFL);
            return v;
        }

        long varLong() {
            long v = 0;
            int shift = 0;
            while (true) {
                byte x = b[p++];
                v |= (long) (x & 0x7F) << shift;
                if ((x & 0x80) == 0) return v;
                shift += 7;
            }
        }

        double rawDouble() {
            return Double.longBitsToDouble(fixedLong());
        }

        double num() {
            long tag = varLong();
            if ((tag & 1) == 0) return (double) unzig(tag >>> 1);
            return rawDouble();
        }

        long optTime(long base) {
            long v = varLong();
            return v == 0 ? NONE : base - unzig(v - 1);
        }
    }
}
