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

    private final String entity;
    private final List<FeatureSpec> specs;
    private final int[] filterBits;
    private final int[] indices;
    private final RowPlan plan;
    private final int ordinal;
    private final int[] slots;   // per feature: its EventRow num or val slot (-1: reads no value)
    private final Aggregation[] aggs;       // per feature: its aggregation (Aggregations registry)
    private final Aggregation.Shape[] shapes;
    private final long[] onMasks;           // per feature: the event types (pipeline ordinals) that fold into it
    private final int[] gates;              // per feature: the schema index of its `gate:` feature (-1: none)
    private final boolean[] sticky;         // per feature: state kept past the keyed-state TTL (Aggregation.sticky)
    private final boolean anySticky;
    private final int defaultType;          // the pipeline's default type ordinal
    private long anyMask;                   // the types that fold into at least one of this entity's features

    public EntityAggregator(FeatureSchema schema, String entity) {
        this(schema, entity, io.realy.scoring.binding.PipelineBinding.current());
    }

    /** The aggregator of {@code entity} for the event types of pipeline {@code pipe}. */
    public EntityAggregator(FeatureSchema schema, String entity, io.realy.scoring.binding.PipelineBinding pipe) {
        this.entity = entity;
        this.specs = schema.stream(entity);
        this.plan = schema.rowPlan();
        this.ordinal = plan.entityOrdinal(entity);
        this.filterBits = new int[specs.size()];
        this.indices = new int[specs.size()];
        this.slots = new int[specs.size()];
        this.aggs = new Aggregation[specs.size()];
        this.shapes = new Aggregation.Shape[specs.size()];
        this.onMasks = new long[specs.size()];
        this.gates = new int[specs.size()];
        this.sticky = new boolean[specs.size()];
        this.defaultType = pipe.defaultOrdinal();
        boolean anyS = false;
        for (int i = 0; i < specs.size(); i++) {
            FeatureSpec s = specs.get(i);
            List<String> on = schema.on(s);
            long m = 0;
            if (on == null) {
                m = 1L << defaultType;
            } else {
                for (String t : on) {
                    int o = pipe.typeOrdinal(t);
                    if (o < 0) throw new IllegalArgumentException(s.name() + ": `event_types` names " + t + ", not an event type of pipeline " + pipe.name);
                    m |= 1L << o;
                }
            }
            onMasks[i] = m;
            anyMask |= m;
            gates[i] = schema.gate(s);
            if (gates[i] >= 0) {
                // a gate is read from the feature vector, which only a scored event carries
                for (int t = 0; t < 64; t++)
                    if ((m & (1L << t)) != 0 && !pipe.scored(t))
                        throw new IllegalArgumentException(s.name() + ": a gated feature folds only scored event types (a fold-only "
                                + "event has no feature vector to read its gate from)");
            }
            filterBits[i] = s.filter() == null ? -1 : schema.filterBit(s.filter());
            indices[i] = s.index();
            aggs[i] = Aggregations.get(s.agg());
            shapes[i] = aggs[i].shape();
            sticky[i] = aggs[i].sticky(s);
            anyS |= sticky[i];
            slots[i] = switch (aggs[i].input()) {
                case NUMERIC -> plan.numSlot(s.value());
                case OPTIONAL_NUMERIC -> s.value() == null ? -1 : plan.numSlot(s.value());
                case KEY -> plan.valSlot(s.value());
                default -> -1;
            };
        }
        this.anySticky = anyS;
    }

    /** Feature {@code i}'s filter bit, and its gate: the gating feature (already in {@code out}) is non-zero. */
    private boolean matches(int i, long filterMask, float[] out) {
        if (filterBits[i] >= 0 && (filterMask & (1L << filterBits[i])) == 0) return false;
        return gates[i] < 0 || (out != null && out[gates[i]] != 0f);
    }

    /** This entity's key on a decoded event (0 = absent). */
    public long key(EventRow r) {
        return r.keys[ordinal];
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
        return process(stateBytes, plan.row(e), filterMask, out);
    }

    /** {@link #process} on an event decoded once ({@link RowPlan#row}): the keyed stages' path. */
    public byte[] process(byte[] stateBytes, EventRow r, long filterMask, float[] out) {
        return process(stateBytes, r, filterMask, out, defaultType);
    }

    /**
     * An event of type {@code type} (its pipeline ordinal): it folds into the features whose `event_types` lists the
     * type and, when {@code out} is given (a scored event), reads the others as of its time without folding;
     * a fold-only event ({@code out == null}) leaves them alone. Returns the new state, or {@code stateBytes}
     * itself when nothing folded (the caller then writes nothing). Mirrors ReferenceEngine.process (Python).
     */
    public byte[] process(byte[] stateBytes, EventRow r, long filterMask, float[] out, int type) {
        long bit = 1L << type;
        if ((anyMask & bit) == 0 && out == null) return stateBytes;
        State st = State.decode(stateBytes, shapes);
        long t = r.timeUs;
        boolean folded = false;
        for (int i = 0; i < specs.size(); i++) {
            boolean fold = (onMasks[i] & bit) != 0;
            if (!fold && out == null) continue;
            boolean match = matches(i, filterMask, out);
            double v = fold ? aggs[i].apply(specs.get(i), st.feats[i], r, slots[i], t, match)
                    : aggs[i].read(specs.get(i), st.feats[i], r, slots[i], t, match);
            folded |= fold;
            if (out != null) out[indices[i]] = (float) v;
        }
        if (!folded) return stateBytes;
        st.lastEventUs = Math.max(st.lastEventUs, t);
        return st.encode(shapes);
    }

    // ------------------------------------------------------------------------------ sticky state
    // A status that holds until it changes (`latest` without a window: card_blocked) must outlive the keyed
    // state's TTL: a blocked card that stays dormant for longer than the TTL must still read blocked. The Flink
    // stage keeps these features' state apart, without expiry, and only while it is not the default, so its
    // size is the number of entities with a non-default status.

    /** Whether any of this entity's features keeps state past the keyed-state TTL. */
    public boolean hasSticky() {
        return anySticky;
    }

    /** Whether events of this type (pipeline ordinal) fold into a sticky feature. */
    public boolean foldsSticky(int type) {
        if (!anySticky) return false;
        for (int i = 0; i < sticky.length; i++) if (sticky[i] && (onMasks[i] & (1L << type)) != 0) return true;
        return false;
    }

    /**
     * The sticky features' state in {@code stateBytes}, compact (feature position, time of the latest change, the
     * value, the value before it), or null when every sticky value is its default: nothing to keep.
     */
    public byte[] stickyState(byte[] stateBytes) {
        if (!anySticky || stateBytes == null) return null;
        State st = State.decode(stateBytes, shapes);
        var bos = new java.io.ByteArrayOutputStream(32);
        int n = 0;
        try (var out = new java.io.DataOutputStream(bos)) {
            out.writeByte(1);
            for (int i = 0; i < sticky.length; i++) {
                Feat f = st.feats[i];
                if (!sticky[i] || f.lastTs == NONE || f.lat == specs.get(i).defaultValue()) continue;
                out.writeShort(i);
                out.writeLong(f.lastTs);
                out.writeDouble(f.lat);
                out.writeDouble(f.lon);
                n++;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return n == 0 ? null : bos.toByteArray();
    }

    /** A state blob holding only the sticky features of {@link #stickyState} (the windows expired with the TTL). */
    public byte[] restoreSticky(byte[] stickyBytes) {
        State st = State.decode(null, shapes);
        try (var in = new DataInputStream(new ByteArrayInputStream(stickyBytes))) {
            if (in.readByte() != 1) return null;
            while (in.available() > 0) {
                int i = in.readUnsignedShort();
                long ts = in.readLong();
                double cur = in.readDouble();
                double before = in.readDouble();
                if (i >= sticky.length || !sticky[i]) return null;      // another layout: start fresh
                st.feats[i].lastTs = ts;
                st.feats[i].lat = cur;
                st.feats[i].lon = before;
                st.lastEventUs = Math.max(st.lastEventUs, ts);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return st.encode(shapes);
    }

    /**
     * Whether a sticky feature holds a non-default value among the entity's values in {@code out} (at their schema
     * indices): its online-store key must then not expire.
     */
    public boolean holdsSticky(float[] out) {
        if (!anySticky || out == null) return false;
        for (int i = 0; i < sticky.length; i++)
            if (sticky[i] && out[indices[i]] != (float) specs.get(i).defaultValue()) return true;
        return false;
    }

    /** Whether events of this type (pipeline ordinal) fold into any of this entity's features. */
    public boolean folds(int type) {
        return (anyMask & (1L << type)) != 0;
    }

    /**
     * Read-only variant of {@link #process}: the feature values {@code e} would get if it were applied
     * to {@code stateBytes} now (update-then-read, exactly the inline semantics), without producing a
     * new state. {@code stateBytes} is never modified. Used by the gateway to re-window an online-store
     * state blob at the event's own time; {@code null} state = an entity never seen before.
     */
    public void evaluate(byte[] stateBytes, TransactionEvent e, long filterMask, float[] out) {
        evaluate(stateBytes, plan.row(e), filterMask, out);
    }

    public void evaluate(byte[] stateBytes, EventRow r, long filterMask, float[] out) {
        evaluate(stateBytes, r, filterMask, out, defaultType);
    }

    /** {@link #evaluate} for an event of type {@code type}: features it does not fold into are read, not updated. */
    public void evaluate(byte[] stateBytes, EventRow r, long filterMask, float[] out, int type) {
        State st = State.decode(stateBytes, shapes);
        long t = r.timeUs;
        long bit = 1L << type;
        for (int i = 0; i < specs.size(); i++) {
            boolean match = matches(i, filterMask, out);
            out[indices[i]] = (float) ((onMasks[i] & bit) != 0
                    ? aggs[i].apply(specs.get(i), st.feats[i], r, slots[i], t, match)
                    : aggs[i].read(specs.get(i), st.feats[i], r, slots[i], t, match));
        }
    }

    /** Whether events of the default type fold into feature {@code i} of this entity (self-inclusion applies). */
    public boolean foldsDefault(int i) {
        return (onMasks[i] & (1L << defaultType)) != 0;
    }

    /**
     * The features of an event that is not folded into state (a re-scored lifecycle stage of a
     * multi-message transaction, already counted when it was assembled), as of its time: windows without
     * it; novelty, time since the last event and speed against the previous events, without recording it.
     * {@code stateBytes} is not modified. Mirrors {@code _read} in python/realy/features/engine.py.
     */
    public void read(byte[] stateBytes, EventRow r, long filterMask, float[] out) {
        State st = State.decode(stateBytes, shapes);
        long t = r.timeUs;
        for (int i = 0; i < specs.size(); i++) {
            boolean match = matches(i, filterMask, out);
            out[indices[i]] = (float) aggs[i].read(specs.get(i), st.feats[i], r, slots[i], t, match);
        }
    }

    /** Re-encode a state blob in the v1 layout (tests: state written by earlier releases). */
    public byte[] toV1(byte[] stateBytes) {
        return State.decode(stateBytes, shapes).encodeV1(shapes);
    }

    /** Decode any supported layout and re-encode in the current one. */
    public byte[] reencode(byte[] stateBytes) {
        return State.decode(stateBytes, shapes).encode(shapes);
    }

    /** Last event time seen in a state blob (the online-store `as_of`). */
    public static long lastEventUs(byte[] stateBytes) {
        if (stateBytes == null || stateBytes.length < 9) return NONE;
        return java.nio.ByteBuffer.wrap(stateBytes, 1, 8).getLong();
    }

    // ------------------------------------------------------------------------------------- state
    /** One feature's state; the {@link Aggregation.Window} its aggregation reads and updates. */
    static final class Feat implements Aggregation.Window {
        long maxK = NONE;
        int size;
        long[] idx = new long[4];
        double[] a = new double[4];
        double[] b = new double[4];
        Map<Long, Long> seen;
        long lastTs = NONE;
        double lat;
        double lon;

        @Override
        public long maxK() {
            return maxK;
        }

        @Override
        public void maxK(long k) {
            maxK = k;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public long bucket(int j) {
            return idx[j];
        }

        @Override
        public double a(int j) {
            return a[j];
        }

        @Override
        public double b(int j) {
            return b[j];
        }

        @Override
        public void addToBucket(long k, double v, java.util.function.DoubleBinaryOperator merge, boolean countMerge) {
            int pos = size;
            while (pos > 0 && idx[pos - 1] > k) pos--;
            if (pos > 0 && idx[pos - 1] == k) {
                int j = pos - 1;
                a[j] = merge.applyAsDouble(a[j], v);
                if (countMerge) b[j] += 1;
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

        @Override
        public void evictBuckets(long upTo) {
            int drop = 0;
            while (drop < size && idx[drop] <= upTo) drop++;
            if (drop == 0) return;
            System.arraycopy(idx, drop, idx, 0, size - drop);
            System.arraycopy(a, drop, a, 0, size - drop);
            System.arraycopy(b, drop, b, 0, size - drop);
            size -= drop;
        }

        @Override
        public Map<Long, Long> seen() {
            return seen;
        }

        @Override
        public void evictSeen(long upTo) {
            for (Iterator<Long> it = seen.values().iterator(); it.hasNext(); ) {
                if (it.next() <= upTo) it.remove();
            }
        }

        @Override
        public long lastTs() {
            return lastTs;
        }

        @Override
        public void lastTs(long t) {
            lastTs = t;
        }

        @Override
        public double lat() {
            return lat;
        }

        @Override
        public double lon() {
            return lon;
        }

        @Override
        public void location(double lat, double lon) {
            this.lat = lat;
            this.lon = lon;
        }
    }

    static final class State {
        Feat[] feats;
        long lastEventUs = NONE;

        static State decode(byte[] bytes, Aggregation.Shape[] specs) {
            State st = new State();
            st.feats = new Feat[specs.length];
            for (int i = 0; i < specs.length; i++) {
                st.feats[i] = new Feat();
                if (specs[i] == Aggregation.Shape.SEEN) st.feats[i].seen = new HashMap<>();
            }
            if (bytes == null || bytes.length == 0) return st;
            if (bytes[0] == STATE_V2) return decodeV2(st, bytes, specs);
            if (bytes[0] != STATE_V1) return new State().fresh(specs);
            try (var in = new DataInputStream(new ByteArrayInputStream(bytes))) {
                in.readByte();
                st.lastEventUs = in.readLong();
                int nf = in.readUnsignedShort();
                if (nf != specs.length) return new State().fresh(specs); // schema changed: reset state
                for (int i = 0; i < nf; i++) {
                    Feat f = st.feats[i];
                    f.maxK = in.readLong();
                    switch (specs[i]) {
                        case BUCKETS -> {
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
                        case SEEN -> {
                            int n = in.readInt();
                            for (int j = 0; j < n; j++) f.seen.put(in.readLong(), in.readLong());
                        }
                        case LAST -> f.lastTs = in.readLong();
                        case LAST_GEO -> {
                            f.lastTs = in.readLong();
                            f.lat = in.readDouble();
                            f.lon = in.readDouble();
                        }
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return st;
        }

        private static State decodeV2(State st, byte[] bytes, Aggregation.Shape[] specs) {
            var in = new Reader(bytes, 1);
            st.lastEventUs = in.fixedLong();
            int nf = (int) in.varLong();
            if (nf != specs.length) return new State().fresh(specs);
            long base = st.lastEventUs;
            for (int i = 0; i < nf; i++) {
                Feat f = st.feats[i];
                long mk = in.varLong();
                f.maxK = mk == 0 ? NONE : unzig(mk - 1);
                switch (specs[i]) {
                    case BUCKETS -> {
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
                    case SEEN -> {
                        int n = (int) in.varLong();
                        long key = 0;
                        for (int j = 0; j < n; j++) {
                            key = j == 0 ? unzig(in.varLong()) : key + in.varLong();
                            f.seen.put(key, base - unzig(in.varLong()));
                        }
                    }
                    case LAST -> f.lastTs = in.optTime(base);
                    case LAST_GEO -> {
                        f.lastTs = in.optTime(base);
                        f.lat = in.rawDouble();
                        f.lon = in.rawDouble();
                    }
                }
            }
            return st;
        }

        private State fresh(Aggregation.Shape[] specs) {
            return decode(null, specs);
        }

        /** The v1 layout (kept to test that state written by earlier releases still decodes identically). */
        byte[] encodeV1(Aggregation.Shape[] specs) {
            var bos = new java.io.ByteArrayOutputStream(64);
            try (var out = new java.io.DataOutputStream(bos)) {
                out.writeByte(STATE_V1);
                out.writeLong(lastEventUs);
                out.writeShort(specs.length);
                for (int i = 0; i < specs.length; i++) {
                    Feat f = feats[i];
                    out.writeLong(f.maxK);
                    switch (specs[i]) {
                        case BUCKETS -> {
                            out.writeInt(f.size);
                            for (int j = 0; j < f.size; j++) {
                                out.writeLong(f.idx[j]);
                                out.writeDouble(f.a[j]);
                                out.writeDouble(f.b[j]);
                            }
                        }
                        case SEEN -> {
                            out.writeInt(f.seen.size());
                            for (var en : f.seen.entrySet()) {
                                out.writeLong(en.getKey());
                                out.writeLong(en.getValue());
                            }
                        }
                        case LAST -> out.writeLong(f.lastTs);
                        case LAST_GEO -> {
                            out.writeLong(f.lastTs);
                            out.writeDouble(f.lat);
                            out.writeDouble(f.lon);
                        }
                    }
                }
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
            return bos.toByteArray();
        }

        byte[] encode(Aggregation.Shape[] specs) {
            var out = new Writer(48 + specs.length * 6);
            out.u8(STATE_VERSION);
            out.fixedLong(lastEventUs);
            out.varLong(specs.length);
            long base = lastEventUs;
            for (int i = 0; i < specs.length; i++) {
                Feat f = feats[i];
                out.varLong(f.maxK == NONE ? 0 : zig(f.maxK) + 1);
                switch (specs[i]) {
                    case BUCKETS -> {
                        out.varLong(f.size);
                        for (int j = 0; j < f.size; j++) {
                            out.varLong(j == 0 ? zig(f.idx[0] - (f.maxK == NONE ? 0 : f.maxK)) : f.idx[j] - f.idx[j - 1]);
                            out.num(f.a[j]);
                            out.num(f.b[j]);
                        }
                    }
                    case SEEN -> {
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
                    case LAST -> out.optTime(f.lastTs, base);
                    case LAST_GEO -> {
                        out.optTime(f.lastTs, base);
                        out.rawDouble(f.lat);
                        out.rawDouble(f.lon);
                    }
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
