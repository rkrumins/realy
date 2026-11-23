package io.realy.scoring.features;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The aggregation registry: the eight built-ins, then every {@link Aggregation} provider on the classpath
 * or in a plugin jar ({@link io.realy.scoring.Plugins}: META-INF/services/io.realy.scoring.features.Aggregation
 * in a jar in PLUGINS_DIR), then any {@link #register}ed in code. A name is registered once. Each needs a
 * Python twin under the same name (python/realy/features/aggregations.py), proved equal by the
 * conformance fixtures ({@link AggregationConformance}).
 */
public final class Aggregations {
    private Aggregations() {}

    private static final long MIN_GEO_DT_US = 60_000_000L;
    private static final Map<String, Aggregation> ALL = new ConcurrentHashMap<>();
    private static final java.util.Set<String> BUILTIN = new java.util.TreeSet<>();

    static {
        for (Aggregation a : builtins()) {
            ALL.put(a.name(), a);
            BUILTIN.add(a.name());
        }
        try {
            for (Aggregation a : io.realy.scoring.Plugins.load(Aggregation.class)) register(a);
        } catch (java.util.ServiceConfigurationError e) {
            throw new IllegalStateException("an aggregation plugin failed to load: " + e.getMessage(), e);
        }
    }

    /** A fresh, empty feature state (what {@link Aggregation#apply} folds into): for plugin tests. */
    public static Aggregation.Window newWindow(Aggregation.Shape shape) {
        EntityAggregator.Feat f = new EntityAggregator.Feat();
        if (shape == Aggregation.Shape.SEEN) f.seen = new java.util.HashMap<>();
        return f;
    }

    /** True for the eight built-ins. */
    public static boolean builtin(String name) {
        return BUILTIN.contains(name);
    }

    /** The registered aggregations that are not built-ins (from plugins or {@link #register}). */
    public static java.util.Set<String> extensions() {
        java.util.Set<String> s = names();
        s.removeAll(BUILTIN);
        return s;
    }

    public static Aggregation get(String name) {
        Aggregation a = ALL.get(name);
        if (a == null) throw new IllegalArgumentException("unknown aggregation " + name + " (registered: " + names() + ")");
        return a;
    }

    public static boolean has(String name) {
        return ALL.containsKey(name);
    }

    public static java.util.Set<String> names() {
        return new java.util.TreeSet<>(ALL.keySet());
    }

    /** Add an aggregation (a plugin, a test); a name already taken is refused. */
    public static void register(Aggregation a) {
        Aggregation prev = ALL.putIfAbsent(a.name(), a);
        if (prev != null && prev != a) throw new IllegalArgumentException("aggregation " + a.name() + " is already registered");
    }

    // ----------------------------------------------------------------------------------- built-ins
    static Iterable<Aggregation> builtins() {
        Map<String, Aggregation> m = new LinkedHashMap<>();
        for (Aggregation a : new Aggregation[] {
                new BucketAgg("count", Aggregation.Input.NONE, Double::sum, true) {
                    @Override
                    protected double contribution(EventRow e, int slot) {
                        return 1.0;
                    }
                },
                new BucketAgg("sum", Aggregation.Input.NUMERIC, Double::sum, true),
                new BucketAgg("avg", Aggregation.Input.NUMERIC, Double::sum, true) {
                    @Override
                    protected double window(FeatureSpec s, double a, double b, double max, boolean any) {
                        return b > 0 ? a / b : s.defaultValue();
                    }
                },
                new BucketAgg("max", Aggregation.Input.NUMERIC, Math::max, false) {
                    @Override
                    protected double window(FeatureSpec s, double a, double b, double max, boolean any) {
                        return any ? max : s.defaultValue();
                    }
                },
                new Distinct(), new Novel(), new TimeSinceLast(), new GeoSpeed()}) {
            m.put(a.name(), a);
        }
        return m.values();
    }

    /**
     * A bucketed aggregation: a matching event adds its contribution to its time bucket (merged with
     * {@code merge}), buckets older than the window are evicted, and the window's buckets are folded into
     * the value (the sum of `a` by default). Subclasses (or plugins: `min`, a weighted sum) override the
     * contribution, the merge and the window fold.
     */
    public static class BucketAgg implements Aggregation {
        private final String name;
        private final Input input;
        private final java.util.function.DoubleBinaryOperator merge;
        private final boolean countMerge;
        private final boolean maxLike;

        public BucketAgg(String name, Input input, java.util.function.DoubleBinaryOperator merge, boolean countMerge) {
            this.name = name;
            this.input = input;
            this.merge = merge;
            this.countMerge = countMerge;
            this.maxLike = !countMerge;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Shape shape() {
            return Shape.BUCKETS;
        }

        @Override
        public Input input() {
            return input;
        }

        /** What a matching event adds (the field's value; 1 for count). */
        protected double contribution(EventRow e, int slot) {
            return e.num[slot];
        }

        /** The value from the window's buckets: Σa, Σb, the fold of `a` by merge, whether any bucket. */
        protected double window(FeatureSpec s, double a, double b, double folded, boolean any) {
            return a;
        }

        private double fold(FeatureSpec s, Window w, long k) {
            long n = s.buckets();
            double a = 0;
            double b = 0;
            double folded = 0;
            boolean any = false;
            for (int j = 0; j < w.size(); j++) {
                long idx = w.bucket(j);
                if (idx > k - n && idx <= k) {
                    if (maxLike) {
                        folded = any ? merge.applyAsDouble(folded, w.a(j)) : w.a(j);
                    } else {
                        a += w.a(j);
                        b += w.b(j);
                    }
                    any = true;
                }
            }
            return window(s, a, b, folded, any);
        }

        @Override
        public double apply(FeatureSpec s, Window w, EventRow e, int slot, long t, boolean match) {
            long k = Math.floorDiv(t, s.granularityUs());
            w.maxK(Math.max(w.maxK(), k));
            if (match) w.addToBucket(k, contribution(e, slot), merge, countMerge);
            w.evictBuckets(w.maxK() - s.buckets());
            return fold(s, w, k);
        }

        @Override
        public double read(FeatureSpec s, Window w, EventRow e, int slot, long t, boolean match) {
            return fold(s, w, Math.floorDiv(t, s.granularityUs()));
        }
    }

    static final class Distinct implements Aggregation {
        @Override
        public String name() {
            return "distinct";
        }

        @Override
        public Shape shape() {
            return Shape.SEEN;
        }

        @Override
        public Input input() {
            return Input.KEY;
        }

        @Override
        public double apply(FeatureSpec s, Window w, EventRow e, int slot, long t, boolean match) {
            long k = Math.floorDiv(t, s.granularityUs());
            long n = s.buckets();
            w.maxK(Math.max(w.maxK(), k));
            if (match) w.seen().merge(e.val[slot], k, Math::max);
            w.evictSeen(w.maxK() - n);
            int c = 0;
            for (long last : w.seen().values()) if (last > k - n) c++;
            return c;
        }

        @Override
        public double read(FeatureSpec s, Window w, EventRow e, int slot, long t, boolean match) {
            long k = Math.floorDiv(t, s.granularityUs());
            long n = s.buckets();
            int c = 0;
            for (long last : w.seen().values()) if (last > k - n) c++;
            return c;
        }
    }

    static final class Novel implements Aggregation {
        @Override
        public String name() {
            return "novel";
        }

        @Override
        public Shape shape() {
            return Shape.SEEN;
        }

        @Override
        public Input input() {
            return Input.KEY;
        }

        @Override
        public double apply(FeatureSpec s, Window w, EventRow e, int slot, long t, boolean match) {
            long k = Math.floorDiv(t, s.granularityUs());
            long n = s.buckets();
            w.maxK(Math.max(w.maxK(), k));
            w.evictSeen(w.maxK() - n);
            if (!match) return 0.0;
            long key = e.val[slot];
            Long last = w.seen().get(key);
            boolean seen = last != null && last > k - n;
            w.seen().merge(key, k, Math::max);
            return seen ? 0.0 : 1.0;
        }

        @Override
        public double read(FeatureSpec s, Window w, EventRow e, int slot, long t, boolean match) {
            if (!match) return 0.0;
            long k = Math.floorDiv(t, s.granularityUs());
            Long last = w.seen().get(e.val[slot]);
            return last != null && last > k - s.buckets() ? 0.0 : 1.0;
        }
    }

    static final class TimeSinceLast implements Aggregation {
        @Override
        public String name() {
            return "time_since_last";
        }

        @Override
        public Shape shape() {
            return Shape.LAST;
        }

        @Override
        public Input input() {
            return Input.NONE;
        }

        @Override
        public double apply(FeatureSpec s, Window w, EventRow e, int slot, long t, boolean match) {
            double v = read(s, w, e, slot, t, match);
            if (match && (w.lastTs() == NONE || t >= w.lastTs())) w.lastTs(t);
            return v;
        }

        @Override
        public double read(FeatureSpec s, Window w, EventRow e, int slot, long t, boolean match) {
            long last = w.lastTs();
            return (last != NONE && t >= last && t - last <= s.windowUs()) ? (t - last) / 1e6 : s.defaultValue();
        }
    }

    static final class GeoSpeed implements Aggregation {
        @Override
        public String name() {
            return "geo_speed_kmh";
        }

        @Override
        public Shape shape() {
            return Shape.LAST_GEO;
        }

        @Override
        public Input input() {
            return Input.LOCATION;
        }

        @Override
        public double apply(FeatureSpec s, Window w, EventRow e, int slot, long t, boolean match) {
            double v = read(s, w, e, slot, t, match);
            if (match && (w.lastTs() == NONE || t >= w.lastTs())) {
                w.lastTs(t);
                w.location(e.lat, e.lon);
            }
            return v;
        }

        @Override
        public double read(FeatureSpec s, Window w, EventRow e, int slot, long t, boolean match) {
            long last = w.lastTs();
            if (match && last != NONE && t >= last && t - last <= s.windowUs()) {
                double km = Geo.haversineKm(w.lat(), w.lon(), e.lat, e.lon);
                return km / (Math.max(t - last, MIN_GEO_DT_US) / 3.6e9);
            }
            return s.defaultValue();
        }
    }
}
