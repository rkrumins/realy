package io.realy.scoring.features;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The derived-function registry: the built-in library names ({@link DerivedFeatures}), then every
 * {@link DerivedFn} provider on the classpath or in a plugin jar ({@link io.realy.scoring.Plugins},
 * META-INF/services/io.realy.scoring.features.DerivedFn), then any {@link #register}ed in code.
 */
public final class DerivedFns {
    private DerivedFns() {}

    /** The library functions DerivedFeatures computes itself (typed, on the event message). */
    public static final Set<String> BUILTIN = Set.of("amount_usd", "is_cnp", "is_contactless", "is_atm", "is_transfer",
            "is_foreign", "dist_from_home_km", "amount_to_avg_ratio", "local_hour", "credit_utilisation_24h");

    private static final Map<String, DerivedFn> ALL = new ConcurrentHashMap<>();

    static {
        try {
            for (DerivedFn fn : io.realy.scoring.Plugins.load(DerivedFn.class)) register(fn);
        } catch (java.util.ServiceConfigurationError e) {
            throw new IllegalStateException("a derived-function plugin failed to load: " + e.getMessage(), e);
        }
    }

    public static boolean builtin(String name) {
        return BUILTIN.contains(name);
    }

    /** True for a built-in or a registered function. */
    public static boolean has(String name) {
        return BUILTIN.contains(name) || ALL.containsKey(name);
    }

    /** A registered (non-built-in) function. */
    public static DerivedFn get(String name) {
        DerivedFn fn = ALL.get(name);
        if (fn == null) throw new IllegalArgumentException("unknown derived fn " + name + " (registered: " + names() + ")");
        return fn;
    }

    public static Set<String> names() {
        Set<String> s = new TreeSet<>(BUILTIN);
        s.addAll(ALL.keySet());
        return s;
    }

    /** The registered functions that are not built-ins (plugins). */
    public static Set<String> extensions() {
        return new TreeSet<>(ALL.keySet());
    }

    public static void register(DerivedFn fn) {
        if (BUILTIN.contains(fn.name())) throw new IllegalArgumentException("derived fn " + fn.name() + " is a built-in");
        DerivedFn prev = ALL.putIfAbsent(fn.name(), fn);
        if (prev != null && prev != fn) throw new IllegalArgumentException("derived fn " + fn.name() + " is already registered");
    }
}
