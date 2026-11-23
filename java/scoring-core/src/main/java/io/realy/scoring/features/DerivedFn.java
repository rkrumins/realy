package io.realy.scoring.features;

import java.util.Map;

/**
 * A derived-feature function for {@code source: derived, fn: <name>} features: computed at scoring time
 * (the gateway, Flink inline) from the event and the features computed before it. The built-in library
 * ({@link DerivedFeatures}) and any registered function ({@link DerivedFns}: a plugin jar in PLUGINS_DIR)
 * share the `fn:` namespace.
 *
 * <p>It sees what a CEL expression feature sees: {@code event}, the event's view (every field by name, enums
 * as their names, integers as Long), and {@code f}, every feature computed before it in schema order. Use a
 * function where CEL is not enough (trigonometry, a lookup table, a small model). Every function needs a
 * Python twin under the same name (python/realy/features/derived_fns.py), proved equal by a conformance
 * fixture ({@link DerivedFnConformance}).
 */
public interface DerivedFn {
    String name();

    double apply(Map<String, Object> event, Map<String, Double> f);
}
