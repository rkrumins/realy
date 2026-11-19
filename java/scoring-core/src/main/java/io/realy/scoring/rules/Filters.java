package io.realy.scoring.rules;

import io.realy.scoring.features.FeatureSchema;
import java.util.List;
import java.util.Map;

/** Named feature filters compiled once and evaluated once per event into a bitmask. */
public final class Filters {
    private final List<Cel.Predicate> predicates;
    private static final Map<String, Double> NO_FEATURES = Map.of();

    public Filters(FeatureSchema schema) {
        predicates = schema.filterNames.stream().map(n -> Cel.compile(schema.filters.get(n))).toList();
    }

    public long mask(Map<String, Object> eventView) {
        long mask = 0;
        Map<String, Object> vars = Map.of("event", eventView, "f", NO_FEATURES);
        for (int i = 0; i < predicates.size(); i++) {
            if (predicates.get(i).test(vars)) mask |= 1L << i;
        }
        return mask;
    }
}
