package io.realy.scoring.rules;

import io.realy.scoring.binding.PipelineBinding;
import io.realy.scoring.features.FeatureSchema;
import java.util.List;
import java.util.Map;

/**
 * Named feature filters compiled once and evaluated once per event into a bitmask. A filter applies to the
 * event types in its `event_types` (feature_schema.yaml `{when, event_types}`), a plain one to the pipeline's default type;
 * for any other type its bit is 0, so a payment never evaluates a login's filters and a login never reads a
 * payment's fields.
 */
public final class Filters {
    private final List<Cel.Predicate> predicates;
    private final long[] typeMasks;      // per filter: the type ordinals it applies to
    private final int defaultType;
    private static final Map<String, Double> NO_FEATURES = Map.of();

    public Filters(FeatureSchema schema) {
        this(schema, PipelineBinding.current());
    }

    /** The filters of {@code schema} for the event types of pipeline {@code p}. */
    public Filters(FeatureSchema schema, PipelineBinding p) {
        predicates = schema.filterNames.stream().map(n -> Cel.compile(schema.filters.get(n))).toList();
        defaultType = p.defaultOrdinal();
        typeMasks = new long[schema.filterNames.size()];
        for (int i = 0; i < typeMasks.length; i++) {
            List<String> on = schema.filterOn.get(schema.filterNames.get(i));
            long m = 0;
            if (on == null) {
                m = 1L << defaultType;
            } else {
                for (String t : on) {
                    int o = p.typeOrdinal(t);
                    if (o < 0) throw new IllegalArgumentException("filter " + schema.filterNames.get(i) + ": `event_types` names " + t
                            + ", not an event type of pipeline " + p.name);
                    m |= 1L << o;
                }
            }
            typeMasks[i] = m;
        }
    }

    /** The mask of an event of the pipeline's default type. */
    public long mask(Map<String, Object> eventView) {
        return mask(eventView, defaultType);
    }

    /** The mask of an event of type {@code type} (pipeline ordinal): only the filters that apply to it. */
    public long mask(Map<String, Object> eventView, int type) {
        long mask = 0;
        long bit = 1L << type;
        Map<String, Object> vars = Map.of("event", eventView, "f", NO_FEATURES);
        for (int i = 0; i < predicates.size(); i++) {
            if ((typeMasks[i] & bit) != 0 && predicates.get(i).test(vars)) mask |= 1L << i;
        }
        return mask;
    }
}
