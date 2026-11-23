package com.example.pulsebench;

import io.realy.scoring.features.Aggregation;
import io.realy.scoring.features.Aggregations;
import io.realy.scoring.features.FeatureSpec;

/**
 * An example plugin aggregation: the smallest value in the window ({@code agg: min}, {@code value:} a
 * numeric field). It reuses the bucket state shape, so the state codec, eviction and windowing are the
 * built-ins'; only the merge (Math.min) and the window fold differ. Its Python twin is
 * python/realy/plugin_example.py, and both pass contracts/testdata/aggregations/min.json.
 *
 * <p>Registered by META-INF/services/io.realy.scoring.features.Aggregation.
 */
public final class MinAggregation extends Aggregations.BucketAgg {
    public MinAggregation() {
        super("min", Aggregation.Input.NUMERIC, Math::min, false);
    }

    @Override
    protected double window(FeatureSpec s, double a, double b, double folded, boolean any) {
        return any ? folded : s.defaultValue();
    }
}
