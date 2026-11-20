package io.realy.scoring.features;

/** One entry of contracts/features/feature_schema.yaml. */
public record FeatureSpec(
        int index,
        String name,
        String source,       // stream | profile | derived
        String entity,       // stream/profile
        String agg,          // count|sum|max|avg|distinct|novel|time_since_last|geo_speed_kmh
        String value,        // event field for sum/max/avg/distinct/novel
        String filter,       // named filter or null
        long windowUs,
        long granularityUs,
        double defaultValue,
        String field,        // profile field
        String fn,           // derived: a library function (DerivedFeatures), or null when `expr` is used
        io.realy.scoring.rules.Cel.Numeric expr) {   // derived: a CEL expression over `event` and `f`

    public boolean isStream() {
        return "stream".equals(source);
    }

    public boolean isProfile() {
        return "profile".equals(source);
    }

    public boolean isDerived() {
        return "derived".equals(source);
    }

    public int buckets() {
        return granularityUs == 0 ? 0 : (int) (windowUs / granularityUs);
    }
}
