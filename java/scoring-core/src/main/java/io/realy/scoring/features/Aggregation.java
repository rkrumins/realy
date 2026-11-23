package io.realy.scoring.features;

import java.util.Map;
import java.util.function.DoubleBinaryOperator;

/**
 * An aggregation kind for stream features (feature_schema.yaml `agg:`): the eight built-ins and any
 * registered one (see {@link Aggregations}). An aggregation keeps its state in one of the shared state
 * {@link Shape}s, so a new aggregation needs no new state encoding and existing state blobs are untouched.
 *
 * <p>Semantics, identical in Java and Python (python/realy/features/aggregations.py): {@link #apply} folds a
 * matching event into the window and returns the feature's value with it ("update, then read");
 * {@link #read} returns the value without folding the event (a re-scored lifecycle stage). Every
 * aggregation needs a Python twin registered under the same name, and both must pass the conformance
 * fixtures (contracts/testdata/aggregations), or verify cannot check its features.
 */
public interface Aggregation {
    /** How the per-feature state is kept (and encoded in the entity's state blob). */
    enum Shape {
        /** time buckets (index k = floor(t / granularity), sum `a` and count `b` per bucket) */
        BUCKETS,
        /** keys with the last bucket each was seen in */
        SEEN,
        /** the last matching event's time */
        LAST,
        /** the last matching event's time and location */
        LAST_GEO
    }

    /** What the feature's `value:` names. */
    enum Input {
        NONE,
        /** a numeric field (EventRow.num) */
        NUMERIC,
        /** a field used as a key (EventRow.val) */
        KEY,
        /** the event's location (lat / lon) */
        LOCATION
    }

    String name();

    Shape shape();

    Input input();

    /** Fold a matching event in (when {@code match}), then return the value. */
    double apply(FeatureSpec spec, Window w, EventRow e, int slot, long t, boolean match);

    /** The value as of {@code t}, without folding the event in. */
    double read(FeatureSpec spec, Window w, EventRow e, int slot, long t, boolean match);

    /** Long.MIN_VALUE: no bucket / no event yet. */
    long NONE = Long.MIN_VALUE;

    /** The feature's state, by shape: what an aggregation reads and updates. */
    interface Window {
        // BUCKETS
        long maxK();

        void maxK(long k);

        int size();

        long bucket(int j);

        double a(int j);

        double b(int j);

        /**
         * Merge {@code v} into bucket {@code k} (insert it when absent: a = v, b = 1). {@code merge} combines
         * a with v; {@code countMerge} adds 1 to b when merging (false for max / min, whose b stays 1).
         */
        void addToBucket(long k, double v, DoubleBinaryOperator merge, boolean countMerge);

        void evictBuckets(long upTo);

        // SEEN
        Map<Long, Long> seen();

        void evictSeen(long upTo);

        // LAST, LAST_GEO
        long lastTs();

        void lastTs(long t);

        double lat();

        double lon();

        void location(double lat, double lon);
    }
}
