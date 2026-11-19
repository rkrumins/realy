package io.realy.scoring.features;

import io.realy.contracts.features.v1.Entity;
import io.realy.contracts.features.v1.EntityFreshness;
import io.realy.contracts.features.v1.FeatureSource;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.scoring.store.StoreBlob;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds the model input vector in canonical schema order from (a) stream features — computed
 * in-process by Flink (inline topology) or read from the online store (gateway topology), (b) profile
 * features from the online store, and (c) derived features. Records missing bits and per-entity
 * freshness for the audit record.
 */
public final class FeatureAssembler {
    public record Lookup(List<String> keys, List<String> entities, List<Boolean> profile) {}

    public record Assembled(float[] values, byte[] missing, List<EntityFreshness> freshness,
                            DerivedFeatures.CustomerAttrs attrs) {}

    private final FeatureSchema schema;
    private final List<EntityAggregator> aggregators = new ArrayList<>();
    private final int[] custAttrIdx; // positions of home_lat, home_lon, home_country_idx, tz_offset_min in customer profile blob

    public FeatureAssembler(FeatureSchema schema) {
        this.schema = schema;
        for (String e : schema.streamByEntity().keySet()) aggregators.add(new EntityAggregator(schema, e));
        List<String> attrs = schema.profileAttributes.getOrDefault("customer", List.of());
        int base = schema.profile("customer").size();
        custAttrIdx = new int[] {pos(attrs, "home_lat", base), pos(attrs, "home_lon", base),
                pos(attrs, "home_country_idx", base), pos(attrs, "tz_offset_min", base)};
    }

    private static int pos(List<String> attrs, String name, int base) {
        int i = attrs.indexOf(name);
        return i < 0 ? -1 : base + i;
    }

    public FeatureSchema schema() {
        return schema;
    }

    public List<EntityAggregator> aggregators() {
        return aggregators;
    }

    /** Keys to fetch: stream blobs (unless precomputed) then customer/merchant profile blobs. */
    public Lookup lookupKeys(TransactionEvent e, boolean includeStream) {
        List<String> keys = new ArrayList<>(10);
        List<String> ents = new ArrayList<>(10);
        List<Boolean> prof = new ArrayList<>(10);
        if (includeStream) {
            for (EntityAggregator a : aggregators) {
                long id = EventView.entityKey(e, a.entity());
                if (id == 0) continue;
                keys.add(StoreBlob.streamKey(a.entity(), id));
                ents.add(a.entity());
                prof.add(false);
            }
        }
        for (String ent : schema.profileByEntity().keySet()) {
            long id = EventView.entityKey(e, ent);
            if (id == 0) continue;
            keys.add(StoreBlob.profileKey(ent, id));
            ents.add(ent);
            prof.add(true);
        }
        return new Lookup(keys, ents, prof);
    }

    /**
     * @param precomputed  full-length vector whose stream-feature slots are already filled (Flink
     *                     inline), or null to take stream features from {@code blobs}
     * @param selfInclusion add the current event to count/sum/max aggregates read from the store
     */
    public Assembled assemble(TransactionEvent e, long filterMask, float[] precomputed, Lookup lk, byte[][] blobs,
                              boolean selfInclusion) {
        int n = schema.size();
        float[] v = precomputed != null ? precomputed.clone() : new float[n];
        byte[] missing = new byte[(n + 7) / 8];
        List<EntityFreshness> fresh = new ArrayList<>(10);
        long t = e.getEventTimeUs();

        if (precomputed == null) {
            for (EntityAggregator a : aggregators) {
                StoreBlob blob = null;
                long id = EventView.entityKey(e, a.entity());
                int bi = id == 0 ? -1 : find(lk, a.entity(), false);
                if (bi >= 0 && blobs != null && blobs[bi] != null) blob = StoreBlob.decode(blobs[bi]);
                if (id == 0) {
                    for (int idx : a.featureIndices()) {
                        v[idx] = (float) schema.features.get(idx).defaultValue();
                        setBit(missing, idx);
                    }
                    continue;
                }
                fillStream(a, blob, e, filterMask, selfInclusion, t, v, missing);
                fresh.add(freshness(a.entity(), FeatureSource.STREAM, blob));
            }
        }
        DerivedFeatures.CustomerAttrs attrs = null;
        for (var en : schema.profileByEntity().entrySet()) {
            String ent = en.getKey();
            int bi = find(lk, ent, true);
            StoreBlob blob = bi >= 0 && blobs != null && blobs[bi] != null ? StoreBlob.decode(blobs[bi]) : null;
            List<FeatureSpec> specs = en.getValue();
            for (int i = 0; i < specs.size(); i++) {
                FeatureSpec s = specs.get(i);
                if (blob != null && i < blob.values().length) {
                    v[s.index()] = blob.values()[i];
                } else {
                    v[s.index()] = (float) s.defaultValue();
                    setBit(missing, s.index());
                }
            }
            if ("customer".equals(ent) && blob != null && custAttrIdx[0] >= 0 && custAttrIdx[3] < blob.values().length) {
                float[] b = blob.values();
                attrs = new DerivedFeatures.CustomerAttrs(b[custAttrIdx[0]], b[custAttrIdx[1]], (int) b[custAttrIdx[2]],
                        (int) b[custAttrIdx[3]]);
            }
            if (EventView.entityKey(e, ent) != 0) fresh.add(freshness(ent, FeatureSource.PROFILE, blob));
        }
        DerivedFeatures.compute(schema, e, attrs, v);
        return new Assembled(v, missing, fresh, attrs);
    }

    private void fillStream(EntityAggregator a, StoreBlob blob, TransactionEvent e, long mask, boolean selfInclusion,
                            long t, float[] v, byte[] missing) {
        int[] idx = a.featureIndices();
        List<FeatureSpec> specs = a.specs();
        boolean includesCurrent = blob != null && blob.asOfUs() >= t;
        for (int i = 0; i < idx.length; i++) {
            FeatureSpec s = specs.get(i);
            boolean have = blob != null && i < blob.values().length;
            double val = have ? blob.values()[i] : s.defaultValue();
            if (selfInclusion && !includesCurrent) {
                boolean match = s.filter() == null || (mask & (1L << schema.filterBit(s.filter()))) != 0;
                double base = have ? val : 0.0;
                switch (s.agg()) {
                    case "count" -> { val = base + (match ? 1 : 0); have = true; }
                    case "sum" -> { val = base + (match ? EventView.numeric(e, s.value()) : 0); have = true; }
                    case "max" -> { if (match) { val = Math.max(have ? val : 0, EventView.numeric(e, s.value())); have = true; } }
                    case "distinct", "novel" -> { if (!have) { val = match ? 1 : 0; have = true; } }
                    case "time_since_last" -> {
                        if (blob != null && s.filter() == null && t >= blob.asOfUs() && t - blob.asOfUs() <= s.windowUs()) {
                            val = (t - blob.asOfUs()) / 1e6;
                            have = true;
                        }
                    }
                    default -> { }
                }
            }
            v[idx[i]] = (float) val;
            if (!have) setBit(missing, idx[i]);
        }
    }

    private static int find(Lookup lk, String entity, boolean profile) {
        if (lk == null) return -1;
        for (int i = 0; i < lk.keys().size(); i++) {
            if (lk.entities().get(i).equals(entity) && lk.profile().get(i) == profile) return i;
        }
        return -1;
    }

    private static EntityFreshness freshness(String entity, FeatureSource src, StoreBlob blob) {
        var b = EntityFreshness.newBuilder().setEntity(entityEnum(entity)).setSource(src).setFound(blob != null);
        if (blob != null) b.setAsOfUs(blob.asOfUs()).setUpdatedAtUs(blob.updatedAtUs());
        return b.build();
    }

    public static Entity entityEnum(String e) {
        return switch (e) {
            case "customer" -> Entity.CUSTOMER;
            case "card" -> Entity.CARD;
            case "device" -> Entity.DEVICE;
            case "ip" -> Entity.IP;
            case "merchant" -> Entity.MERCHANT;
            case "terminal" -> Entity.TERMINAL;
            case "payee" -> Entity.PAYEE;
            default -> Entity.ENTITY_UNSPECIFIED;
        };
    }

    static void setBit(byte[] bm, int i) {
        bm[i >> 3] |= (byte) (1 << (i & 7));
    }
}
