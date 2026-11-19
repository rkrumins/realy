package io.realy.scoring.features;

import io.realy.contracts.payments.v1.EntryMode;
import io.realy.contracts.payments.v1.PaymentType;
import io.realy.contracts.payments.v1.TransactionEvent;

/**
 * Fixed library of scoring-time derived features (feature_schema.yaml `fn`). Mirrored by
 * python/realy/features/derived.py. `profile` holds customer profile attributes (may be null).
 */
public final class DerivedFeatures {
    private DerivedFeatures() {}

    /** Customer profile attributes needed by derived functions. */
    public record CustomerAttrs(double homeLat, double homeLon, int homeCountryIdx, int tzOffsetMin) {}

    /**
     * Compute all derived features into {@code v} (other features must already be filled: some
     * derived functions read stream/profile features from {@code v}).
     */
    public static void compute(FeatureSchema schema, TransactionEvent e, CustomerAttrs attrs, float[] v) {
        for (FeatureSpec s : schema.features) {
            if (!s.isDerived()) continue;
            v[s.index()] = (float) value(schema, s.fn(), e, attrs, v);
        }
    }

    static double value(FeatureSchema schema, String fn, TransactionEvent e, CustomerAttrs a, float[] v) {
        return switch (fn) {
            case "amount_usd" -> e.getAmountUsdMinor() / 100.0;
            case "is_cnp" -> e.getPaymentType() == PaymentType.CARD_PURCHASE && !e.getCardPresent() ? 1.0 : 0.0;
            case "is_contactless" -> e.getEntryMode() == EntryMode.CONTACTLESS ? 1.0 : 0.0;
            case "is_atm" -> e.getPaymentType() == PaymentType.ATM_WITHDRAWAL ? 1.0 : 0.0;
            case "is_transfer" -> e.getPaymentType() == PaymentType.TRANSFER ? 1.0 : 0.0;
            case "is_foreign" -> {
                if (a == null || e.getMerchantCountry().isEmpty()) yield 0.0;
                int idx = schema.countryIndex(e.getMerchantCountry());
                yield idx >= 0 && idx != a.homeCountryIdx() ? 1.0 : 0.0;
            }
            case "dist_from_home_km" -> {
                if (a == null || (e.getLat() == 0.0 && e.getLon() == 0.0)) yield 0.0;
                yield Geo.haversineKm(a.homeLat(), a.homeLon(), e.getLat(), e.getLon());
            }
            case "amount_to_avg_ratio" -> {
                int i = indexOrNeg(schema, "cust_avg_ticket_usd");
                double avg = i >= 0 ? v[i] : 40.0;
                yield (e.getAmountUsdMinor() / 100.0) / Math.max(avg, 1.0);
            }
            case "local_hour" -> {
                long secs = Math.floorDiv(e.getEventTimeUs(), 1_000_000L) + (a == null ? 0 : a.tzOffsetMin() * 60L);
                yield Math.floorMod(secs, 86_400L) / 3600.0;
            }
            case "credit_utilisation_24h" -> {
                int lim = e.getCreditLimitUsd();
                int i = indexOrNeg(schema, "card_amount_sum_24h");
                if (lim <= 0 || i < 0) yield 0.0;
                yield (v[i] / 100.0) / lim;
            }
            default -> throw new IllegalArgumentException("unknown derived fn " + fn);
        };
    }

    private static int indexOrNeg(FeatureSchema schema, String name) {
        try {
            return schema.indexOf(name);
        } catch (IllegalArgumentException ex) {
            return -1;
        }
    }
}
