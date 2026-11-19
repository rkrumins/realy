package io.realy.scoring;

import static org.assertj.core.api.Assertions.assertThat;

import io.realy.contracts.payments.v1.EntryMode;
import io.realy.contracts.payments.v1.PaymentType;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.scoring.features.EntityAggregator;
import io.realy.scoring.features.EventView;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.rules.Filters;
import org.junit.jupiter.api.Test;

class EntityAggregatorTest {
    static final FeatureSchema S = FeatureSchema.load();
    static final Filters F = new Filters(S);
    static final long MIN = 60_000_000L;

    static TransactionEvent ev(long t, long amount, boolean cardPresent, long merchant, double lat, double lon) {
        return TransactionEvent.newBuilder().setEventId(t).setEventTimeUs(t).setPaymentType(PaymentType.CARD_PURCHASE)
                .setCustomerId(1).setCardId(10).setAmountUsdMinor(amount).setMerchantId(merchant).setMerchantCountry("GB")
                .setCardPresent(cardPresent).setEntryMode(cardPresent ? EntryMode.CHIP : EntryMode.ECOM_3DS)
                .setLat(lat).setLon(lon).build();
    }

    float[] run(EntityAggregator a, byte[][] state, TransactionEvent e) {
        float[] out = new float[S.size()];
        state[0] = a.process(state[0], e, F.mask(EventView.of(e)), out);
        return out;
    }

    @Test
    void countsSlideOutOfWindowAndCardTestingFeatureCounts() {
        var card = new EntityAggregator(S, "card");
        byte[][] st = new byte[1][];
        long t0 = 1_800_000_000_000_000L;
        int c10 = S.indexOf("card_small_cnp_count_10m");
        int c5 = S.indexOf("card_count_5m");
        float[] o = null;
        for (int i = 0; i < 4; i++) o = run(card, st, ev(t0 + i * MIN, 150, false, 7, 0, 0));
        assertThat(o[c10]).isEqualTo(4f);
        assertThat(o[c5]).isEqualTo(4f);
        o = run(card, st, ev(t0 + 20 * MIN, 150, false, 7, 0, 0)); // older ones out of 10m window
        assertThat(o[c10]).isEqualTo(1f);
        o = run(card, st, ev(t0 + 21 * MIN, 5000, false, 8, 0, 0)); // not small -> filter miss
        assertThat(o[c10]).isEqualTo(1f);
        assertThat(o[S.indexOf("card_distinct_merchants_10m")]).isEqualTo(2f);
        assertThat(o[S.indexOf("card_amount_sum_1h")]).isEqualTo(5 * 150f + 5000f);
        assertThat(o[S.indexOf("card_secs_since_last")]).isEqualTo(60f);
    }

    @Test
    void geoSpeedDetectsImpossibleTravel() {
        var card = new EntityAggregator(S, "card");
        byte[][] st = new byte[1][];
        long t0 = 1_800_000_000_000_000L;
        int g = S.indexOf("card_geo_speed_kmh");
        assertThat(run(card, st, ev(t0, 2000, true, 1, 51.5, -0.12))[g]).isEqualTo(0f); // London
        float v = run(card, st, ev(t0 + 30 * MIN, 2000, true, 2, 40.71, -74.0))[g];     // New York 30 min later
        assertThat(v).isGreaterThan(9000f);
        assertThat(run(card, st, ev(t0 + 40 * MIN, 2000, false, 2, 0, 0))[g]).isEqualTo(0f); // CNP: no geo
    }

    @Test
    void novelDeviceOnlyFirstTime() {
        var cust = new EntityAggregator(S, "customer");
        byte[][] st = new byte[1][];
        long t0 = 1_800_000_000_000_000L;
        int nov = S.indexOf("cust_device_novel");
        var b = ev(t0, 100, false, 1, 0, 0).toBuilder().setDeviceId(99);
        float[] out = new float[S.size()];
        st[0] = cust.process(st[0], b.build(), F.mask(EventView.of(b.build())), out);
        assertThat(out[nov]).isEqualTo(1f);
        var e2 = b.setEventTimeUs(t0 + MIN).build();
        st[0] = cust.process(st[0], e2, F.mask(EventView.of(e2)), out);
        assertThat(out[nov]).isEqualTo(0f);
        var e3 = b.setEventTimeUs(t0 + 2 * MIN).setDeviceId(100).build();
        st[0] = cust.process(st[0], e3, F.mask(EventView.of(e3)), out);
        assertThat(out[nov]).isEqualTo(1f);
        assertThat(EntityAggregator.lastEventUs(st[0])).isEqualTo(t0 + 2 * MIN);
    }
}
