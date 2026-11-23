package io.realy.scoring;

import static org.assertj.core.api.Assertions.assertThat;

import io.realy.contracts.payments.v1.Channel;
import io.realy.contracts.payments.v1.EntryMode;
import io.realy.contracts.payments.v1.PaymentType;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.scoring.features.EntityAggregator;
import io.realy.scoring.features.EventView;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.rules.Filters;
import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * The compact (v2) state layout: exact round trips, identical features whether the previous state was
 * written as v1 (earlier releases, RocksDB state restored from old savepoints) or v2, and smaller blobs.
 */
class StateCodecTest {
    static final FeatureSchema S = FeatureSchema.load();
    static final Filters F = new Filters(S);

    static TransactionEvent event(Random r, long t, long customer, long card) {
        boolean transfer = r.nextDouble() < 0.15;
        boolean cp = !transfer && r.nextDouble() < 0.5;
        var b = TransactionEvent.newBuilder().setEventId(t ^ customer).setEventTimeUs(t).setCustomerId(customer)
                .setAmountUsdMinor(50 + r.nextInt(250_000)).setMerchantCountry(r.nextDouble() < 0.9 ? "GB" : "FR")
                .setLat(51.5 + r.nextGaussian()).setLon(-0.1 + r.nextGaussian()).setIpCountry(r.nextDouble() < 0.95 ? "GB" : "US");
        if (transfer) {
            b.setPaymentType(PaymentType.TRANSFER).setChannel(Channel.ONLINE_BANKING).setPayeeId(6L << 40 | r.nextInt(5))
                    .setDeviceId(3L << 40 | r.nextInt(3));
        } else {
            b.setPaymentType(PaymentType.CARD_PURCHASE).setCardId(card).setMerchantId(4L << 40 | r.nextInt(40))
                    .setCardPresent(cp).setEntryMode(cp ? EntryMode.CONTACTLESS : EntryMode.ECOM_3DS)
                    .setChannel(cp ? Channel.POS : Channel.ECOM).setCvvMatch(r.nextDouble() < 0.97);
            if (!cp) b.setDeviceId(3L << 40 | r.nextInt(3)).setIpV4(0x0A000000 | r.nextInt(1000));
        }
        return b.build();
    }

    @Test
    void v1AndV2StatesGiveIdenticalFeaturesAndV2IsSmaller() {
        long v1Bytes = 0, v2Bytes = 0, blobs = 0;
        for (String entity : S.streamByEntity().keySet()) {
            var agg = new EntityAggregator(S, entity);
            Random r = new Random(entity.hashCode());
            for (int k = 0; k < 40; k++) {                       // 40 entities, a few days of activity each
                byte[] viaV2 = null, viaV1 = null;
                long t = 1_800_000_000_000_000L + r.nextInt(1_000_000);
                for (int i = 0; i < 120; i++) {
                    t += (long) (r.nextDouble() * (r.nextDouble() < 0.3 ? 60e6 : 4 * 3600e6));
                    TransactionEvent e = event(r, t, 1 + k, 2L << 40 | k);
                    long mask = F.mask(EventView.of(e));
                    float[] o2 = new float[S.size()], o1 = new float[S.size()];
                    viaV2 = agg.process(viaV2, e, mask, o2);
                    viaV1 = agg.process(viaV1 == null ? null : agg.toV1(viaV1), e, mask, o1);
                    assertThat(o1).as("%s event %d", entity, i).containsExactly(o2);
                    assertThat(viaV2[0]).isEqualTo((byte) 2);
                    assertThat(agg.reencode(viaV2)).isEqualTo(viaV2);              // exact round trip
                    assertThat(agg.reencode(agg.toV1(viaV2))).isEqualTo(viaV2);    // v1 -> v2 loses nothing
                    assertThat(EntityAggregator.lastEventUs(viaV2)).isEqualTo(EntityAggregator.lastEventUs(agg.toV1(viaV2)));
                }
                v1Bytes += agg.toV1(viaV2).length;
                v2Bytes += viaV2.length;
                blobs++;
            }
        }
        System.out.printf("state blobs: v1 %.0f B, v2 %.0f B on average (%d blobs)%n",
                v1Bytes / (double) blobs, v2Bytes / (double) blobs, blobs);
        assertThat(v2Bytes).isLessThan(v1Bytes / 2);
    }

    @Test
    void specialValuesRoundTripExactly() {
        var agg = new EntityAggregator(S, "customer");
        Random r = new Random(7);
        byte[] st = null;
        long t = 1_800_000_000_000_000L;
        for (int i = 0; i < 30; i++) {
            t += 3_600_000_000L;
            st = agg.process(st, event(r, t, 9, 2L << 40 | 9), 0L, new float[S.size()]);
        }
        byte[] again = agg.reencode(st);
        assertThat(Arrays.equals(again, st)).isTrue();
        assertThat(agg.reencode(new byte[] {9, 1, 2})).isEqualTo(agg.reencode(null));   // unknown layout: fresh state
    }
}
