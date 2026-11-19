package io.realy.scoring;

import static org.assertj.core.api.Assertions.assertThat;

import io.realy.contracts.payments.v1.EntryMode;
import io.realy.contracts.payments.v1.PaymentType;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.scoring.features.EntityAggregator;
import io.realy.scoring.features.EventView;
import io.realy.scoring.features.FeatureAssembler;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.rules.Filters;
import io.realy.scoring.store.StoreBlob;
import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Test;

/** Exact online-store features: state blobs (StoreBlob v2) re-windowed at read time by the gateway. */
class OnlineStateTest {
    static final FeatureSchema S = FeatureSchema.load();
    static final Filters F = new Filters(S);
    static final long MIN = 60_000_000L;
    static final long T0 = 1_800_000_000_000_000L;

    static TransactionEvent ev(long id, long t, long amount, boolean cardPresent, long merchant, long device) {
        return TransactionEvent.newBuilder().setEventId(id).setEventTimeUs(t).setPaymentType(PaymentType.CARD_PURCHASE)
                .setCustomerId(1).setCardId(10).setAmountUsdMinor(amount).setMerchantId(merchant).setMerchantCountry("GB")
                .setCardPresent(cardPresent).setEntryMode(cardPresent ? EntryMode.CONTACTLESS : EntryMode.ECOM_3DS)
                .setDeviceId(device).setLat(51.5).setLon(-0.1).build();
    }

    static long mask(TransactionEvent e) {
        return F.mask(EventView.of(e));
    }

    @Test
    void evaluateEqualsProcessAndNeverTouchesTheState() {
        Random r = new Random(7);
        for (String entity : new String[] {"customer", "card"}) {
            var a = new EntityAggregator(S, entity);
            byte[] state = null;
            long t = T0;
            for (int i = 0; i < 400; i++) {
                t += (long) (r.nextExponential() * 3 * MIN);
                var e = ev(i + 1, t, 50 + r.nextInt(30_000), r.nextBoolean(), r.nextInt(12), r.nextInt(4));
                byte[] before = state == null ? null : state.clone();
                float[] viaEvaluate = new float[S.size()];
                float[] viaProcess = new float[S.size()];
                a.evaluate(state, e, mask(e), viaEvaluate);
                assertThat(state).isEqualTo(before);
                state = a.process(state, e, mask(e), viaProcess);
                for (int idx : a.featureIndices()) {
                    assertThat(viaEvaluate[idx]).as("%s %s event %d", entity, S.features.get(idx).name(), i)
                            .isEqualTo(viaProcess[idx]);
                }
            }
        }
    }

    /** The gateway reads a blob Flink wrote at the card's previous event, 20 minutes later. */
    @Test
    void staleStateBlobIsRewindowedExactlyWhileAValuesBlobIsNot() {
        var card = new EntityAggregator(S, "card");
        var asm = new FeatureAssembler(S);
        byte[] state = null;
        float[] tmp = new float[S.size()];
        TransactionEvent last = null;
        for (int i = 0; i < 5; i++) {
            last = ev(i + 1, T0 + i * MIN, 150, false, 7, 0);
            state = card.process(state, last, mask(last), tmp);
        }
        int[] idx = card.featureIndices();
        float[] storedValues = new float[idx.length];
        for (int i = 0; i < idx.length; i++) storedValues[i] = tmp[idx[i]];

        var now = ev(99, T0 + 25 * MIN, 150, false, 7, 0);   // the 5 earlier events left the 10m window
        float[] exact = new float[S.size()];
        card.process(state, now, mask(now), exact);         // what Flink-inline computes

        var v2 = new StoreBlob(S.version, last.getEventTimeUs(), 0, storedValues, last.getEventId(), state).encode();
        var v1 = new StoreBlob(S.version, last.getEventTimeUs(), 0, storedValues).encode();
        float[] fromState = assembleCard(asm, now, v2);
        float[] fromValues = assembleCard(asm, now, v1);

        int c10 = S.indexOf("card_small_cnp_count_10m");
        assertThat(exact[c10]).isEqualTo(1f);
        assertThat(fromState[c10]).isEqualTo(1f);
        assertThat(fromValues[c10]).isEqualTo(6f);           // the staleness v2 removes: 5 expired + current
        for (int i : idx) assertThat(fromState[i]).as(S.features.get(i).name()).isEqualTo(exact[i]);
    }

    /** Flink already applied this event (it was ahead of the gateway): serve the stored values as-is. */
    @Test
    void blobThatAlreadyIncludesTheEventServesItsStoredValues() {
        var card = new EntityAggregator(S, "card");
        var asm = new FeatureAssembler(S);
        byte[] state = null;
        float[] out = new float[S.size()];
        TransactionEvent e = null;
        for (int i = 0; i < 3; i++) {
            e = ev(i + 1, T0 + i * MIN, 150, false, 7, 0);
            state = card.process(state, e, mask(e), out);
        }
        int[] idx = card.featureIndices();
        float[] vals = new float[idx.length];
        for (int i = 0; i < idx.length; i++) vals[i] = out[idx[i]];
        float[] served = assembleCard(asm, e, new StoreBlob(S.version, e.getEventTimeUs(), 0, vals, e.getEventId(), state).encode());
        for (int i = 0; i < idx.length; i++) assertThat(served[idx[i]]).isEqualTo(vals[i]);
        assertThat(served[S.indexOf("card_small_cnp_count_10m")]).isEqualTo(3f);    // not double-counted
    }

    @Test
    void entityTheStoreHasNeverSeenGetsFirstEventValues() {
        var card = new EntityAggregator(S, "card");
        var asm = new FeatureAssembler(S);
        var e = ev(1, T0, 150, false, 7, 0);
        float[] exact = new float[S.size()];
        card.process(null, e, mask(e), exact);
        float[] served = assembleCard(asm, e, null);
        for (int i : card.featureIndices()) assertThat(served[i]).as(S.features.get(i).name()).isEqualTo(exact[i]);
    }

    @Test
    void blobFormatRoundTripsAndStaysReadableAsValues() {
        float[] vals = {1f, 2.5f, 3f};
        byte[] state = {9, 8, 7, 6};
        var v2 = new StoreBlob(1, 11, 22, vals, 33, state);
        var back = StoreBlob.decode(v2.encode());
        assertThat(back.hasState()).isTrue();
        assertThat(back.schemaVersion()).isEqualTo(1);
        assertThat(back.lastEventId()).isEqualTo(33);
        assertThat(back.state()).isEqualTo(state);
        assertThat(back.values()).isEqualTo(vals);
        assertThat(v2.encode()).hasSize(v2.encodedSize());
        // a truncated state section degrades to the values (never a wrong state)
        byte[] cut = Arrays.copyOf(v2.encode(), v2.encode().length - 2);
        assertThat(StoreBlob.decode(cut).hasState()).isFalse();
        assertThat(StoreBlob.decode(cut).values()).isEqualTo(vals);
        var v1 = StoreBlob.decode(new StoreBlob(1, 11, 22, vals).encode());
        assertThat(v1.hasState()).isFalse();
        assertThat(v1.values()).isEqualTo(vals);
    }

    /** Assemble with only the card's stream blob present (others absent but looked up). */
    private static float[] assembleCard(FeatureAssembler asm, TransactionEvent e, byte[] cardBlob) {
        var lk = asm.lookupKeys(e, true);
        byte[][] blobs = new byte[lk.keys().size()][];
        for (int i = 0; i < blobs.length; i++) {
            if (lk.keys().get(i).equals(StoreBlob.streamKey("card", e.getCardId()))) blobs[i] = cardBlob;
        }
        return asm.assemble(e, mask(e), null, lk, blobs, true).values();
    }
}
