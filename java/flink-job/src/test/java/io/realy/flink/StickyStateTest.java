package io.realy.flink;

import static org.assertj.core.api.Assertions.assertThat;

import io.realy.contracts.cards.v1.CardStatusChanged;
import io.realy.contracts.payments.v1.CardInfo;
import io.realy.contracts.payments.v1.CardStatus;
import io.realy.contracts.payments.v1.PaymentType;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.scoring.binding.PipelineBinding;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.rules.Filters;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

/**
 * A status that holds until it changes outlives the keyed-state TTL: a card blocked and then left dormant for
 * longer than STATE_TTL_DAYS still reads blocked when it is used again, while its windows (and an unblocked card's
 * whole state) expire as before. Driven through the card stage with Flink's operator harness, which advances the
 * processing time the TTL is measured in.
 */
class StickyStateTest {
    static final PipelineBinding P = PipelineBinding.load("payments-status");
    static final String YAML = bundle();
    static final FeatureSchema S = FeatureSchema.of(YAML);
    static final Filters F = new Filters(S, P);
    static final long T0 = 1_800_000_000_000_000L, MIN = 60_000_000L, DAY = 86_400_000L;

    static String bundle() {
        try {
            return FeatureSchema.bundle("features/feature_schema_status.yaml");
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static Envelope change(long t, long card, CardStatus st) throws Exception {
        byte[] raw = CardStatusChanged.newBuilder().setEventId(t + card).setEventTimeUs(t).setCustomerId(card).setCardId(card)
                .setStatus(st).build().toByteArray();
        return EventDeserializer.standalone(P, S, F, "card_status_changed", raw, null, 1);
    }

    static Envelope pay(long t, long card) {
        TransactionEvent e = TransactionEvent.newBuilder().setEventId(t + card).setEventTimeUs(t).setPaymentType(PaymentType.CARD_PURCHASE)
                .setCustomerId(card).setCardId(card).setCardInfo(CardInfo.newBuilder().setId(card)).setAmountUsdMinor(2_000)
                .setMerchantId(5).setMerchantCountry("GB").build();
        return EventDeserializer.envelope(e, e.toByteArray(), null, false, S, F, 1);
    }

    @Test
    void aBlockedCardOutlivesTheStateTtlAndAnUnblockedOneExpires() throws Exception {
        var fn = new EntityFeatureFunction("card", false, false, false, YAML, List.of(), "payments-status");
        int card = S.rowPlan().entityOrdinal("card");
        try (var h = new KeyedOneInputStreamOperatorTestHarness<Long, Envelope, Envelope>(
                new KeyedProcessOperator<>(fn), e -> e.row.keys[card], Types.LONG)) {
            h.open();
            h.setStateTtlProcessingTime(0);
            h.processElement(new StreamRecord<>(pay(T0, 1)));                                // card 1: used, then blocked
            h.processElement(new StreamRecord<>(change(T0 + MIN, 1, CardStatus.CARD_STATUS_STOLEN)));
            h.processElement(new StreamRecord<>(change(T0 + MIN, 2, CardStatus.CARD_STATUS_BLOCKED)));   // card 2: locked
            h.processElement(new StreamRecord<>(change(T0 + 2 * MIN, 2, CardStatus.CARD_STATUS_ACTIVE))); // ... and unlocked
            h.processElement(new StreamRecord<>(pay(T0 + 3 * MIN, 3)));                      // card 3: only used
            h.getOutput().clear();
            // nine days without any event on these cards: past the 8-day TTL of the windowed state
            h.setStateTtlProcessingTime(9 * DAY);
            long later = T0 + 9 * DAY * 1000;
            h.processElement(new StreamRecord<>(pay(later, 1)));
            h.processElement(new StreamRecord<>(pay(later, 2)));
            h.processElement(new StreamRecord<>(pay(later, 3)));
            List<Envelope> out = h.extractOutputValues();
            assertThat(out).hasSize(3);
            float[] c1 = out.get(0).features, c2 = out.get(1).features, c3 = out.get(2).features;
            int blocked = S.indexOf("card_blocked"), code = S.indexOf("card_status_code"), since = S.indexOf("card_secs_since_status_change");
            assertThat(c1[blocked]).as("a blocked card still reads blocked after the TTL").isEqualTo(1f);
            assertThat(c1[code]).isEqualTo(4f);
            assertThat(c1[since]).as("its windows expired with the TTL").isEqualTo(2_592_000f);
            assertThat(c2[blocked]).as("an unblocked card keeps nothing").isZero();
            assertThat(c2[code]).isEqualTo(1f);
            // the 30-day window outlasts the 9 days in event time, so only the TTL can have cleared it
            assertThat(c2[since]).as("an unblocked card's whole state expired").isEqualTo(2_592_000f);
            assertThat(c3[blocked]).isZero();
        }
    }
}
