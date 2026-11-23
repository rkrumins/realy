package io.realy.scoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.realy.contracts.cards.v1.CardStatusChanged;
import io.realy.contracts.payments.v1.CardInfo;
import io.realy.contracts.payments.v1.CardStatus;
import io.realy.contracts.payments.v1.PaymentType;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.scoring.binding.PipelineBinding;
import io.realy.scoring.features.EntityAggregator;
import io.realy.scoring.features.EventRow;
import io.realy.scoring.features.EventView;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.rules.Filters;
import org.junit.jupiter.api.Test;

/**
 * Entity status from change feeds (pipeline payments-status): `latest` features fold a card's status changes, a
 * payment reads the status as of its time, a `gate:` makes a later stage's feature depend on it, and the sticky
 * part of the state (a status held until it changes) is extracted and restored on its own.
 */
class StatusFeaturesTest {
    static final PipelineBinding P = PipelineBinding.load("payments-status");
    static final FeatureSchema S = schema();
    static final Filters F = new Filters(S, P);
    static final long T0 = 1_800_000_000_000_000L, MIN = 60_000_000L;
    static final int CHANGE = P.typeOrdinal("card_status_changed"), PAY = P.typeOrdinal("payment");

    static FeatureSchema schema() {
        try {
            return FeatureSchema.parse(FeatureSchema.bundle("features/feature_schema_status.yaml"));
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static TransactionEvent pay(long t) {
        return TransactionEvent.newBuilder().setEventId(t).setEventTimeUs(t).setPaymentType(PaymentType.CARD_PURCHASE)
                .setCustomerId(1).setCardId(10).setAccountId(20).setDeviceId(30).setMerchantId(40).setMerchantCountry("GB")
                .setAmountUsdMinor(5_000).setCardInfo(CardInfo.newBuilder().setId(10)).build();
    }

    /** A status change as the Flink deserializer reads it: its row and its own type's filter mask. */
    static Object[] change(long t, CardStatus st) {
        byte[] raw = CardStatusChanged.newBuilder().setEventId(t).setEventTimeUs(t).setCustomerId(1).setCardId(10)
                .setStatus(st).build().toByteArray();
        var binding = P.binding("card_status_changed");
        var codec = io.realy.scoring.binding.WireCodec.of(binding.message);
        try {
            Object[] values = codec.decode(raw);
            EventRow row = S.rowPlan().row(binding, codec, values);
            return new Object[] {row, F.mask(codec.view(values), CHANGE)};
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void theStatusSchemaAppendsStatusAndGatedFeatures() {
        assertThat(S.size()).isEqualTo(83);
        assertThat(S.gate(S.features.get(S.indexOf("dev_blocked_card_attempts_24h")))).isEqualTo(S.indexOf("card_blocked"));
        assertThat(S.features.get(S.indexOf("card_blocked")).windowUs()).isZero();
        // the account is computed in its customer's stage (it does not directly follow the customer in the registry)
        assertThat(S.streamByEntity().keySet()).startsWith("customer", "account", "card");
        var reg = io.realy.scoring.binding.EntityRegistry.load();
        assertThat(reg.stageOf("account")).isEqualTo("customer");
        assertThat(reg.stageOf("card")).isEqualTo("card");
        assertThat(reg.stageOf("terminal")).isEqualTo("terminal");
        assertThat(reg.stages(S.streamByEntity().keySet()).get("customer")).containsExactly("account");
    }

    @Test
    void aPaymentReadsTheCardsLatestStatusAsOfItsTime() {
        var card = new EntityAggregator(S, "card", P);
        byte[] st = null;
        float[] out = new float[S.size()];
        TransactionEvent p = pay(T0);
        st = card.process(st, S.rowPlan().row(p), F.mask(EventView.of(p)), out, PAY);
        assertThat(out[S.indexOf("card_blocked")]).isZero();
        Object[] c = change(T0 + MIN, CardStatus.CARD_STATUS_STOLEN);
        st = card.process(st, (EventRow) c[0], (long) c[1], null, CHANGE);
        out = new float[S.size()];
        p = pay(T0 + 2 * MIN);
        card.evaluate(st, S.rowPlan().row(p), F.mask(EventView.of(p)), out, PAY);
        assertThat(out[S.indexOf("card_blocked")]).isEqualTo(1f);
        assertThat(out[S.indexOf("card_reported_lost_stolen")]).isEqualTo(1f);
        assertThat(out[S.indexOf("card_status_code")]).isEqualTo(4f);              // STOLEN, an enum as its number
        assertThat(out[S.indexOf("card_secs_since_status_change")]).isEqualTo(60f);
        // the sticky part alone (what outlives the keyed-state TTL), restored into an otherwise empty state
        byte[] sticky = card.stickyState(st);
        assertThat(sticky).isNotNull();
        assertThat(card.holdsSticky(out)).isTrue();
        float[] r = new float[S.size()];
        card.evaluate(card.restoreSticky(sticky), S.rowPlan().row(p), F.mask(EventView.of(p)), r, PAY);
        assertThat(r[S.indexOf("card_blocked")]).isEqualTo(1f);
        assertThat(r[S.indexOf("card_status_code")]).isEqualTo(4f);
        assertThat(r[S.indexOf("card_secs_since_status_change")]).isEqualTo(2_592_000f);   // windows did not survive
        // unblocked: nothing sticky left to keep
        c = change(T0 + 3 * MIN, CardStatus.CARD_STATUS_ACTIVE);
        st = card.process(st, (EventRow) c[0], (long) c[1], null, CHANGE);
        assertThat(card.stickyState(st)).isNull();
        out = new float[S.size()];
        p = pay(T0 + 4 * MIN);
        card.evaluate(st, S.rowPlan().row(p), F.mask(EventView.of(p)), out, PAY);
        assertThat(out[S.indexOf("card_blocked")]).isZero();
        assertThat(card.holdsSticky(out)).isFalse();
        assertThat(card.foldsSticky(CHANGE)).isTrue();
        assertThat(card.foldsSticky(PAY)).isFalse();
    }

    @Test
    void aGatedFeatureFoldsOnlyWhileItsGateIsOn() {
        var dev = new EntityAggregator(S, "device", P);
        int gated = S.indexOf("dev_blocked_card_attempts_24h"), gate = S.indexOf("card_blocked");
        byte[] st = null;
        float[] out = new float[S.size()];
        TransactionEvent p = pay(T0);
        st = dev.process(st, S.rowPlan().row(p), F.mask(EventView.of(p)), out, PAY);   // card_blocked 0 in `out`
        assertThat(out[gated]).isZero();
        out = new float[S.size()];
        out[gate] = 1f;                                                                // the card stage's value
        p = pay(T0 + MIN);
        st = dev.process(st, S.rowPlan().row(p), F.mask(EventView.of(p)), out, PAY);
        assertThat(out[gated]).isEqualTo(1f);
    }

    @Test
    void schemaRulesForWindowsAndGates() {
        String head = "schema_version: 9\nentities: {customer: {key: customer_id}, card: {key: card_id}, device: {key: device_id}}\n"
                + "filters: {}\ncountries: [US]\nfeatures:\n";
        assertThatThrownBy(() -> FeatureSchema.parse(head + "  - {name: c, source: stream, entity: card, agg: count}\n"))
                .hasMessageContaining("needs a `window`");
        assertThatThrownBy(() -> FeatureSchema.parse(head + "  - {name: b, source: stream, entity: card, agg: latest}\n"
                + "  - {name: g, source: stream, entity: customer, agg: count, gate: b, window: 1h, granularity: 1m}\n"))
                .hasMessageContaining("computed after the customer stage");
        FeatureSchema ok = FeatureSchema.parse(head + "  - {name: b, source: stream, entity: card, agg: latest, window: 1h}\n");
        assertThat(ok.features.get(0).granularityUs()).isEqualTo(ok.features.get(0).windowUs());
        // a gate is read from the feature vector: a gated feature cannot fold a type that carries none
        FeatureSchema foldOnly = FeatureSchema.parse(head + "  - {name: b, source: stream, entity: card, agg: latest}\n"
                + "  - {name: g, source: stream, entity: device, agg: count, gate: b, event_types: [card_status_changed], window: 1h, granularity: 1m}\n");
        assertThatThrownBy(() -> new EntityAggregator(foldOnly, "device", P)).hasMessageContaining("only scored event types");
    }
}
