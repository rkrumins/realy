package io.realy.scoring;

import static org.assertj.core.api.Assertions.assertThat;

import io.realy.contracts.payments.v1.AccountInfo;
import io.realy.contracts.payments.v1.Authentication;
import io.realy.contracts.payments.v1.CardInfo;
import io.realy.contracts.payments.v1.CardStatus;
import io.realy.contracts.payments.v1.DeviceInfo;
import io.realy.contracts.payments.v1.LineItem;
import io.realy.contracts.payments.v1.MerchantInfo;
import io.realy.contracts.payments.v1.PaymentType;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.scoring.binding.PipelineBinding;
import io.realy.scoring.features.DerivedFeatures;
import io.realy.scoring.features.EntityAggregator;
import io.realy.scoring.features.EventRow;
import io.realy.scoring.features.EventView;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.rules.Filters;
import org.junit.jupiter.api.Test;

/**
 * Pipeline payments-rich: features, filters, an entity key and derived expressions defined on the nested
 * blocks of a TransactionEvent, through the same row, filter and aggregator code the Flink stages and the
 * gateway run.
 */
class RichPaymentTest {
    static final PipelineBinding RICH = PipelineBinding.load("payments-rich");
    static final FeatureSchema S = schema();
    static final Filters F = new Filters(S, RICH);
    static final long T0 = 1_800_000_000_000_000L, MIN = 60_000_000L;

    static FeatureSchema schema() {
        try {
            return FeatureSchema.parse(FeatureSchema.bundle("features/feature_schema_rich.yaml"));
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static TransactionEvent pay(long t, CardStatus status, int attempts, boolean gift, String wallet) {
        TransactionEvent.Builder b = TransactionEvent.newBuilder().setEventId(t).setEventTimeUs(t)
                .setPaymentType(PaymentType.CARD_PURCHASE).setCustomerId(1).setCardId(10).setAccountId(20).setDeviceId(30)
                .setMerchantId(40).setMerchantCountry("GB").setAmountUsdMinor(25_000)
                .setCardInfo(CardInfo.newBuilder().setId(10).setBinCountry("US").setStatus(status).setWallet(wallet))
                .setAccountInfo(AccountInfo.newBuilder().setId(20).addFlags("AML_HOLD"))
                .setMerchantInfo(MerchantInfo.newBuilder().setId(40).setRiskTier(3))
                .setDeviceInfo(DeviceInfo.newBuilder().setId(30).setEmulator(true))
                .setAuthentication(Authentication.newBuilder().setAttempts(attempts));
        b.addItems(LineItem.newBuilder().setSku("x").setQty(1).setCategory(gift ? "GIFT_CARD" : "GROCERY"));
        b.addItems(LineItem.newBuilder().setSku("y").setQty(2).setCategory("HOME"));
        return b.build();
    }

    @Test
    void theRichSchemaAppendsNestedFeaturesToTheBase() {
        assertThat(S.size()).isEqualTo(73);
        assertThat(S.entityKeys.get("card")).isEqualTo("card_info.id");
        assertThat(S.features.get(0).name()).isEqualTo(FeatureSchema.load().features.get(0).name());
    }

    @Test
    void filtersOverNestedFieldsAndListsSetTheirBits() {
        long m = F.mask(EventView.of(pay(T0, CardStatus.CARD_STATUS_BLOCKED, 2, true, "")));
        for (String f : new String[] {"foreign_bin", "card_not_active", "gift_card_basket", "tampered_device"})
            assertThat(m & (1L << S.filterBit(f))).as(f).isNotZero();
        long clean = F.mask(EventView.of(TransactionEvent.newBuilder().setEventId(1).setCardId(10).build()));
        assertThat(clean & (1L << S.filterBit("card_not_active"))).isZero();
        assertThat(clean & (1L << S.filterBit("gift_card_basket"))).isZero();
    }

    @Test
    void rowsHoldNestedValuesAndTheCardKeyComesFromItsBlock() {
        TransactionEvent e = pay(T0, CardStatus.CARD_STATUS_STOLEN, 3, false, "APPLE_PAY");
        EventRow r = S.rowPlan().row(e);
        assertThat(r.num[S.rowPlan().numSlot("authentication.attempts")]).isEqualTo(3.0);
        assertThat(r.num[S.rowPlan().numSlot("card_info.status")]).isEqualTo(4.0);       // an enum as its number
        assertThat(r.num[S.rowPlan().numSlot("merchant_info.risk_tier")]).isEqualTo(3.0);
        assertThat(r.val[S.rowPlan().valSlot("card_info.wallet")]).isEqualTo(io.realy.common.Hashing.fnv1a64("APPLE_PAY"));
        // payments-rich keys the card from card_info.id: a payment whose flat card_id disagrees uses the block
        TransactionEvent odd = e.toBuilder().setCardId(999).build();
        assertThat(EventView.entityKey(odd, "card", RICH)).isEqualTo(10L);
        assertThat(EventView.entityKey(odd, "card", PipelineBinding.load("payments"))).isEqualTo(999L);
    }

    @Test
    void nestedStreamFeaturesAndDerivedExpressionsCompute() {
        var card = new EntityAggregator(S, "card", RICH);
        var cust = new EntityAggregator(S, "customer", RICH);
        byte[] cs = null, us = null;
        float[] out = null;
        for (int i = 0; i < 3; i++) {
            TransactionEvent e = pay(T0 + i * MIN, i == 1 ? CardStatus.CARD_STATUS_BLOCKED : CardStatus.CARD_STATUS_ACTIVE,
                    2, i > 0, i == 2 ? "APPLE_PAY" : "");
            out = new float[S.size()];
            long m = F.mask(EventView.of(e));
            cs = card.process(cs, S.rowPlan().row(e), m, out);
            us = cust.process(us, S.rowPlan().row(e), m, out);
            DerivedFeatures.compute(S, e, null, out);
        }
        assertThat(out[S.indexOf("card_inactive_attempts_24h")]).isEqualTo(1f);
        assertThat(out[S.indexOf("card_max_status_30d")]).isEqualTo(2f);           // BLOCKED = 2 seen within 30 days
        assertThat(out[S.indexOf("card_foreign_bin_count_24h")]).isEqualTo(3f);
        assertThat(out[S.indexOf("cust_gift_card_baskets_24h")]).isEqualTo(2f);
        assertThat(out[S.indexOf("cust_auth_attempts_sum_1h")]).isEqualTo(6f);
        assertThat(out[S.indexOf("cust_distinct_wallets_30d")]).isEqualTo(2f);      // "" and APPLE_PAY
        assertThat(out[S.indexOf("basket_items")]).isEqualTo(2f);
        assertThat(out[S.indexOf("account_aml_hold")]).isEqualTo(1f);
    }
}
