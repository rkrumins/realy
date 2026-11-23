package io.realy.scoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.realy.contracts.payments.v1.EntryMode;
import io.realy.contracts.payments.v1.PaymentType;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.scoring.features.DerivedFeatures;
import io.realy.scoring.features.FeatureSchema;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/** Derived features written as CEL expressions (feature_schema.yaml `expr`): no Java code per feature. */
class ExpressionFeatureTest {
    static String schemaWith(String extra) throws Exception {
        Path p = Path.of(System.getProperty("realy.repo", "../.."), "contracts", "features", "feature_schema.yaml");
        if (!Files.exists(p)) p = Path.of("../../contracts/features/feature_schema.yaml");
        return Files.readString(p).stripTrailing() + "\n" + extra;
    }

    @Test
    void expressionsOverTheEventAndOtherFeatures() throws Exception {
        FeatureSchema s = FeatureSchema.parse(schemaWith("""
                  - { name: cvv_mismatch_t, source: derived, expr: "event.cvv_match ? 0.0 : 1.0" }
                  - { name: big_vs_avg, source: derived, expr: "f.amount_usd > 2.0 * f.cust_avg_ticket_usd ? 1.0 : 0.0" }
                """));
        int cvv = s.indexOf("cvv_mismatch_t"), big = s.indexOf("big_vs_avg"), avg = s.indexOf("cust_avg_ticket_usd");
        float[] v = new float[s.features.size()];
        v[avg] = 40f;
        TransactionEvent e = TransactionEvent.newBuilder().setEventId(1).setPaymentType(PaymentType.CARD_PURCHASE)
                .setEntryMode(EntryMode.ECOM_NON_3DS).setAmountUsdMinor(15_000).setCvvMatch(false).build();
        DerivedFeatures.compute(s, e, null, v);
        assertThat(v[cvv]).isEqualTo(1f);
        assertThat(v[big]).isEqualTo(1f);              // $150 > 2 x $40 (amount_usd is computed earlier)
        DerivedFeatures.compute(s, e.toBuilder().setCvvMatch(true).setAmountUsdMinor(5_000).build(), null, v);
        assertThat(v[cvv]).isEqualTo(0f);
        assertThat(v[big]).isEqualTo(0f);
    }

    @Test
    void booleansAreOneOrZeroAndOtherTypesFailLoudly() throws Exception {
        FeatureSchema s = FeatureSchema.parse(schemaWith(
                "  - { name: avs_fail, source: derived, expr: \"!event.avs_match\" }\n"
                + "  - { name: bad_type, source: derived, expr: \"event.merchant_country\" }\n"));
        float[] v = new float[s.features.size()];
        TransactionEvent e = TransactionEvent.newBuilder().setEventId(2).setPaymentType(PaymentType.CARD_PURCHASE)
                .setAvsMatch(false).setMerchantCountry("GB").build();
        assertThatThrownBy(() -> DerivedFeatures.compute(s, e, null, v)).hasMessageContaining("number or a bool");
        assertThat(v[s.indexOf("avs_fail")]).isEqualTo(1f);   // computed before the failing one
    }

    @Test
    void badDefinitionsFailAtLoad() {
        assertThatThrownBy(() -> FeatureSchema.parse(schemaWith(
                "  - { name: x, source: derived, expr: \"f.no_such_feature +\" }\n")))
                .hasMessageContaining("invalid CEL expression");
        assertThatThrownBy(() -> FeatureSchema.parse(schemaWith(
                "  - { name: y, source: derived, fn: is_cnp, expr: \"1.0\" }\n")))
                .hasMessageContaining("exactly one of");
    }
}
