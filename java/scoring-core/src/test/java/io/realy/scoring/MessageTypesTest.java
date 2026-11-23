package io.realy.scoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.realy.contracts.devices.v1.DeviceTelemetry;
import io.realy.contracts.identity.v1.LoginEvent;
import io.realy.contracts.identity.v1.LoginOutcome;
import io.realy.contracts.payments.v1.PaymentType;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.scoring.binding.PipelineBinding;
import io.realy.scoring.binding.WireCodec;
import io.realy.scoring.features.EntityAggregator;
import io.realy.scoring.features.EventRow;
import io.realy.scoring.features.EventView;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.rules.Filters;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Event types that carry a subset of the keyed entities (pipeline payments-ext): a login folds into the
 * customer, device and IP features listed for it, a payment reads them without being counted, and the base
 * schema's features keep their indices and behaviour.
 */
class MessageTypesTest {
    static final PipelineBinding EXT = PipelineBinding.load("payments-ext");
    static final FeatureSchema BASE = FeatureSchema.load();
    static final FeatureSchema S = schema();
    static final Filters F = new Filters(S, EXT);
    static final long T0 = 1_800_000_000_000_000L, MIN = 60_000_000L;
    static final int LOGIN = EXT.typeOrdinal("login"), PAYMENT = EXT.typeOrdinal("payment"),
            TELEMETRY = EXT.typeOrdinal("device_telemetry");

    static FeatureSchema schema() {
        try {
            return FeatureSchema.parse(FeatureSchema.bundle("features/feature_schema_ext.yaml"));
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    record Typed(EventRow row, long mask) {}

    static Typed login(long t, boolean ok, long device, int ip) {
        byte[] raw = LoginEvent.newBuilder().setEventId(t).setEventTimeUs(t).setCustomerId(1).setDeviceId(device)
                .setIpV4(ip).setOutcome(ok ? LoginOutcome.LOGIN_SUCCESS : LoginOutcome.LOGIN_FAILED_PASSWORD).build().toByteArray();
        return typed("login", raw);
    }

    static Typed telemetry(long t, long device, double risk, boolean emulator) {
        byte[] raw = DeviceTelemetry.newBuilder().setEventId(t).setEventTimeUs(t).setDeviceId(device).setIpV4(7)
                .setRiskScore(risk).setEmulator(emulator).build().toByteArray();
        return typed("device_telemetry", raw);
    }

    static Typed typed(String type, byte[] raw) {
        try {
            var b = EXT.binding(type);
            var c = WireCodec.of(b.message);
            Object[] v = c.decode(raw);
            return new Typed(S.rowPlan().row(b, c, v), F.mask(c.view(v), EXT.typeOrdinal(type)));
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static TransactionEvent payment(long t, long device) {
        return TransactionEvent.newBuilder().setEventId(t).setEventTimeUs(t).setPaymentType(PaymentType.CARD_PURCHASE)
                .setCustomerId(1).setCardId(10).setDeviceId(device).setIpV4(7).setAmountUsdMinor(2500).setMerchantId(3)
                .setMerchantCountry("GB").build();
    }

    @Test
    void theExtendedSchemaAppendsToTheBaseWithIndicesUnchanged() {
        assertThat(BASE.size()).isEqualTo(62);
        assertThat(S.size()).isEqualTo(72);
        for (int i = 0; i < BASE.size(); i++) assertThat(S.features.get(i).name()).isEqualTo(BASE.features.get(i).name());
        assertThat(S.indexOf("cust_failed_logins_1h")).isGreaterThanOrEqualTo(62);
        assertThat(S.on(S.features.get(S.indexOf("dev_distinct_customers_all_24h")))).containsExactly("payment", "login");
        assertThat(S.on(S.features.get(0))).isNull(); // base features: the default type
    }

    @Test
    void scopedFiltersApplyOnlyToTheirTypes() {
        int failed = S.filterBit("login_failed");
        assertThat(login(T0, false, 5, 9).mask & (1L << failed)).isNotZero();
        assertThat(login(T0, true, 5, 9).mask & (1L << failed)).isZero();
        // a payment never sets a login filter; a login never sets a payment filter
        long pay = F.mask(EventView.of(payment(T0, 5)), PAYMENT);
        assertThat(pay & (1L << failed)).isZero();
        long loginBits = login(T0, false, 5, 9).mask;
        for (String n : BASE.filterNames) assertThat(loginBits & (1L << S.filterBit(n))).as(n).isZero();
    }

    @Test
    void aLoginFoldsItsFeaturesAndAPaymentReadsThemWithoutBeingCounted() {
        var cust = new EntityAggregator(S, "customer", EXT);
        int failed = S.indexOf("cust_failed_logins_1h"), ips = S.indexOf("cust_distinct_login_ips_24h");
        int custCount = S.indexOf("cust_count_1h");
        assertThat(cust.folds(LOGIN)).isTrue();
        byte[] st = null;
        for (int i = 0; i < 3; i++) {
            Typed l = login(T0 + i * MIN, false, 5, 100 + i);
            st = cust.process(st, l.row, l.mask, null, LOGIN); // fold-only: no output vector
        }
        Typed ok = login(T0 + 3 * MIN, true, 5, 100);
        st = cust.process(st, ok.row, ok.mask, null, LOGIN);

        TransactionEvent p = payment(T0 + 4 * MIN, 5);
        float[] out = new float[S.size()];
        byte[] after = cust.process(st, S.rowPlan().row(p), F.mask(EventView.of(p), PAYMENT), out, PAYMENT);
        assertThat(out[failed]).isEqualTo(3f);
        assertThat(out[ips]).isEqualTo(3f);
        // the payment counts itself in the payment features, and logins never did
        assertThat(out[custCount]).isEqualTo(1f);

        // a second payment still sees 3 failed logins: the first payment was not counted as one
        TransactionEvent p2 = payment(T0 + 5 * MIN, 5);
        float[] out2 = new float[S.size()];
        cust.process(after, S.rowPlan().row(p2), F.mask(EventView.of(p2), PAYMENT), out2, PAYMENT);
        assertThat(out2[failed]).isEqualTo(3f);
        assertThat(out2[custCount]).isEqualTo(2f);

        // an hour later the failed logins have left the window
        TransactionEvent late = payment(T0 + 70 * MIN, 5);
        float[] out3 = new float[S.size()];
        cust.process(after, S.rowPlan().row(late), F.mask(EventView.of(late), PAYMENT), out3, PAYMENT);
        assertThat(out3[failed]).isEqualTo(0f);
    }

    @Test
    void oneFeatureFedByTwoTypes() {
        var dev = new EntityAggregator(S, "device", EXT);
        int all = S.indexOf("dev_distinct_customers_all_24h");
        byte[] st = null;
        Typed l = login(T0, true, 5, 9);
        st = dev.process(st, l.row, l.mask, null, LOGIN);
        TransactionEvent p = payment(T0 + MIN, 5).toBuilder().setCustomerId(2).build();
        float[] out = new float[S.size()];
        dev.process(st, S.rowPlan().row(p), F.mask(EventView.of(p), PAYMENT), out, PAYMENT);
        assertThat(out[all]).isEqualTo(2f); // customer 1 by its login, customer 2 by its payment
    }

    @Test
    void aTypeThatFoldsNothingAtAStageLeavesItsStateAlone() {
        var merchant = new EntityAggregator(S, "merchant", EXT);
        var card = new EntityAggregator(S, "card", EXT);
        assertThat(merchant.folds(LOGIN)).isFalse();
        assertThat(card.folds(LOGIN)).isFalse();
        assertThat(card.folds(EXT.typeOrdinal("card_lifecycle"))).isTrue();
        Typed l = login(T0, false, 5, 9);
        byte[] st = new byte[] {1, 2, 3};
        assertThat(merchant.process(st, l.row, l.mask, null, LOGIN)).isSameAs(st);
    }

    @Test
    void deviceTelemetryFoldsDeviceAndIpFeatures() {
        var dev = new EntityAggregator(S, "device", EXT);
        var ip = new EntityAggregator(S, "ip", EXT);
        int risk = S.indexOf("dev_max_risk_24h"), tampered = S.indexOf("dev_tampered_reports_24h"),
                ipRisk = S.indexOf("ip_max_reputation_risk_24h");
        byte[] d = null;
        for (double r : new double[] {0.2, 0.9, 0.4}) {
            Typed t = telemetry(T0, 5, r, r > 0.5);
            d = dev.process(d, t.row, t.mask, null, TELEMETRY);
        }
        TransactionEvent p = payment(T0 + MIN, 5);
        float[] out = new float[S.size()];
        dev.process(d, S.rowPlan().row(p), F.mask(EventView.of(p), PAYMENT), out, PAYMENT);
        assertThat(out[risk]).isEqualTo(0.9f);
        assertThat(out[tampered]).isEqualTo(1f);
        assertThat(ip.folds(TELEMETRY)).isTrue();
        assertThat(out[ipRisk]).isEqualTo(0f); // not the device stage's feature
    }

    @Test
    void thePipelineRoutesTypesByTopicAndRole() {
        assertThat(EXT.standalone("login")).isTrue();
        assertThat(EXT.standalone("device_telemetry")).isTrue();
        assertThat(EXT.standalone("payment")).isFalse();
        assertThat(EXT.standalone("payment_initiation")).isFalse(); // a transaction part
        assertThat(EXT.scored("login")).isFalse();
        assertThat(EXT.extraTopics()).containsExactlyEntriesOf(java.util.Map.of("payments.devices.telemetry", List.of("device_telemetry")));
        assertThat(EXT.partitionKeyOf("device_telemetry")).isEqualTo("device");
        assertThat(EXT.partitionKeyOf("login")).isEqualTo("customer");
        assertThat(PipelineBinding.load("payments").extraTopics()).isEmpty();
    }

    @Test
    void theBaseSchemaIsUnchangedByThePipelineItRunsIn() {
        // payments features fold the default type in either pipeline, so a payment reads the same values
        var base = new EntityAggregator(BASE, "customer", PipelineBinding.load("payments"));
        var ext = new EntityAggregator(S, "customer", EXT);
        TransactionEvent p = payment(T0, 5);
        float[] a = new float[BASE.size()], b = new float[S.size()];
        byte[] sa = base.process(null, BASE.rowPlan().row(p), new Filters(BASE, PipelineBinding.load("payments")).mask(EventView.of(p)), a);
        byte[] sb = ext.process(null, S.rowPlan().row(p), F.mask(EventView.of(p), PAYMENT), b, PAYMENT);
        for (int i = 0; i < BASE.size(); i++) assertThat(b[i]).as(BASE.features.get(i).name()).isEqualTo(a[i]);
        assertThat(sa).isNotNull();
        assertThat(sb).isNotNull();
    }

    @Test
    void schemaErrorsAreLoadErrors() {
        String ext = "schema_version: 3\nextends: feature_schema.yaml\nfeatures:\n";
        String base = io.realy.scoring.binding.Contracts.text("features/feature_schema.yaml") + "\n---\n";
        String dup = base + ext + "  - { name: cust_count_1h, source: stream, entity: customer, agg: count, window: 1h, granularity: 1m }\n";
        assertThatThrownBy(() -> FeatureSchema.parse(dup)).hasMessageContaining("cust_count_1h");
        String on = base + ext + "  - { name: x_logins, source: stream, entity: customer, agg: count, on: [login], window: 1h, granularity: 1m }\n";
        assertThatThrownBy(() -> FeatureSchema.parse(on)).hasMessageContaining("event_types");
    }
}
