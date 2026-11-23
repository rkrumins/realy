package io.realy.flink;

import static org.assertj.core.api.Assertions.assertThat;

import io.realy.contracts.identity.v1.LoginEvent;
import io.realy.contracts.identity.v1.LoginOutcome;
import io.realy.contracts.payments.v1.PaymentType;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.scoring.binding.PipelineBinding;
import io.realy.scoring.features.EntityAggregator;
import io.realy.scoring.features.EventRow;
import io.realy.scoring.features.EventView;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.rules.Filters;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

/**
 * Logins (fold-only, customer + device + IP) interleaved with payments through the keyed stages of
 * pipeline payments-ext, without --assemble: the logins update state and are dropped after the last stage
 * that folds them, every payment comes out once, and its customer and card features equal the sequential
 * reference (a payment sees the failed logins without being counted as one).
 */
class MessageTypesJobTest {
    @Test
    void loginsFoldIntoTheStagesAndOnlyPaymentsComeOut() throws Exception {
        PipelineBinding pipe = PipelineBinding.load("payments-ext");
        String yaml = FeatureSchema.bundle("features/feature_schema_ext.yaml");
        FeatureSchema schema = FeatureSchema.of(yaml);
        Filters filters = new Filters(schema, pipe);
        int login = pipe.typeOrdinal("login"), payment = pipe.typeOrdinal("payment");
        List<String> ents = List.of("customer", "card", "device", "merchant", "ip");

        Random rnd = new Random(11);
        List<Envelope> input = new ArrayList<>();
        Map<String, Map<Long, byte[]>> st = new HashMap<>();
        Map<Long, float[]> expected = new HashMap<>();
        long t = 1_800_000_000_000_000L;
        int payments = 0;
        for (int i = 0; i < 3000; i++) {
            t += 1_000_000L * (1 + rnd.nextInt(30));
            long cust = 1 + rnd.nextInt(30);
            long device = 500 + rnd.nextInt(8);
            int ip = 1000 + rnd.nextInt(50);
            Envelope env;
            EventRow row;
            long mask;
            float[] out;
            int type;
            if (rnd.nextInt(3) == 0) {
                byte[] raw = LoginEvent.newBuilder().setEventId(i + 1).setEventTimeUs(t).setCustomerId(cust).setDeviceId(device)
                        .setIpV4(ip).setOutcome(rnd.nextInt(4) == 0 ? LoginOutcome.LOGIN_FAILED_PASSWORD : LoginOutcome.LOGIN_SUCCESS)
                        .setNewDevice(rnd.nextInt(5) == 0).build().toByteArray();
                env = EventDeserializer.standalone(pipe, schema, filters, "login", raw, null, 1);
                assertThat(env.features).isNull();
                row = env.row;
                mask = env.filterMask;
                out = null;
                type = login;
            } else {
                TransactionEvent e = TransactionEvent.newBuilder().setEventId(i + 1).setEventTimeUs(t)
                        .setPaymentType(PaymentType.CARD_PURCHASE).setCustomerId(cust).setCardId(1000 + cust)
                        .setAmountUsdMinor(150 + rnd.nextInt(9000)).setMerchantId(1 + rnd.nextInt(20)).setMerchantCountry("GB")
                        .setDeviceId(device).setIpV4(ip).build();
                env = EventDeserializer.envelope(e, e.toByteArray(), null, false, schema, filters, 1);
                row = schema.rowPlan().row(e);
                mask = filters.mask(EventView.of(e), payment);
                out = new float[schema.size()];
                type = payment;
                payments++;
            }
            input.add(env);
            // the sequential reference: one thread, input order
            for (String ent : List.of("customer", "card")) {
                var agg = new EntityAggregator(schema, ent, pipe);
                var m = st.computeIfAbsent(ent, k -> new HashMap<>());
                long key = agg.key(row);
                if (key == 0) continue;
                m.put(key, agg.process(m.get(key), row, mask, out, type));
            }
            if (out != null) expected.put(env.eventId, out);
        }

        Configuration conf = new Configuration();
        conf.setString("state.backend.type", "rocksdb");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        var out = FraudJob.features(env.fromData(input).setParallelism(1), ents, false, List.of(), false, yaml, null,
                Map.of(), "payments-ext").executeAndCollect(10_000);

        assertThat(out).hasSize(payments);
        int failed = schema.indexOf("cust_failed_logins_1h"), sawFailed = 0;
        for (Envelope o : out) {
            assertThat(o.type).isNull();
            float[] exp = expected.get(o.eventId);
            assertThat(exp).as("event " + o.eventId + " is a payment").isNotNull();
            for (String ent : List.of("customer", "card")) {
                for (var s : schema.stream(ent)) {
                    assertThat(o.features[s.index()]).as(s.name() + " event " + o.eventId).isEqualTo(exp[s.index()]);
                }
            }
            if (o.features[failed] > 0) sawFailed++;
        }
        assertThat(sawFailed).as("payments that saw a failed login").isPositive();
    }
}
