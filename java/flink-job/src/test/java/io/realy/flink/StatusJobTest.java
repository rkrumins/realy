package io.realy.flink;

import static org.assertj.core.api.Assertions.assertThat;

import io.realy.contracts.accounts.v1.AccountStatus;
import io.realy.contracts.accounts.v1.AccountStatusChanged;
import io.realy.contracts.cards.v1.CardStatusChanged;
import io.realy.contracts.payments.v1.CardInfo;
import io.realy.contracts.payments.v1.CardStatus;
import io.realy.contracts.payments.v1.PaymentType;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.scoring.binding.PipelineBinding;
import io.realy.scoring.features.EntityAggregator;
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
 * Status change feeds through the parallel keyed stages (pipeline payments-status): card and account status
 * changes interleaved with payments, the account computed inside its customer's stage. Every payment's card and
 * account features (statuses included) equal a sequential reference, and the device's gated count sees the card
 * stage's card_blocked, whatever the parallelism.
 */
class StatusJobTest {
    @Test
    void statusFeaturesAreExactThroughTheParallelStages() throws Exception {
        PipelineBinding pipe = PipelineBinding.load("payments-status");
        String yaml = FeatureSchema.bundle("features/feature_schema_status.yaml");
        FeatureSchema schema = FeatureSchema.of(yaml);
        Filters filters = new Filters(schema, pipe);
        int payment = pipe.typeOrdinal("payment");
        List<String> ents = new ArrayList<>(schema.streamByEntity().keySet());

        Random rnd = new Random(7);
        List<Envelope> input = new ArrayList<>();
        Map<String, Map<Long, byte[]>> st = new HashMap<>();
        Map<Long, float[]> expected = new HashMap<>();
        long t = 1_800_000_000_000_000L;
        int payments = 0, blockedSeen = 0;
        for (int i = 0; i < 4000; i++) {
            t += 1_000_000L * (1 + rnd.nextInt(20));
            long cust = 1 + rnd.nextInt(40);
            long card = 1000 + cust * 2 + rnd.nextInt(2);       // two cards and two accounts per customer
            long acct = 5000 + cust * 2 + rnd.nextInt(2);
            Envelope env;
            float[] out = null;
            int r = rnd.nextInt(10);
            if (r == 0) {
                CardStatus s = rnd.nextBoolean() ? CardStatus.CARD_STATUS_BLOCKED : CardStatus.CARD_STATUS_ACTIVE;
                byte[] raw = CardStatusChanged.newBuilder().setEventId(i + 1).setEventTimeUs(t).setCustomerId(cust).setCardId(card)
                        .setStatus(s).build().toByteArray();
                env = EventDeserializer.standalone(pipe, schema, filters, "card_status_changed", raw, null, 1);
            } else if (r == 1) {
                var b = AccountStatusChanged.newBuilder().setEventId(i + 1).setEventTimeUs(t).setCustomerId(cust).setAccountId(acct)
                        .setStatus(rnd.nextInt(3) == 0 ? AccountStatus.ACCOUNT_STATUS_FROZEN : AccountStatus.ACCOUNT_STATUS_OPEN);
                if (rnd.nextBoolean()) b.addFlags("AML_HOLD");
                env = EventDeserializer.standalone(pipe, schema, filters, "account_status_changed", b.build().toByteArray(), null, 1);
            } else {
                TransactionEvent e = TransactionEvent.newBuilder().setEventId(i + 1).setEventTimeUs(t)
                        .setPaymentType(PaymentType.CARD_PURCHASE).setCustomerId(cust).setCardId(card).setAccountId(acct)
                        .setCardInfo(CardInfo.newBuilder().setId(card)).setAmountUsdMinor(100 + rnd.nextInt(9000))
                        .setMerchantId(1 + rnd.nextInt(10)).setMerchantCountry("GB").setDeviceId(700 + rnd.nextInt(6)).build();
                env = EventDeserializer.envelope(e, e.toByteArray(), null, false, schema, filters, 1);
                out = new float[schema.size()];
                payments++;
            }
            int type = env.type == null ? payment : pipe.typeOrdinal(env.type);
            input.add(env);
            // the sequential reference: one thread, input order, in evaluation order (gates read earlier stages)
            for (String ent : ents) {
                var agg = new EntityAggregator(schema, ent, pipe);
                var m = st.computeIfAbsent(ent, k -> new HashMap<>());
                long key = agg.key(env.row);
                if (key == 0) {
                    if (out != null) agg.defaults(out);
                    continue;
                }
                byte[] next = agg.process(m.get(key), env.row, env.filterMask, out, type);
                m.put(key, next);
            }
            if (out != null) {
                expected.put(env.eventId, out);
                if (out[schema.indexOf("card_blocked")] > 0) blockedSeen++;
            }
        }

        Configuration conf = new Configuration();
        conf.setString("state.backend.type", "rocksdb");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(3);
        var out = FraudJob.features(env.fromData(input).setParallelism(1), ents, false, List.of(), false, yaml, null,
                Map.of(), "payments-status").executeAndCollect(10_000);

        assertThat(out).hasSize(payments);
        assertThat(blockedSeen).as("payments on a blocked card").isPositive();
        for (Envelope o : out) {
            float[] exp = expected.get(o.eventId);
            for (String ent : List.of("customer", "account", "card")) {
                for (var s : schema.stream(ent)) {
                    assertThat(o.features[s.index()]).as(s.name() + " event " + o.eventId).isEqualTo(exp[s.index()]);
                }
            }
            int g = schema.indexOf("dev_blocked_card_attempts_24h");
            if (o.features[schema.indexOf("card_blocked")] == 0) continue;
            assertThat(o.features[g]).as("a payment on a blocked card is counted by its device").isPositive();
        }
    }
}
