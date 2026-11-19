package io.realy.flink;

import static org.assertj.core.api.Assertions.assertThat;

import io.realy.contracts.payments.v1.EntryMode;
import io.realy.contracts.payments.v1.PaymentType;
import io.realy.contracts.payments.v1.TransactionEvent;
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

/** MiniCluster + RocksDB: keyed stages must equal sequential per-entity aggregation. */
class FeatureJobTest {
    @Test
    void keyedStagesMatchSequentialReference() throws Exception {
        FeatureSchema schema = FeatureSchema.load();
        Filters filters = new Filters(schema);
        Random rnd = new Random(7);
        List<TransactionEvent> events = new ArrayList<>();
        long t = 1_800_000_000_000_000L;
        for (int i = 0; i < 2000; i++) {
            t += 1_000_000L * (1 + rnd.nextInt(20));
            long cust = 1 + rnd.nextInt(40);
            boolean cp = rnd.nextBoolean();
            events.add(TransactionEvent.newBuilder().setEventId(i + 1).setEventTimeUs(t)
                    .setPaymentType(PaymentType.CARD_PURCHASE).setCustomerId(cust).setCardId(1000 + cust)
                    .setAmountUsdMinor(rnd.nextBoolean() ? 150 : 5000 + rnd.nextInt(5000))
                    .setMerchantId(1 + rnd.nextInt(30)).setMerchantCountry("GB").setCardPresent(cp)
                    .setEntryMode(cp ? EntryMode.CONTACTLESS : EntryMode.ECOM_3DS)
                    .setDeviceId(cp ? 0 : 500 + rnd.nextInt(10)).setLat(51.5 + rnd.nextDouble()).setLon(-0.1).build());
        }
        // Sequential reference (single thread, input order) for customer + card entities.
        List<String> ents = List.of("customer", "card", "device", "merchant");
        Map<String, Map<Long, byte[]>> st = new HashMap<>();
        Map<Long, float[]> expected = new HashMap<>();
        for (TransactionEvent e : events) {
            float[] v = new float[schema.size()];
            long mask = filters.mask(EventView.of(e));
            for (String ent : List.of("customer", "card")) {
                var agg = new EntityAggregator(schema, ent);
                var m = st.computeIfAbsent(ent, k -> new HashMap<>());
                long key = EventView.entityKey(e, ent);
                m.put(key, agg.process(m.get(key), e, mask, v));
            }
            expected.put(e.getEventId(), v);
        }

        Configuration conf = new Configuration();
        conf.setString("state.backend.type", "rocksdb");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(3);
        List<Envelope> input = new ArrayList<>();
        for (TransactionEvent e : events) {
            input.add(EventDeserializer.envelope(e, e.toByteArray(), null, false, filters, schema.size(), 1));
        }
        var out = FraudJob.features(env.fromData(input).setParallelism(1), ents, false).executeAndCollect(10_000);
        assertThat(out).hasSize(events.size());
        int checked = 0;
        for (Envelope o : out) {
            float[] exp = expected.get(o.eventId);
            for (var s : schema.stream("card")) {
                assertThat(o.features[s.index()]).as(s.name() + " event " + o.eventId).isEqualTo(exp[s.index()]);
            }
            for (var s : schema.stream("customer")) {
                assertThat(o.features[s.index()]).as(s.name()).isEqualTo(exp[s.index()]);
            }
            assertThat(o.spans.length / 3).isGreaterThanOrEqualTo(2 + 2 * ents.size());
            checked++;
        }
        assertThat(checked).isEqualTo(2000);
    }
}
