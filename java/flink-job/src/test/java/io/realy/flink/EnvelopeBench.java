package io.realy.flink;

import static org.assertj.core.api.Assertions.assertThat;

import io.realy.contracts.payments.v1.EntryMode;
import io.realy.contracts.payments.v1.PaymentType;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.rules.Filters;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The Envelope travels between seven keyed stages, so its serializer is on the hot path. It must stay a
 * Flink POJO (fast field-by-field serializer, schema evolution on restore), never fall back to Kryo.
 * With -Dbench=true it also prints the serialized size and the round-trip cost.
 */
class EnvelopeBench {
    static Envelope sample() {
        FeatureSchema schema = FeatureSchema.load();
        TransactionEvent e = TransactionEvent.newBuilder().setEventId(7).setEventTimeUs(1_800_000_000_000_000L)
                .setPaymentType(PaymentType.CARD_PURCHASE).setCustomerId(11).setCardId(12).setMerchantId(13)
                .setDeviceId(14).setAmountUsdMinor(1999).setMerchantCountry("GB").setEntryMode(EntryMode.ECOM_3DS).build();
        return EventDeserializer.envelope(e, e.toByteArray(), new byte[24], false, schema, new Filters(schema), 1);
    }

    @Test
    void envelopeIsAFlinkPojoNotKryo() {
        TypeInformation<Envelope> ti = TypeInformation.of(Envelope.class);
        assertThat(ti).isInstanceOf(PojoTypeInfo.class);
        var cfg = new SerializerConfigImpl();
        cfg.setGenericTypes(false);   // throws on any field that would need Kryo
        TypeSerializer<Envelope> ser = ti.createSerializer(cfg);
        assertThat(ser.getClass().getSimpleName()).isEqualTo("PojoSerializer");
    }

    @Test
    @Tag("bench")
    void roundTripCost() throws Exception {
        if (!Boolean.getBoolean("bench")) return;
        TypeSerializer<Envelope> ser = TypeInformation.of(Envelope.class).createSerializer(new ExecutionConfig().getSerializerConfig());
        Envelope env = sample();
        for (int i = 0; i < 4; i++) env.span(1, 2, 3, 100, 5);
        Envelope featuresMode = sample();      // `features` mode: no raw event, no spans
        featuresMode.event = null;
        featuresMode.spans = new long[0];
        DataOutputSerializer fo = new DataOutputSerializer(1024);
        ser.serialize(featuresMode, fo);
        System.out.printf("{\"bench\":\"envelope-features-mode\",\"bytes\":%d}%n", fo.length());
        DataOutputSerializer out = new DataOutputSerializer(1024);
        int n = 300_000;
        double us = 0;
        int bytes = 0;
        for (int r = 0; r < 3; r++) {
            long t0 = System.nanoTime();
            for (int i = 0; i < n; i++) {
                out.clear();
                ser.serialize(env, out);
                bytes = out.length();
                ser.deserialize(new DataInputDeserializer(out.getSharedBuffer(), 0, bytes));
            }
            us = (System.nanoTime() - t0) / 1e3 / n;
        }
        System.out.printf("{\"bench\":\"envelope\",\"bytes\":%d,\"round_trip_us\":%.3f}%n", bytes, us);
    }
}
