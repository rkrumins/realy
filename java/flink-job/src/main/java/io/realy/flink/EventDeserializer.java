package io.realy.flink;

import io.realy.common.BenchClock;
import io.realy.contracts.bench.v1.Stage;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.scoring.features.EventView;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.rules.Filters;
import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

/** Parses TransactionEvent + bench trace header, stamps INGEST, evaluates CEL filters once. */
public final class EventDeserializer implements KafkaRecordDeserializationSchema<Envelope> {
    private transient Filters filters;
    private transient int nFeatures;

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        FeatureSchema schema = FeatureSchema.load();
        filters = new Filters(schema);
        nFeatures = schema.size();
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> r, Collector<Envelope> out) throws IOException {
        long ingest = BenchClock.nowUs();
        TransactionEvent e = TransactionEvent.parseFrom(r.value());
        Header th = r.headers().lastHeader("x-bench-trace");
        Envelope env = envelope(e, r.value(), th == null ? null : th.value(), r.headers().lastHeader("x-mirrored") != null,
                filters, nFeatures, ingest);
        env.backfill = r.headers().lastHeader("x-backfill") != null;
        out.collect(env);
    }

    /** Build an envelope (shared with tests). */
    public static Envelope envelope(TransactionEvent e, byte[] raw, byte[] trace, boolean mirrored, Filters filters,
                                    int nFeatures, long ingest) {
        Envelope env = new Envelope();
        env.event = raw;
        env.trace = trace;
        env.mirrored = mirrored;
        env.eventId = e.getEventId();
        env.eventTimeUs = e.getEventTimeUs();
        env.customerId = e.getCustomerId();
        env.cardId = e.getCardId();
        env.deviceId = e.getDeviceId();
        env.ip = Integer.toUnsignedLong(e.getIpV4());
        env.merchantId = e.getMerchantId();
        env.terminalId = e.getTerminalId();
        env.payeeId = e.getPayeeId();
        env.filterMask = filters.mask(EventView.of(e));
        env.features = new float[nFeatures];
        env.ingestUs = ingest;
        int dom = BenchClock.domain();
        env.span(Stage.INGEST_VALUE, 4, dom, ingest, 0);
        long now = BenchClock.nowUs();
        env.span(Stage.DESERIALIZE_VALUE, 0, dom, ingest, now - ingest);
        env.lastEmitUs = now;
        return env;
    }

    @Override
    public TypeInformation<Envelope> getProducedType() {
        return TypeInformation.of(Envelope.class);
    }
}
