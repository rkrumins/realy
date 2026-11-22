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

/**
 * Parses the TransactionEvent once (the only parse in the feature stages: the keyed stages read the
 * decoded {@link io.realy.scoring.features.EventRow}), reads the bench trace header, stamps INGEST and
 * evaluates the CEL filters once.
 */
public final class EventDeserializer implements KafkaRecordDeserializationSchema<Envelope> {
    private final String schemaYaml;
    private final boolean keepRaw;
    private transient Filters filters;
    private transient FeatureSchema schema;

    public EventDeserializer(String schemaYaml) {
        this(schemaYaml, true);
    }

    /**
     * @param keepRaw carry the raw event bytes in the envelope: needed only where the job scores (the
     *                decision record holds the event); in `features` mode no stage reads them after the
     *                source, so leaving them out shrinks every shuffle
     */
    public EventDeserializer(String schemaYaml, boolean keepRaw) {
        this.schemaYaml = schemaYaml;
        this.keepRaw = keepRaw;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        schema = FeatureSchema.of(schemaYaml);
        filters = new Filters(schema);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> r, Collector<Envelope> out) throws IOException {
        long ingest = BenchClock.nowUs();
        TransactionEvent e = TransactionEvent.parseFrom(r.value());
        Header th = r.headers().lastHeader("x-bench-trace");
        Envelope env = envelope(e, r.value(), th == null ? null : th.value(), r.headers().lastHeader("x-mirrored") != null,
                schema, filters, ingest);
        env.backfill = r.headers().lastHeader("x-backfill") != null;
        if (!keepRaw) env.event = null;
        out.collect(env);
    }

    /** Build an envelope (shared with tests). */
    public static Envelope envelope(TransactionEvent e, byte[] raw, byte[] trace, boolean mirrored, FeatureSchema schema,
                                    Filters filters, long ingest) {
        Envelope env = new Envelope();
        env.event = raw;
        env.trace = trace;
        env.mirrored = mirrored;
        env.eventId = e.getEventId();
        env.eventTimeUs = e.getEventTimeUs();
        env.row = schema.rowPlan().row(e);
        env.filterMask = filters.mask(EventView.of(e));
        env.features = new float[schema.size()];
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
