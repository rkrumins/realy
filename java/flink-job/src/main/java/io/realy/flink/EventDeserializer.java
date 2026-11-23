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
    private final boolean assembling;   // the job assembles multi-message transactions (--assemble)
    private transient io.realy.scoring.binding.PipelineBinding pipeline;
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
        this(schemaYaml, keepRaw, false);
    }

    public EventDeserializer(String schemaYaml, boolean keepRaw, boolean assembling) {
        this.schemaYaml = schemaYaml;
        this.keepRaw = keepRaw;
        this.assembling = assembling;
    }

    private io.realy.scoring.binding.PipelineBinding pipeline() {
        if (pipeline == null) pipeline = io.realy.scoring.binding.PipelineBinding.current();
        return pipeline;
    }

    /**
     * A part of a multi-message transaction: decoded with its binding's codec only far enough to key it
     * (the row's entity keys); the assembler reads the rest.
     */
    public static Envelope part(io.realy.scoring.binding.PipelineBinding pipeline, FeatureSchema schema, String type,
                                byte[] raw, byte[] trace, long ingest) throws IOException {
        var b = pipeline.binding(type);
        var codec = io.realy.scoring.binding.WireCodec.of(b.message);
        Envelope env = new Envelope();
        env.type = type;
        env.event = raw;
        env.trace = trace;
        env.row = schema.rowPlan().row(b, codec, codec.decode(raw));
        env.eventId = env.row.eventId;
        env.eventTimeUs = env.row.timeUs;
        env.ingestUs = ingest;
        env.lastEmitUs = BenchClock.nowUs();
        return env;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        schema = FeatureSchema.of(schemaYaml);
        filters = new Filters(schema);
    }

    private transient io.realy.common.DeadLetters deadLetters;

    /**
     * A record that cannot be decoded (unparseable, a tombstone, an unknown event type, a missing id, a
     * filter that fails on it) goes to the dead-letter topic and is skipped. Throwing here would fail the
     * task, restart it from the checkpoint onto the same record, and loop forever (a poison pill).
     */
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> r, Collector<Envelope> out) throws IOException {
        try {
            deserialize0(r, out);
        } catch (Exception e) {
            if (deadLetters == null) deadLetters = new io.realy.common.DeadLetters("flink");
            FlinkMetrics.get().add("poison_records", 1);
            deadLetters.send(r, e);
        }
    }

    private void deserialize0(ConsumerRecord<byte[], byte[]> r, Collector<Envelope> out) throws IOException {
        if (r.value() == null) throw new IllegalArgumentException("a record without a value (tombstone)");
        long ingest = BenchClock.nowUs();
        Header ty = r.headers().lastHeader("x-event-type");
        Header rid = r.headers().lastHeader("x-run-id");
        if (ty != null) {
            String type = new String(ty.value(), java.nio.charset.StandardCharsets.UTF_8);
            if (!type.equals(pipeline().defaultType)) {
                Header tr = r.headers().lastHeader("x-bench-trace");
                Envelope p = part(pipeline(), schema, type, r.value(), tr == null ? null : tr.value(), ingest);
                p.backfill = r.headers().lastHeader("x-backfill") != null;
                p.runId = rid == null ? null : rid.value();
                out.collect(p);
                return;
            }
        }
        TransactionEvent e = TransactionEvent.parseFrom(r.value());
        // event_id 0 is a real id (the generator's first: gen 0, seq 0); a payment without its partition key
        // or its time is not one (an empty or garbled payload decodes to all defaults)
        if (e.getCustomerId() == 0 || e.getEventTimeUs() == 0) {
            throw new IllegalArgumentException("customer_id or event_time_us is 0 (not a payment event, or a garbled payload)");
        }
        Header th = r.headers().lastHeader("x-bench-trace");
        Envelope env = envelope(e, r.value(), th == null ? null : th.value(), r.headers().lastHeader("x-mirrored") != null,
                schema, filters, ingest);
        env.backfill = r.headers().lastHeader("x-backfill") != null;
        env.runId = rid == null ? null : rid.value();
        // the raw event stays when the assembler re-publishes it (passthrough) or the job scores it
        if (!keepRaw && !assembling) env.event = null;
        out.collect(env);
    }

    /** Build an envelope (shared with tests). */
    public static Envelope envelope(TransactionEvent e, byte[] raw, byte[] trace, boolean mirrored, FeatureSchema schema,
                                    Filters filters, long ingest) {
        return envelope(e, raw, trace, mirrored, schema, filters, ingest, null);
    }

    /** @param txn the context of the transaction the event was assembled from (filters see event.txn) */
    public static Envelope envelope(TransactionEvent e, byte[] raw, byte[] trace, boolean mirrored, FeatureSchema schema,
                                    Filters filters, long ingest, io.realy.contracts.flows.v1.TxnContext txn) {
        Envelope env = new Envelope();
        env.event = raw;
        env.trace = trace;
        env.mirrored = mirrored;
        env.eventId = e.getEventId();
        env.eventTimeUs = e.getEventTimeUs();
        env.row = schema.rowPlan().row(e);
        env.filterMask = filters.mask(EventView.of(e, io.realy.scoring.binding.TxnContexts.view(txn)));
        if (txn != null) env.txn = txn.toByteArray();
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
