package io.realy.flink;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.realy.contracts.flows.v1.TxnContext;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.scoring.binding.PipelineBinding;
import io.realy.scoring.features.EntityAggregator;
import io.realy.scoring.features.EventView;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.rules.Filters;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

/**
 * The golden multi-message stream (contracts/testdata/golden/flows) through the real job graph on a
 * MiniCluster with RocksDB: the assembler (keyed by customer) then the keyed feature stages (the
 * customer stage reusing the assembler's keying). Every assembled transaction must come out as the twin
 * assembled it, with its context, and its customer and card features must equal sequential aggregation
 * over the twin's order.
 */
class FlowJobTest {
    static Path dir() {
        Path p = Path.of("").toAbsolutePath();
        while (p != null && !Files.isDirectory(p.resolve("contracts/testdata/golden/flows"))) p = p.getParent();
        return p.resolve("contracts/testdata/golden/flows");
    }

    record Rec(String type, byte[] payload) {}

    static List<Rec> records(Path d) throws Exception {
        List<Rec> out = new ArrayList<>();
        try (DataInputStream in = new DataInputStream(new GZIPInputStream(Files.newInputStream(d.resolve("records.bin.gz"))))) {
            while (true) {
                int tl;
                try {
                    tl = Short.toUnsignedInt(Short.reverseBytes(in.readShort()));
                } catch (EOFException eof) {
                    return out;
                }
                String type = new String(in.readNBytes(tl), StandardCharsets.UTF_8);
                out.add(new Rec(type, in.readNBytes(Integer.reverseBytes(in.readInt()))));
            }
        }
    }

    /** The transaction contracts the golden stream was generated with (its manifest). */
    static List<String> contracts(Path d) throws Exception {
        List<String> out = new ArrayList<>();
        new ObjectMapper().readTree(Files.readString(d.resolve("manifest.json"))).path("transactions")
                .forEach(x -> out.add(x.asText()));
        return out.isEmpty() ? List.of("a2a_transfer") : out;
    }

    @Test
    void assemblesTheGoldenStreamAndComputesFeaturesInItsOrder() throws Exception {
        run(false);
    }

    /** Re-scored stages too: read-only, their features must be the twin's read of the state. */
    @Test
    void scoredStagesReadTheirFeaturesWithoutFoldingThemIn() throws Exception {
        run(true);
    }

    void run(boolean scoreStages) throws Exception {
        Path d = dir();
        FeatureSchema schema = FeatureSchema.load();
        Filters filters = new Filters(schema);
        PipelineBinding pipeline = PipelineBinding.current();
        List<Envelope> input = new ArrayList<>();
        for (Rec r : records(d)) {
            if (r.type().isEmpty()) {
                TransactionEvent e = TransactionEvent.parseFrom(r.payload());
                input.add(EventDeserializer.envelope(e, r.payload(), null, false, schema, filters, 1));
            } else {
                input.add(EventDeserializer.part(pipeline, schema, r.type(), r.payload(), null, 1));
            }
        }
        // the twin's outputs (re-scored stages are off in this job), in its order
        List<JsonNode> want = new ArrayList<>();
        ObjectMapper om = new ObjectMapper();
        try (InputStream in = new GZIPInputStream(Files.newInputStream(d.resolve("expected.jsonl.gz")))) {
            for (String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
                if (line.isBlank()) continue;
                JsonNode w = om.readTree(line);
                if (scoreStages || !"stage".equals(w.get("kind").asText())) want.add(w);
            }
        }
        Configuration conf = new Configuration();
        conf.setString("state.backend.type", "rocksdb");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        int custOrd = schema.rowPlan().entityOrdinal("customer");
        var assembled = env.fromData(input).setParallelism(1)
                .keyBy(new FraudJob.EntityKey(custOrd))
                .process(new AssembleFunction(contracts(d), null, scoreStages)).uid("assemble");
        var out = FraudJob.features(assembled, List.of("customer", "card"), false, List.of(), false, null, "customer")
                .executeAndCollect(100_000);

        assertThat(out).hasSameSizeAs(want);
        Map<Long, Envelope> byId = new HashMap<>();
        for (Envelope o : out) byId.put(o.eventId, o);
        assertThat(byId).hasSameSizeAs(want);   // event ids are unique (a stage decision has its own)
        int transfers = 0;
        for (JsonNode w : want) {
            long id = w.get("event_id").asLong();
            Envelope o = byId.get(id);
            assertThat(o).as("event " + id).isNotNull();
            TransactionEvent we = TransactionEvent.parseFrom(HexFormat.of().parseHex(w.get("event").asText()));
            assertThat(TransactionEvent.parseFrom(o.event)).as("event " + id).isEqualTo(we);
            if (!"passthrough".equals(w.get("kind").asText())) {
                transfers++;
                TxnContext c = TxnContext.parseFrom(o.txn);
                assertThat(c.getPartsPresent()).as("parts of " + id).isEqualTo(w.get("parts_present").asInt());
                assertThat(c.getPartial()).isEqualTo(w.get("partial").asBoolean());
                assertThat(c.getStringsOrDefault("payee_check", "PAYEE_CHECK_UNSPECIFIED"))
                        .isEqualTo(w.get("context").get("payee_check").asText());
            } else {
                assertThat(o.txn).isNull();
            }
            assertThat(o.readOnly).as("read-only " + id).isEqualTo(w.get("read_only").asBoolean());
            // the twin's customer and card features (a stage only reads them)
            var feats = w.get("features");
            for (String ent : List.of("customer", "card")) {
                for (var s : schema.stream(ent)) {
                    assertThat(o.features[s.index()]).as(s.name() + " of " + w.get("kind").asText() + " event " + id)
                            .isEqualTo((float) feats.get(s.name()).asDouble());
                }
            }
        }
        assertThat(transfers).isGreaterThan(scoreStages ? 5000 : 4500);
    }
}
