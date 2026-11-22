package io.realy.scoring;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.realy.contracts.flows.v1.TxnState;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.scoring.binding.AssemblerCore;
import io.realy.scoring.binding.Assembly;
import io.realy.scoring.binding.PipelineBinding;
import io.realy.scoring.binding.TxnContexts;
import io.realy.scoring.binding.WireCodec;
import io.realy.scoring.rules.Cel;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.Test;

/**
 * The Java assembler against the Python twin on the golden multi-message stream
 * (contracts/testdata/golden/flows, `python -m realy.golden --flows`): every output, in order, with the
 * same unified event, context, stage, parts present and partial flag.
 */
class AssemblyTest {
    static final Map<String, Cel.Predicate> WHEN = new ConcurrentHashMap<>();
    static final Assembly.StageMatcher MATCHER = (expr, view) -> WHEN.computeIfAbsent(expr, Cel::compile).test(Map.of("event", view));

    /** One key's open transactions in a map (creation order kept, as the twin closes them at the end). */
    static final class MapStore implements AssemblerCore.Store {
        final Map<String, TxnState> m = new LinkedHashMap<>();

        public Iterable<String> keys() {
            return new ArrayList<>(m.keySet());
        }

        public TxnState get(String k) {
            return m.get(k);
        }

        public void put(String k, TxnState s) {
            m.put(k, s);
        }

        public void remove(String k) {
            m.remove(k);
        }
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
                int pl = Integer.reverseBytes(in.readInt());
                out.add(new Rec(type, in.readNBytes(pl)));
            }
        }
    }

    @Test
    void javaAssemblerEqualsThePythonTwinOnTheGoldenStream() throws Exception {
        Path d = GoldenPaymentsTest.dir().getParent().resolve("flows");
        var pipeline = PipelineBinding.current();
        AssemblerCore core = new AssemblerCore(List.of("a2a_transfer"), MATCHER);
        Map<Long, MapStore> stores = new LinkedHashMap<>();
        List<Map<String, Object>> got = new ArrayList<>();
        for (Rec r : records(d)) {
            String type = r.type().isEmpty() ? "payment" : r.type();
            var binding = io.realy.scoring.binding.EventBinding.load(type);
            WireCodec c = WireCodec.of(binding.message);
            Object[] v = c.decode(r.payload());
            long cust = ((Number) v[c.indexOf(binding.entities.get(pipeline.partitionKey))]).longValue();
            long t = ((Number) v[c.indexOf(binding.timeField)]).longValue();
            MapStore st = stores.computeIfAbsent(cust, k -> new MapStore());
            for (Assembly.Out o : core.process(st, type, r.payload(), t, null, 0, 0, false)) got.add(out(o));
            if (!core.isPart(type)) {
                TransactionEvent e = TransactionEvent.parseFrom(r.payload());
                Map<String, Object> p = new HashMap<>();
                p.put("kind", "passthrough");
                p.put("event", e);
                p.put("context", TxnContexts.defaults());
                got.add(p);
            }
        }
        // the twin closes what is still waiting in creation order (all keys)
        List<String[]> order = new ArrayList<>();
        for (var en : stores.entrySet()) for (String k : en.getValue().keys()) order.add(new String[]{Long.toString(en.getKey()), k});
        for (MapStore st : stores.values()) for (Assembly.Out o : core.closeAll(st)) got.add(out(o));

        List<JsonNode> want = new ArrayList<>();
        ObjectMapper om = new ObjectMapper();
        try (InputStream in = new GZIPInputStream(Files.newInputStream(d.resolve("expected.jsonl.gz")))) {
            for (String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
                if (!line.isBlank()) want.add(om.readTree(line));
            }
        }
        assertThat(got).hasSameSizeAs(want);
        int partial = 0;
        for (int i = 0; i < want.size(); i++) {
            JsonNode w = want.get(i);
            Map<String, Object> g = got.get(i);
            String at = "output " + i + " (" + w.get("kind").asText() + " event " + w.get("event_id").asText() + ")";
            assertThat(g.get("kind")).as(at).isEqualTo(w.get("kind").asText());
            TransactionEvent we = TransactionEvent.parseFrom(HexFormat.of().parseHex(w.get("event").asText()));
            assertThat(g.get("event")).as(at).isEqualTo(we);
            if (!"passthrough".equals(w.get("kind").asText())) {
                assertThat(g.get("stage")).as(at).isEqualTo(w.get("stage").asText());
                assertThat(g.get("parts")).as(at).isEqualTo(w.get("parts_present").asInt());
                assertThat(g.get("partial")).as(at).isEqualTo(w.get("partial").asBoolean());
                assertThat(g.get("read_only")).as(at).isEqualTo(w.get("read_only").asBoolean());
                if (w.get("partial").asBoolean()) partial++;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> ctx = (Map<String, Object>) g.get("context");
            w.get("context").fields().forEachRemaining(e -> {
                Object x = ctx.get(e.getKey());
                JsonNode y = e.getValue();
                if (y.isBoolean()) assertThat(x).as(at + " txn." + e.getKey()).isEqualTo(y.asBoolean());
                else if (y.isTextual()) assertThat(x).as(at + " txn." + e.getKey()).isEqualTo(y.asText());
                else if (y.isIntegralNumber()) assertThat(((Number) x).longValue()).as(at + " txn." + e.getKey()).isEqualTo(y.asLong());
                else assertThat(((Number) x).doubleValue()).as(at + " txn." + e.getKey()).isCloseTo(y.asDouble(), org.assertj.core.data.Offset.offset(1e-9));
            });
            assertThat(ctx.keySet()).as(at).containsAll(iter(w.get("context").fieldNames()));
        }
        assertThat(partial).isGreaterThan(0);
    }

    @Test
    void aScoredStageIsTimedFromItsOwnMessage() throws Exception {
        var a = new Assembly(io.realy.scoring.binding.TransactionContract.load("a2a_transfer"), MATCHER);
        TxnState.Builder st = TxnState.newBuilder().setTxnId(77);
        long t0 = 1_800_000_000_000_000L;
        byte[] init = io.realy.contracts.flows.v1.PaymentInitiation.newBuilder().setEventId(77).setEventTimeUs(t0)
                .setPaymentId(77).setCustomerId(5).setPayeeId(9).build().toByteArray();
        byte[] check = io.realy.contracts.flows.v1.PayeeCheck.newBuilder().setEventId(78).setEventTimeUs(t0 + 50_000)
                .setPaymentId(77).setCustomerId(5).setPayeeId(9).build().toByteArray();
        byte[] sub = io.realy.contracts.flows.v1.PaymentSubmission.newBuilder().setEventId(79).setEventTimeUs(t0 + 9_000_000)
                .setPaymentId(77).setCustomerId(5).setPayeeId(10).build().toByteArray();
        byte[] trInit = {1, 2, 3};
        byte[] trSub = {4, 5, 6};
        assertThat(a.onPart(st, "initiation", init, t0, trInit, 100, 0, false)).isEmpty();
        var asm = a.onPart(st, "payee_check", check, t0 + 50_000, null, 0, 0, false);
        assertThat(asm).hasSize(1);
        assertThat(asm.get(0).trace()).isEqualTo(trInit);            // the transaction: from its trigger
        assertThat(asm.get(0).ingestUs()).isEqualTo(100);
        var stage = a.onPart(st, "submission", sub, t0 + 9_000_000, trSub, 900, 0, false);
        assertThat(stage).hasSize(1);
        assertThat(stage.get(0).kind()).isEqualTo("stage");
        assertThat(stage.get(0).readOnly()).isTrue();
        assertThat(stage.get(0).trace()).isEqualTo(trSub);            // the stage: from its own message
        assertThat(stage.get(0).ingestUs()).isEqualTo(900);
        assertThat(stage.get(0).event().getEventId()).isEqualTo(79);
        assertThat(stage.get(0).event().getPayeeId()).isEqualTo(10);
        // state keeps no traces of recorded parts (only a held stage message needs one)
        assertThat(st.getLatestList()).allMatch(p -> p.getTrace().isEmpty());
    }

    static List<String> iter(java.util.Iterator<String> it) {
        List<String> out = new ArrayList<>();
        it.forEachRemaining(out::add);
        return out;
    }

    static Map<String, Object> out(Assembly.Out o) {
        Map<String, Object> m = new HashMap<>();
        m.put("kind", o.kind());
        m.put("event", o.event());
        m.put("stage", o.ctx().getStage());
        m.put("parts", o.ctx().getPartsPresent());
        m.put("partial", o.ctx().getPartial());
        m.put("read_only", o.readOnly());
        m.put("context", TxnContexts.view(o.ctx()));
        return m;
    }
}
