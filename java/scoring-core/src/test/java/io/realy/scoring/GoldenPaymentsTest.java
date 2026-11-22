package io.realy.scoring;

import static org.assertj.core.api.Assertions.assertThat;

import io.realy.contracts.bench.v1.BenchTrace;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.contracts.scoring.v1.DecisionRecord;
import io.realy.contracts.scoring.v1.IngressMode;
import io.realy.contracts.features.v1.FeatureVector;
import io.realy.scoring.features.EntityAggregator;
import io.realy.scoring.features.EventView;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.model.ModelClient;
import io.realy.scoring.model.ProxyModel;
import io.realy.scoring.rules.Filters;
import io.realy.scoring.store.OnlineStore;
import io.realy.scoring.store.StoreBlob;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;

/**
 * The golden payments set (contracts/testdata/golden/payments, built by {@code python -m realy.golden}):
 * replays its events through the keyed feature aggregators (as the Flink job does, stage by stage, writing
 * the same online-store blobs) and the scoring orchestrator (as the gateway does, reading them back), and
 * compares the canonical decisions and the final state blobs byte for byte with what was recorded.
 *
 * <p>Any change to the event path that changes a payment decision, a feature value or a state byte fails
 * here. A deliberate change is re-recorded with {@code ./gradlew :scoring-core:test --tests '*Golden*'
 * -Dgolden.record=true} and the diff of java/ reviewed (decisions.tsv shows which events moved).
 */
class GoldenPaymentsTest {
    static final long FIXED_WALL_US = 1_700_000_000_000_000L;

    static Path dir() {
        Path p = Path.of("").toAbsolutePath();
        while (p != null && !Files.isDirectory(p.resolve("contracts/testdata/golden/payments"))) p = p.getParent();
        assertThat(p).as("contracts/testdata/golden/payments above " + Path.of("").toAbsolutePath()).isNotNull();
        return p.resolve("contracts/testdata/golden/payments");
    }

    /** HashMap-backed store: the gateway's view of what the feature job wrote so far. */
    static final class MapStore implements OnlineStore {
        final Map<String, byte[]> kv = new TreeMap<>();

        @Override
        public CompletableFuture<byte[][]> getAll(List<String> keys) {
            byte[][] out = new byte[keys.size()][];
            for (int i = 0; i < out.length; i++) out[i] = kv.get(keys.get(i));
            return CompletableFuture.completedFuture(out);
        }

        @Override
        public CompletableFuture<Void> setAll(List<String> keys, List<byte[]> values, long ttlSeconds) {
            for (int i = 0; i < keys.size(); i++) kv.put(keys.get(i), values.get(i));
            return CompletableFuture.completedFuture(null);
        }
    }

    static List<TransactionEvent> events(Path d) throws IOException {
        List<TransactionEvent> out = new ArrayList<>();
        try (InputStream in = new GZIPInputStream(Files.newInputStream(d.resolve("events.pb.gz")))) {
            TransactionEvent e;
            while ((e = TransactionEvent.parseDelimitedFrom(in)) != null) out.add(e);
        }
        return out;
    }

    static void profiles(Path d, MapStore store) throws IOException {
        try (DataInputStream in = new DataInputStream(new GZIPInputStream(Files.newInputStream(d.resolve("profiles.bin.gz"))))) {
            while (true) {
                int kl;
                try {
                    kl = Short.toUnsignedInt(Short.reverseBytes(in.readShort()));
                } catch (EOFException eof) {
                    return;
                }
                byte[] k = in.readNBytes(kl);
                int bl = Integer.reverseBytes(in.readInt());
                store.kv.put(new String(k, StandardCharsets.UTF_8), in.readNBytes(bl));
            }
        }
    }

    /** Timing and identity fields cleared: what remains is a pure function of the events and config. */
    static DecisionRecord canonical(DecisionRecord d) {
        FeatureVector.Builder fv = d.getFeatures().toBuilder().clearComputedAtUs();
        for (int i = 0; i < fv.getFreshnessCount(); i++) fv.setFreshness(i, fv.getFreshness(i).toBuilder().clearUpdatedAtUs());
        return d.toBuilder().clearDecidedAtUs().clearInstanceId().clearTrace().setFeatures(fv).build();
    }

    record Result(List<DecisionRecord> decisions, MapStore store) {}

    static Result replay(Path d) throws Exception {
        FeatureSchema schema = FeatureSchema.load();
        Filters filters = new Filters(schema);
        MapStore store = new MapStore();
        profiles(d, store);
        var holder = new ScoringConfigHolder(ScoringConfigHolder.defaults());
        holder.update(Files.readString(d.resolve("scoring_config.json")));
        assertThat(holder.get().cfg().getVersion()).as("scoring_config.json applied").isNotBlank();
        ProxyModel model = ProxyModel.load(null).bind(schema.names());
        ModelClient client = (f, timeout) -> CompletableFuture.completedFuture(
                new ModelClient.Result(model.score(f, 0, f.length), model.name, model.version, 0, 0, 1, 0));
        var orch = new ScoringOrchestrator(schema, store, client, holder, "golden", 1);
        // the Flink job's stages: one per entity with stream features, in schema order; customer and card
        // blobs carry their aggregator state (STORE_STATE default), the others carry values only
        Map<String, EntityAggregator> aggs = new LinkedHashMap<>();
        for (String ent : schema.streamByEntity().keySet()) aggs.put(ent, new EntityAggregator(schema, ent));
        List<String> stateEntities = List.of("customer", "card");
        Map<String, Map<Long, byte[]>> state = new LinkedHashMap<>();
        List<DecisionRecord> out = new ArrayList<>();
        float[] buf = new float[schema.size()];
        for (TransactionEvent e : events(d)) {
            // gateway first (it usually reads before the feature job has folded this event in; customer and
            // card features include it by re-windowing their state at the event's time)
            var r = ScoringOrchestrator.Request.of(e, BenchTrace.newBuilder(), io.realy.common.BenchClock.nowUs(),
                    IngressMode.ASYNC_KAFKA_GATEWAY);
            out.add(canonical(orch.score(r).get()));
            // then the feature job
            long mask = filters.mask(EventView.of(e));
            for (var en : aggs.entrySet()) {
                String ent = en.getKey();
                EntityAggregator agg = en.getValue();
                long key = EventView.entityKey(e, ent);
                if (key == 0) {
                    agg.defaults(buf);
                    continue;
                }
                var m = state.computeIfAbsent(ent, k -> new java.util.HashMap<>());
                byte[] next = agg.process(m.get(key), e, mask, buf);
                m.put(key, next);
                int[] idx = agg.featureIndices();
                float[] vals = new float[idx.length];
                for (int i = 0; i < idx.length; i++) vals[i] = buf[idx[i]];
                long asOf = EntityAggregator.lastEventUs(next);
                StoreBlob blob = stateEntities.contains(ent)
                        ? new StoreBlob(schema.version, asOf, FIXED_WALL_US, vals, e.getEventId(), next)
                        : new StoreBlob(schema.version, asOf, FIXED_WALL_US, vals);
                store.kv.put(StoreBlob.streamKey(ent, key), blob.encode());
            }
        }
        return new Result(out, store);
    }

    static String sha(byte[] b) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(b));
    }

    static Map<String, String> digests(Result res) throws Exception {
        ByteArrayOutputStream dec = new ByteArrayOutputStream();
        for (DecisionRecord x : res.decisions()) x.writeDelimitedTo(dec);
        ByteArrayOutputStream st = new ByteArrayOutputStream();
        for (var kv : res.store().kv.entrySet()) {
            if (!kv.getKey().startsWith("fs:")) continue;
            st.write(kv.getKey().getBytes(StandardCharsets.UTF_8));
            st.write(0);
            st.write(kv.getValue());
        }
        Map<String, String> m = new LinkedHashMap<>();
        m.put("decisions", sha(dec.toByteArray()));
        m.put("state", sha(st.toByteArray()));
        return m;
    }

    /** One line per decision: what moved, readable in a diff. */
    static String tsv(List<DecisionRecord> ds) {
        StringBuilder sb = new StringBuilder("event_id\toutcome\tpath\tscore_bits\trule_hits\treason_codes\tfeatures_sha\n");
        for (DecisionRecord d : ds) {
            StringBuilder hits = new StringBuilder();
            for (var h : d.getRuleHitsList()) hits.append(hits.isEmpty() ? "" : ",").append(h.getRuleId());
            ByteBuffer fb = ByteBuffer.allocate(4 * d.getFeatures().getValuesCount()).order(ByteOrder.LITTLE_ENDIAN);
            for (float f : d.getFeatures().getValuesList()) fb.putFloat(f);
            String fsha;
            try {
                fsha = sha(fb.array()).substring(0, 12);
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
            sb.append(Long.toUnsignedString(d.getEventId())).append('\t').append(d.getOutcome().name()).append('\t')
                    .append(d.getPath().name()).append('\t').append(Integer.toHexString(Float.floatToIntBits(d.getScore())))
                    .append('\t').append(hits).append('\t').append(String.join(",", d.getReasonCodesList())).append('\t')
                    .append(fsha).append('\n');
        }
        return sb.toString();
    }

    /** The served feature vectors, n x features float32 little-endian (read by the Python twin test). */
    static byte[] features(List<DecisionRecord> ds) {
        int nf = ds.isEmpty() ? 0 : ds.get(0).getFeatures().getValuesCount();
        ByteBuffer b = ByteBuffer.allocate(4 * nf * ds.size()).order(ByteOrder.LITTLE_ENDIAN);
        for (DecisionRecord d : ds) for (float f : d.getFeatures().getValuesList()) b.putFloat(f);
        return b.array();
    }

    static byte[] gzip(byte[] raw) throws IOException {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        try (GZIPOutputStream g = new GZIPOutputStream(o) {
            {
                def.setLevel(9);
            }
        }) {
            g.write(raw);
        }
        return o.toByteArray();
    }

    @Test
    void decisionsAndStateMatchTheRecordedGoldenSet() throws Exception {
        Path d = dir();
        Result res = replay(d);
        assertThat(res.decisions()).hasSize(events(d).size());
        Map<String, String> got = digests(res);
        String tsv = tsv(res.decisions());
        Path j = d.resolve("java");
        if (Boolean.getBoolean("golden.record")) {
            Files.createDirectories(j);
            StringBuilder s = new StringBuilder();
            got.forEach((k, v) -> s.append(k).append('\t').append(v).append('\n'));
            Files.writeString(j.resolve("digests.tsv"), s.toString());
            Files.write(j.resolve("decisions.tsv.gz"), gzip(tsv.getBytes(StandardCharsets.UTF_8)));
            Files.write(j.resolve("features.f32.gz"), gzip(features(res.decisions())));
            return;
        }
        Map<String, String> want = new LinkedHashMap<>();
        for (String line : Files.readAllLines(j.resolve("digests.tsv"))) {
            String[] kv = line.split("\t");
            if (kv.length == 2) want.put(kv[0], kv[1]);
        }
        if (!got.equals(want)) {
            String[] a = tsv.split("\n");
            String[] b;
            try (InputStream in = new GZIPInputStream(Files.newInputStream(j.resolve("decisions.tsv.gz")))) {
                b = new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n");
            }
            List<String> diff = new ArrayList<>();
            for (int i = 0; i < Math.max(a.length, b.length) && diff.size() < 10; i++) {
                String x = i < a.length ? a[i] : "<none>", y = i < b.length ? b[i] : "<none>";
                if (!x.equals(y)) diff.add("recorded: " + y + "\n     now: " + x);
            }
            assertThat(got).as("golden digests (first differing decisions:\n" + String.join("\n", diff) + ")").isEqualTo(want);
        }
    }
}
