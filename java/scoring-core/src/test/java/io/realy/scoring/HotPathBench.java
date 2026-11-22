package io.realy.scoring;

import io.realy.contracts.bench.v1.BenchTrace;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.contracts.scoring.v1.IngressMode;
import io.realy.scoring.features.EntityAggregator;
import io.realy.scoring.features.EventView;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.model.ModelClient;
import io.realy.scoring.model.ProxyModel;
import io.realy.scoring.rules.Filters;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Per-event CPU of the hot path on the golden payments set (run with -Dbench=true; prints one JSON line,
 * also written to -Dbench.out=<file>). Single thread, warmed up, in-process store and model, so it
 * measures our code only: the numbers to compare before and after a change on the same host.
 */
class HotPathBench {
    static double usPer(long t0, int n) {
        return (System.nanoTime() - t0) / 1e3 / n;
    }

    @Test
    @Tag("bench")
    void hotPathCostPerEvent() throws Exception {
        if (!Boolean.getBoolean("bench")) return;
        Path d = GoldenPaymentsTest.dir();
        List<TransactionEvent> events = GoldenPaymentsTest.events(d);
        List<byte[]> raw = new ArrayList<>();
        for (TransactionEvent e : events) raw.add(e.toByteArray());
        FeatureSchema schema = FeatureSchema.load();
        Filters filters = new Filters(schema);
        int n = events.size();
        int rounds = 6;
        Map<String, Double> out = new LinkedHashMap<>();

        // 1. parse + event view (what every Flink stage and the gateway pay per event today)
        double parse = 0, view = 0, mask = 0;
        for (int r = 0; r < rounds; r++) {
            long t0 = System.nanoTime();
            long sink = 0;
            for (byte[] b : raw) sink += TransactionEvent.parseFrom(b).getEventId();
            parse = usPer(t0, n);
            t0 = System.nanoTime();
            for (TransactionEvent e : events) sink += EventView.of(e).size();
            view = usPer(t0, n);
            t0 = System.nanoTime();
            for (TransactionEvent e : events) sink += filters.mask(EventView.of(e));
            mask = usPer(t0, n);
            if (sink == 42) System.out.println();
        }
        out.put("parse_us", parse);
        out.put("event_view_us", view);
        out.put("filter_mask_us", mask);

        // 2. keyed aggregation per entity (the Flink stages, state in memory instead of RocksDB)
        float[] buf = new float[schema.size()];
        long[] masks = new long[n];
        for (int i = 0; i < n; i++) masks[i] = filters.mask(EventView.of(events.get(i)));
        for (String ent : schema.streamByEntity().keySet()) {
            EntityAggregator agg = new EntityAggregator(schema, ent);
            double us = 0;
            for (int r = 0; r < rounds; r++) {
                Map<Long, byte[]> st = new HashMap<>();
                long t0 = System.nanoTime();
                for (int i = 0; i < n; i++) {
                    TransactionEvent e = events.get(i);
                    long key = EventView.entityKey(e, ent);
                    if (key == 0) continue;
                    st.put(key, agg.process(st.get(key), e, masks[i], buf));
                }
                us = usPer(t0, n);
            }
            out.put("agg_" + ent + "_us", us);
        }

        // 3. the gateway's scoring path end to end (store lookup, assembly, model, rules, policy, record)
        var res = GoldenPaymentsTest.replay(d);   // fills the store with the blobs the feature job writes
        var holder = new ScoringConfigHolder(ScoringConfigHolder.defaults());
        holder.update(Files.readString(d.resolve("scoring_config.json")));
        ProxyModel model = ProxyModel.load(null).bind(schema.names());
        ModelClient client = (f, timeout) -> CompletableFuture.completedFuture(
                new ModelClient.Result(model.score(f, 0, f.length), model.name, model.version, 0, 0, 1, 0));
        var orch = new ScoringOrchestrator(schema, res.store(), client, holder, "bench", 1);
        double score = 0;
        for (int r = 0; r < rounds; r++) {
            long t0 = System.nanoTime();
            for (TransactionEvent e : events) {
                orch.score(ScoringOrchestrator.Request.of(e, BenchTrace.newBuilder(), io.realy.common.BenchClock.nowUs(),
                        IngressMode.ASYNC_KAFKA_GATEWAY)).get();
            }
            score = usPer(t0, n);
        }
        out.put("gateway_score_us", score);
        StringBuilder json = new StringBuilder("{\"bench\":\"hot-path\",\"events\":").append(n);
        out.forEach((k, v) -> json.append(",\"").append(k).append("\":").append(String.format("%.3f", v)));
        json.append('}');
        System.out.println(json);
        String file = System.getProperty("bench.out", "");
        if (!file.isBlank()) Files.writeString(Path.of(file), json + "\n");
    }
}
