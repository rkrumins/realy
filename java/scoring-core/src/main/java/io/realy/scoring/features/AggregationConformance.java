package io.realy.scoring.features;

import com.fasterxml.jackson.databind.JsonNode;
import io.realy.common.ProtoJson;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Replays a conformance fixture through the registered Java aggregations and lists every value that
 * differs from the Python twin's. Fixtures are written by the Python registry
 * ({@code python -m realy.features.aggregations conformance}, contracts/testdata/aggregations): one event
 * sequence (time, a numeric value, a key, a location, whether the feature's filter matched) and, per
 * aggregation case, the value {@code read} and then {@code apply} gave at each event. Values must agree to
 * 1e-9 (relative), far inside verify's tolerance; the window, eviction and fold logic must agree exactly
 * for that to hold over 60 events.
 *
 * <p>A plugin's own tests call {@link #check(Path)} on the fixture its Python twin wrote.
 */
public final class AggregationConformance {
    private AggregationConformance() {}

    public static final double RTOL = 1e-9;

    /** The mismatches (empty = conformant), at most 20; a case whose aggregation is not registered is one. */
    public static List<String> check(Path fixture) throws IOException {
        JsonNode root = ProtoJson.jsonMapper().readTree(Files.readString(fixture));
        JsonNode events = root.get("events");
        List<String> out = new ArrayList<>();
        for (JsonNode c : root.get("cases")) {
            String agg = c.get("agg").asText();
            if (!Aggregations.has(agg)) {
                out.add(agg + ": not registered in Java (registered: " + Aggregations.names() + ")");
                continue;
            }
            Aggregation a = Aggregations.get(agg);
            long w = Math.round(c.get("window_s").asDouble() * 1_000_000);
            long g = Math.round(c.get("granularity_s").asDouble() * 1_000_000);
            // a case with `"value": null` runs an optional-value aggregation without one (`latest` of the filter bit)
            boolean noValue = c.has("value") && c.get("value").isNull();
            FeatureSpec spec = new FeatureSpec(0, "conformance_" + agg, "stream", "customer", agg,
                    noValue ? null : a.input() == Aggregation.Input.KEY ? "k" : "v", null, w, g, c.get("default").asDouble(),
                    null, null, null);
            int slot = noValue ? -1 : 0;
            Aggregation.Window win = Aggregations.newWindow(a.shape());
            JsonNode steps = c.get("steps");
            for (int i = 0; i < events.size(); i++) {
                JsonNode e = events.get(i);
                EventRow row = new EventRow();
                row.timeUs = e.get("t").asLong();
                row.lat = e.get("lat").asDouble();
                row.lon = e.get("lon").asDouble();
                row.num = new double[] {e.get("value").asDouble()};
                row.val = new long[] {e.get("key").asLong()};
                boolean match = e.get("match").asBoolean();
                double r = a.read(spec, win, row, slot, row.timeUs, match);
                double v = a.apply(spec, win, row, slot, row.timeUs, match);
                compare(out, agg, i, "read", steps.get(i).get("read").asDouble(), r);
                compare(out, agg, i, "apply", steps.get(i).get("apply").asDouble(), v);
                if (out.size() >= 20) return out;
            }
        }
        return out;
    }

    private static void compare(List<String> out, String agg, int i, String op, double want, double got) {
        boolean ok = Double.compare(want, got) == 0
                || Math.abs(want - got) <= RTOL * Math.max(1.0, Math.max(Math.abs(want), Math.abs(got)));
        if (!ok) out.add(agg + " event " + i + " " + op + ": python " + want + ", java " + got);
    }

    /** {@code AggregationConformance <fixture.json>...}: exit 1 on any mismatch. */
    public static void main(String[] args) throws IOException {
        int bad = 0;
        for (String f : args) {
            List<String> m = check(Path.of(f));
            System.out.println(f + ": " + (m.isEmpty() ? "conformant" : m.size() + " mismatches"));
            m.forEach(x -> System.out.println("  " + x));
            bad += m.size();
        }
        System.exit(bad == 0 ? 0 : 1);
    }
}
