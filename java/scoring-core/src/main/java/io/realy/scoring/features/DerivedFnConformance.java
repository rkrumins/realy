package io.realy.scoring.features;

import com.fasterxml.jackson.databind.JsonNode;
import io.realy.common.ProtoJson;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Replays a derived-function conformance fixture through the registered Java functions and lists every
 * value that differs from the Python twin's. Fixtures are written by
 * {@code python -m realy.features.derived_fns conformance} (contracts/testdata/derived): rows of an event
 * view and the features before the function, with the value the twin computed. Values agree to 1e-9.
 */
public final class DerivedFnConformance {
    private DerivedFnConformance() {}

    public static List<String> check(Path fixture) throws IOException {
        JsonNode root = ProtoJson.jsonMapper().readTree(Files.readString(fixture));
        List<String> out = new ArrayList<>();
        for (JsonNode c : root.get("cases")) {
            String name = c.get("fn").asText();
            if (DerivedFns.builtin(name) || !DerivedFns.extensions().contains(name)) {
                out.add(name + ": not a registered plugin function in Java (registered: " + DerivedFns.extensions() + ")");
                continue;
            }
            DerivedFn fn = DerivedFns.get(name);
            int i = 0;
            for (JsonNode r : c.get("rows")) {
                Map<String, Object> ev = new LinkedHashMap<>();
                r.get("event").fields().forEachRemaining(x -> ev.put(x.getKey(), value(x.getValue())));
                Map<String, Double> f = new LinkedHashMap<>();
                r.get("f").fields().forEachRemaining(x -> f.put(x.getKey(), x.getValue().asDouble()));
                double want = r.get("value").asDouble();
                double got = fn.apply(ev, f);
                boolean ok = Double.compare(want, got) == 0
                        || Math.abs(want - got) <= AggregationConformance.RTOL * Math.max(1.0, Math.max(Math.abs(want), Math.abs(got)));
                if (!ok) out.add(name + " row " + i + ": python " + want + ", java " + got);
                if (out.size() >= 20) return out;
                i++;
            }
        }
        return out;
    }

    /** JSON as the CEL event view holds it: integers as Long, other numbers as Double, text, booleans. */
    static Object value(JsonNode n) {
        if (n.isIntegralNumber()) return n.asLong();
        if (n.isNumber()) return n.asDouble();
        if (n.isBoolean()) return n.asBoolean();
        if (n.isNull()) return null;
        return n.asText();
    }
}
