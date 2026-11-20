package io.realy.scoring.features;

import com.fasterxml.jackson.databind.JsonNode;
import io.realy.common.Env;
import io.realy.common.ProtoJson;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Java mirror of python/realy/features/schema.py (same file, same validation). */
public final class FeatureSchema {
    public static final List<String> ENTITY_ORDER =
            List.of("customer", "card", "device", "ip", "merchant", "terminal", "payee");
    private static final Pattern DUR = Pattern.compile("^(\\d+)(s|m|h|d)$");

    public final int version;
    public final Map<String, String> entityKeys;        // entity -> event field
    public final Map<String, String> filters;           // name -> CEL
    public final List<String> filterNames;              // bit index order
    public final List<FeatureSpec> features;
    public final List<String> countries;
    public final Map<String, List<String>> profileAttributes;
    private final Map<String, FeatureSpec> byName = new LinkedHashMap<>();
    private final Map<String, List<FeatureSpec>> streamByEntity = new LinkedHashMap<>();
    private final Map<String, List<FeatureSpec>> profileByEntity = new LinkedHashMap<>();

    private FeatureSchema(JsonNode root) {
        version = root.get("schema_version").asInt();
        Map<String, String> ek = new LinkedHashMap<>();
        root.get("entities").fields().forEachRemaining(e -> ek.put(e.getKey(), e.getValue().get("key").asText()));
        entityKeys = Collections.unmodifiableMap(ek);
        Map<String, String> fl = new LinkedHashMap<>();
        root.get("filters").fields().forEachRemaining(e -> fl.put(e.getKey(), e.getValue().asText()));
        filters = Collections.unmodifiableMap(fl);
        filterNames = List.copyOf(fl.keySet());
        if (filterNames.size() > 63) throw new IllegalArgumentException("at most 63 filters supported");
        List<String> cs = new ArrayList<>();
        root.get("countries").forEach(c -> cs.add(c.asText()));
        countries = List.copyOf(cs);
        Map<String, List<String>> pa = new LinkedHashMap<>();
        JsonNode paNode = root.get("profile_attributes");
        if (paNode != null) {
            paNode.fields().forEachRemaining(e -> {
                List<String> l = new ArrayList<>();
                e.getValue().forEach(v -> l.add(v.asText()));
                pa.put(e.getKey(), List.copyOf(l));
            });
        }
        profileAttributes = Collections.unmodifiableMap(pa);
        List<FeatureSpec> fs = new ArrayList<>();
        int i = 0;
        for (JsonNode f : root.get("features")) {
            String src = f.get("source").asText();
            String name = f.get("name").asText();
            double def = f.has("default") ? f.get("default").asDouble() : 0.0;
            FeatureSpec spec;
            switch (src) {
                case "stream" -> {
                    String entity = f.get("entity").asText();
                    if (!entityKeys.containsKey(entity)) throw new IllegalArgumentException(name + ": unknown entity " + entity);
                    String filter = text(f, "filter");
                    if (filter != null && !filters.containsKey(filter)) throw new IllegalArgumentException(name + ": unknown filter " + filter);
                    long w = parseDurationUs(f.get("window").asText());
                    long g = parseDurationUs(f.get("granularity").asText());
                    if (w % g != 0) throw new IllegalArgumentException(name + ": window must be a multiple of granularity");
                    spec = new FeatureSpec(i, name, src, entity, f.get("agg").asText(), text(f, "value"), filter, w, g, def, null, null, null);
                }
                case "profile" -> spec = new FeatureSpec(i, name, src, f.get("entity").asText(), null, null, null, 0, 0, def,
                        f.get("field").asText(), null, null);
                case "derived" -> {
                    String fnName = text(f, "fn");
                    String expr = text(f, "expr");
                    if ((fnName == null) == (expr == null))
                        throw new IllegalArgumentException(name + ": a derived feature needs exactly one of `fn` or `expr`");
                    spec = new FeatureSpec(i, name, src, null, null, null, null, 0, 0, def, null, fnName,
                            expr == null ? null : io.realy.scoring.rules.Cel.compileNumeric(expr));
                }
                default -> throw new IllegalArgumentException(name + ": unknown source " + src);
            }
            fs.add(spec);
            byName.put(name, spec);
            i++;
        }
        features = List.copyOf(fs);
        for (String e : ENTITY_ORDER) {
            List<FeatureSpec> s = features.stream().filter(x -> x.isStream() && e.equals(x.entity())).toList();
            if (!s.isEmpty()) streamByEntity.put(e, s);
            List<FeatureSpec> p = features.stream().filter(x -> x.isProfile() && e.equals(x.entity())).toList();
            if (!p.isEmpty()) profileByEntity.put(e, p);
        }
    }

    private static String text(JsonNode n, String field) {
        JsonNode v = n.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }

    public static long parseDurationUs(String s) {
        Matcher m = DUR.matcher(s.trim());
        if (!m.matches()) throw new IllegalArgumentException("bad duration " + s);
        long n = Long.parseLong(m.group(1));
        return switch (m.group(2)) {
            case "s" -> n * 1_000_000L;
            case "m" -> n * 60_000_000L;
            case "h" -> n * 3_600_000_000L;
            default -> n * 86_400_000_000L;
        };
    }

    public static FeatureSchema parse(String yaml) {
        try {
            return new FeatureSchema(ProtoJson.yamlMapper().readTree(yaml));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static volatile FeatureSchema DEFAULT;

    /** FEATURE_SCHEMA env/path override, else the copy bundled from contracts/ on the classpath. */
    public static FeatureSchema load() {
        if (DEFAULT == null) {
            synchronized (FeatureSchema.class) {
                if (DEFAULT == null) {
                    try {
                        String path = Env.str("FEATURE_SCHEMA", "");
                        if (!path.isEmpty()) {
                            DEFAULT = parse(Files.readString(Path.of(path)));
                        } else {
                            try (InputStream in = FeatureSchema.class.getClassLoader()
                                    .getResourceAsStream("realy/contracts/features/feature_schema.yaml")) {
                                if (in == null) throw new IllegalStateException("feature_schema.yaml not on classpath");
                                DEFAULT = parse(new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
                            }
                        }
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                }
            }
        }
        return DEFAULT;
    }

    public int size() {
        return features.size();
    }

    public FeatureSpec get(String name) {
        FeatureSpec s = byName.get(name);
        if (s == null) throw new IllegalArgumentException("unknown feature " + name);
        return s;
    }

    public int indexOf(String name) {
        return get(name).index();
    }

    public List<String> names() {
        return features.stream().map(FeatureSpec::name).toList();
    }

    public Map<String, List<FeatureSpec>> streamByEntity() {
        return streamByEntity;
    }

    public List<FeatureSpec> stream(String entity) {
        return streamByEntity.getOrDefault(entity, List.of());
    }

    public List<FeatureSpec> profile(String entity) {
        return profileByEntity.getOrDefault(entity, List.of());
    }

    public Map<String, List<FeatureSpec>> profileByEntity() {
        return profileByEntity;
    }

    public int filterBit(String filterName) {
        return filterNames.indexOf(filterName);
    }

    public int countryIndex(String code) {
        return countries.indexOf(code);
    }
}
