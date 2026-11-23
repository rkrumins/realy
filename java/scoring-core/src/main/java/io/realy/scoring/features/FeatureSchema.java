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
    /** The entity registry's order (contracts/entities.yaml): the keyed-stage order. */
    public static final List<String> ENTITY_ORDER = io.realy.scoring.binding.EntityRegistry.load().names();
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
    /** Stream features' `event_types` (the types that fold into them), by feature index; absent = the default type. */
    private final Map<Integer, List<String>> onTypes = new java.util.HashMap<>();
    /** Stream features' `gate:` (the schema index of the feature that must be non-zero for them to fold), by feature index. */
    private final Map<Integer, Integer> gates = new java.util.HashMap<>();
    private final Map<Integer, String> gateNames = new java.util.LinkedHashMap<>();
    /** Scoped filters' `event_types` (the types they are evaluated for); absent = the default type. */
    public final Map<String, List<String>> filterOn = new LinkedHashMap<>();

    private FeatureSchema(JsonNode root) {
        version = root.get("schema_version").asInt();
        Map<String, String> ek = new LinkedHashMap<>();
        root.get("entities").fields().forEachRemaining(e -> ek.put(e.getKey(), e.getValue().get("key").asText()));
        entityKeys = Collections.unmodifiableMap(ek);
        Map<String, String> fl = new LinkedHashMap<>();
        root.get("filters").fields().forEachRemaining(e -> {
            JsonNode v = e.getValue();
            if (v.isObject()) {                 // {when: CEL, event_types: [types]}: evaluated for those types only
                if (!v.has("when") || v.has("on"))
                    throw new IllegalArgumentException("filter " + e.getKey() + ": a scoped filter is {when: <CEL>, event_types: [types]}");
                fl.put(e.getKey(), v.get("when").asText());
                if (v.hasNonNull("event_types")) {
                    List<String> types = new ArrayList<>();
                    v.get("event_types").forEach(x -> types.add(x.asText()));
                    filterOn.put(e.getKey(), List.copyOf(types));
                }
            } else {
                fl.put(e.getKey(), v.asText());
            }
        });
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
                    String agg = text(f, "agg");
                    if (agg == null || !Aggregations.has(agg))
                        throw new IllegalArgumentException(name + ": unknown aggregation " + agg + " (registered: " + Aggregations.names() + ")");
                    Aggregation a = Aggregations.get(agg);
                    // a window is required, except by an aggregation that holds a value until it changes (`latest`);
                    // shapes without buckets need no granularity (it defaults to the window)
                    long w = text(f, "window") == null ? 0 : parseDurationUs(f.get("window").asText());
                    boolean bucketless = a.shape() == Aggregation.Shape.LAST || a.shape() == Aggregation.Shape.LAST_GEO;
                    long g = text(f, "granularity") != null ? parseDurationUs(f.get("granularity").asText()) : bucketless ? w : -1;
                    if (w == 0 && !a.sticky(new FeatureSpec(i, name, src, entity, agg, null, null, 0, 0, def, null, null, null)))
                        throw new IllegalArgumentException(name + ": aggregation " + agg + " needs a `window`");
                    if (g < 0) throw new IllegalArgumentException(name + ": aggregation " + agg + " needs a `granularity`");
                    if (g > 0 ? w % g != 0 : w != 0) throw new IllegalArgumentException(name + ": window must be a multiple of granularity");
                    Aggregation.Input in = a.input();
                    if ((in == Aggregation.Input.NUMERIC || in == Aggregation.Input.KEY) && text(f, "value") == null)
                        throw new IllegalArgumentException(name + ": aggregation " + agg + " needs `value:` (the event field it reads)");
                    spec = new FeatureSpec(i, name, src, entity, agg, text(f, "value"), filter, w, g, def, null, null, null);
                    if (f.has("on")) throw new IllegalArgumentException(name + ": name the folding types with `event_types:` (not `on:`)");
                    JsonNode on = f.get("event_types");
                    if (on != null && !on.isNull()) {
                        if (!on.isArray() || on.isEmpty())
                            throw new IllegalArgumentException(name + ": `event_types` lists the event types that fold into it, e.g. [payment, login]");
                        List<String> types = new ArrayList<>();
                        on.forEach(x -> types.add(x.asText()));
                        onTypes.put(i, List.copyOf(types));
                    }
                    String gate = text(f, "gate");
                    if (gate != null) gateNames.put(i, gate);
                }
                case "profile" -> spec = new FeatureSpec(i, name, src, f.get("entity").asText(), null, null, null, 0, 0, def,
                        f.get("field").asText(), null, null);
                case "derived" -> {
                    String fnName = text(f, "fn");
                    String expr = text(f, "expr");
                    if ((fnName == null) == (expr == null))
                        throw new IllegalArgumentException(name + ": a derived feature needs exactly one of `fn` or `expr`");
                    if (fnName != null && !DerivedFns.has(fnName))
                        throw new IllegalArgumentException(name + ": unknown derived fn " + fnName + " (registered: " + DerivedFns.names() + ")");
                    spec = new FeatureSpec(i, name, src, null, null, null, null, 0, 0, def, null, fnName,
                            expr == null ? null : io.realy.scoring.rules.Cel.compileNumeric(expr));
                }
                default -> throw new IllegalArgumentException(name + ": unknown source " + src);
            }
            fs.add(spec);
            if (byName.containsKey(name)) throw new IllegalArgumentException("feature defined twice: " + name);
            byName.put(name, spec);
            i++;
        }
        features = List.copyOf(fs);
        var reg = io.realy.scoring.binding.EntityRegistry.load();
        java.util.Set<String> withStream = new java.util.HashSet<>();
        for (FeatureSpec f : features) if (f.isStream()) withStream.add(f.entity());
        // stream features by entity in evaluation order (stage order, hosted entities right after their stage's own)
        for (String e : reg.evaluationOrder(withStream)) {
            List<FeatureSpec> s = features.stream().filter(x -> x.isStream() && e.equals(x.entity())).toList();
            if (!s.isEmpty()) streamByEntity.put(e, s);
        }
        List<String> order = reg.evaluationOrder(withStream);
        for (var g : gateNames.entrySet()) gates.put(g.getKey(), gateIndex(features.get(g.getKey()), g.getValue(), byName, order));
        for (String e : reg.names()) {
            List<FeatureSpec> p = features.stream().filter(x -> x.isProfile() && e.equals(x.entity())).toList();
            if (!p.isEmpty()) profileByEntity.put(e, p);
        }
    }

    /**
     * A `gate:` names a stream feature computed before this one: of an earlier keyed stage (registry order), or of
     * the same entity and earlier in the schema. Its value is then in the feature vector when this one folds.
     */
    private static int gateIndex(FeatureSpec f, String gate, Map<String, FeatureSpec> byName, List<String> order) {
        FeatureSpec g = byName.get(gate);
        if (g == null || !g.isStream() || g.index() >= f.index())
            throw new IllegalArgumentException(f.name() + ": `gate` " + gate + " is not a stream feature defined before it");
        if (order.indexOf(g.entity()) > order.indexOf(f.entity()))
            throw new IllegalArgumentException(f.name() + ": `gate` " + gate + " is a " + g.entity() + " feature, computed after the "
                    + f.entity() + " stage (a gate must come from this stage or an earlier one: " + order + ")");
        return g.index();
    }

    /** The schema index of stream feature {@code s}'s `gate:` (it folds only while that feature is non-zero), or -1. */
    public int gate(FeatureSpec s) {
        Integer g = gates.get(s.index());
        return g == null ? -1 : g;
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

    /**
     * The schema of a YAML text: one document, or a bundle ({@link #loadText}) of a base followed by the
     * schemas that extend it, merged as the Python loader merges them (entities, filters and profile
     * attributes add up, a filter may not be redefined differently, countries and the version come from the
     * last that sets them, features are appended in order).
     */
    public static FeatureSchema parse(String yaml) {
        try {
            List<JsonNode> docs = new ArrayList<>();
            try (com.fasterxml.jackson.databind.MappingIterator<JsonNode> it =
                         ProtoJson.yamlMapper().readerFor(JsonNode.class).readValues(yaml)) {
                while (it.hasNext()) {
                    JsonNode d = it.next();
                    if (d != null && !d.isNull() && !d.isMissingNode()) docs.add(d);
                }
            }
            return new FeatureSchema(docs.size() == 1 ? docs.get(0) : merge(docs));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static JsonNode merge(List<JsonNode> docs) {
        var m = ProtoJson.yamlMapper();
        var out = m.createObjectNode();
        var entities = m.createObjectNode();
        var filters = m.createObjectNode();
        var attrs = m.createObjectNode();
        var feats = m.createArrayNode();
        for (JsonNode d : docs) {
            d.path("entities").fields().forEachRemaining(e -> entities.set(e.getKey(), e.getValue()));
            d.path("filters").fields().forEachRemaining(e -> {
                JsonNode prev = filters.get(e.getKey());
                if (prev != null && !prev.equals(e.getValue()))
                    throw new IllegalArgumentException("filter " + e.getKey() + " is redefined differently by an extending schema");
                filters.set(e.getKey(), e.getValue());
            });
            d.path("profile_attributes").fields().forEachRemaining(e -> attrs.set(e.getKey(), e.getValue()));
            if (d.has("countries") && d.get("countries").size() > 0) out.set("countries", d.get("countries"));
            if (d.has("schema_version")) out.set("schema_version", d.get("schema_version"));
            d.path("features").forEach(feats::add);
        }
        out.set("entities", entities);
        out.set("filters", filters);
        out.set("profile_attributes", attrs);
        out.set("features", feats);
        return out;
    }

    /** The event types that fold into stream feature {@code s} (`event_types`), or null for the pipeline's default type. */
    public List<String> on(FeatureSpec s) {
        return onTypes.get(s.index());
    }

    private static volatile FeatureSchema DEFAULT;
    private static final java.util.concurrent.ConcurrentHashMap<String, FeatureSchema> PARSED =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * The schema YAML this process resolves (FEATURE_SCHEMA path, else the bundled copy). The Flink job
     * resolves it once in its main and ships the text to every operator ({@link #of}), so TaskManagers
     * never resolve their own copy and cannot diverge from the job that planned the graph.
     */
    public static String loadText() {
        try {
            String path = Env.str("FEATURE_SCHEMA", "");
            if (!path.isEmpty()) return bundle(Path.of(path));
            String pf = io.realy.scoring.binding.PipelineBinding.current().features;
            if (pf != null && pf.startsWith("contracts/features/")) return bundle(pf.substring("contracts/".length()));
            return bundle("features/feature_schema.yaml");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * A schema file preceded by what it `extends:` (recursively), as YAML documents separated by "---": the
     * text both languages parse and hash (python/realy/features/schema.py schema_bundle). A file that
     * extends nothing is its own text.
     */
    static String bundle(Path file) throws IOException {
        String text = Files.readString(file);
        String base = extendsOf(text);
        return base == null ? text : bundle(file.getParent().resolve(base)) + "\n---\n" + text;
    }

    /** The same from the contracts on the classpath (or REALY_CONTRACTS): {@code rel} under contracts/. */
    public static String bundle(String rel) throws IOException {
        String text = io.realy.scoring.binding.Contracts.text(rel);
        String base = extendsOf(text);
        if (base == null) return text;
        String dir = rel.contains("/") ? rel.substring(0, rel.lastIndexOf('/') + 1) : "";
        return bundle(dir + base) + "\n---\n" + text;
    }

    private static String extendsOf(String text) throws IOException {
        JsonNode r = ProtoJson.yamlMapper().readTree(text);
        return r != null && r.hasNonNull("extends") ? r.get("extends").asText() : null;
    }

    /** The schema of this YAML text, parsed once per process (null = {@link #load()}). */
    public static FeatureSchema of(String yaml) {
        if (yaml == null || yaml.isEmpty()) return load();
        return PARSED.computeIfAbsent(yaml, FeatureSchema::parse);
    }

    /** FEATURE_SCHEMA env/path override, else the copy bundled from contracts/ on the classpath. */
    public static FeatureSchema load() {
        if (DEFAULT == null) {
            synchronized (FeatureSchema.class) {
                if (DEFAULT == null) DEFAULT = parse(loadText());
            }
        }
        return DEFAULT;
    }

    public int size() {
        return features.size();
    }

    private volatile RowPlan rowPlan;

    /** How events are decoded once for the keyed stages (entity keys and the fields stream features read). */
    public RowPlan rowPlan() {
        RowPlan p = rowPlan;
        if (p == null) {
            synchronized (this) {
                if (rowPlan == null) rowPlan = new RowPlan(this);
                p = rowPlan;
            }
        }
        return p;
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
