package io.realy.scoring.binding;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A transaction formed by several linked messages (contracts/transactions/&lt;name&gt;.yaml). Mirrors
 * python/realy/bindings/transactions.py; {@link Assembly} assembles it.
 */
public final class TransactionContract {
    /** unified field &lt;- role.field | const | role.field mapped through an enum-name table. */
    public record FieldMap(String target, String role, String field, JsonNode constant, Map<String, String> enumMap) {}

    public record Stage(String name, String role, String when, boolean scored, boolean terminal, List<FieldMap> fields) {}

    /** kind: from | count | changed | elapsed. */
    public record Context(String name, String kind, String role, String field, String betweenA, String betweenB) {}

    public final String name;
    public final String link;
    /** role -> event type, in contract order (bit i = i-th role). */
    public final Map<String, String> roles = new LinkedHashMap<>();
    public final String into;
    public final String trigger;
    public final List<String> require = new ArrayList<>();
    public final List<String> optional = new ArrayList<>();
    public final long waitMs;
    public final String onTimeout;
    public final List<FieldMap> fields;
    public final List<Stage> stages = new ArrayList<>();
    public final List<Context> context = new ArrayList<>();

    TransactionContract(JsonNode r) {
        name = r.get("name").asText();
        link = r.get("link").asText();
        r.get("messages").fields().forEachRemaining(e -> roles.put(e.getKey(), e.getValue().asText()));
        JsonNode a = r.path("assemble");
        into = a.path("into").asText("payment");
        trigger = a.get("trigger").asText();
        a.path("require").forEach(x -> require.add(x.asText()));
        a.path("optional").forEach(x -> optional.add(x.asText()));
        waitMs = a.path("wait_ms").asLong(150);
        onTimeout = a.path("on_timeout").asText("score_partial");
        fields = fieldMaps(a.path("fields"));
        r.path("stages").fields().forEachRemaining(e -> {
            JsonNode s = e.getValue();
            stages.add(new Stage(e.getKey(), s.get("message").asText(), s.hasNonNull("when") ? s.get("when").asText() : null,
                    s.path("scored").asBoolean(false), s.path("terminal").asBoolean(false), fieldMaps(s.path("fields"))));
        });
        r.path("context").fields().forEachRemaining(e -> {
            JsonNode s = e.getValue();
            if (s.has("from")) {
                String[] rf = s.get("from").asText().split("\\.", 2);
                context.add(new Context(e.getKey(), "from", rf[0], rf[1], null, null));
            } else if (s.has("count")) {
                context.add(new Context(e.getKey(), "count", s.get("count").asText(), null, null, null));
            } else if (s.has("changed")) {
                context.add(new Context(e.getKey(), "changed", null, s.get("changed").asText(),
                        s.get("between").get(0).asText(), s.get("between").get(1).asText()));
            } else if (s.has("elapsed")) {
                context.add(new Context(e.getKey(), "elapsed", null, null, s.get("elapsed").get(0).asText(),
                        s.get("elapsed").get(1).asText()));
            } else {
                throw new IllegalArgumentException("transaction " + name + ": context " + e.getKey() + " needs from / count / changed / elapsed");
            }
        });
        if (!roles.containsKey(trigger)) throw new IllegalArgumentException("transaction " + name + ": trigger " + trigger);
    }

    private static List<FieldMap> fieldMaps(JsonNode m) {
        List<FieldMap> out = new ArrayList<>();
        m.fields().forEachRemaining(e -> {
            JsonNode v = e.getValue();
            if (v.isTextual()) {
                String[] rf = v.asText().split("\\.", 2);
                out.add(new FieldMap(e.getKey(), rf[0], rf[1], null, Map.of()));
            } else if (v.has("const")) {
                out.add(new FieldMap(e.getKey(), null, null, v.get("const"), Map.of()));
            } else {
                String[] rf = v.get("from").asText().split("\\.", 2);
                Map<String, String> mp = new LinkedHashMap<>();
                v.path("map").fields().forEachRemaining(x -> mp.put(x.getKey(), x.getValue().asText()));
                out.add(new FieldMap(e.getKey(), rf[0], rf[1], null, mp));
            }
        });
        return out;
    }

    public static TransactionContract load(String name) {
        return new TransactionContract(Contracts.yaml("transactions/" + name + ".yaml"));
    }

    public List<String> roleNames() {
        return new ArrayList<>(roles.keySet());
    }

    public int bit(String role) {
        int i = roleNames().indexOf(role);
        if (i < 0) throw new IllegalArgumentException("transaction " + name + " has no role " + role);
        return 1 << i;
    }

    /** Roles a message of this event type plays (usually one). */
    public List<String> rolesOf(String type) {
        List<String> out = new ArrayList<>();
        roles.forEach((r, t) -> { if (t.equals(type)) out.add(r); });
        return out;
    }

    public boolean isStageRole(String role) {
        for (Stage s : stages) if (s.role().equals(role)) return true;
        return false;
    }

    public boolean isAssemblyRole(String role) {
        return role.equals(trigger) || require.contains(role) || optional.contains(role);
    }
}
