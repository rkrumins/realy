package io.realy.scoring.binding;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The entity registry (contracts/entities.yaml): every kind of thing events refer to and state is kept
 * for, in keyed-stage order. Mirrors python/realy/bindings/registry.py.
 */
public final class EntityRegistry {
    public record Entity(String name, int ordinal, String id, String enumName, String ownedBy) {}

    private final Map<String, Entity> entities = new LinkedHashMap<>();
    private final List<String> names = new ArrayList<>();
    private static volatile EntityRegistry DEFAULT;

    EntityRegistry(JsonNode root) {
        JsonNode ents = root.get("entities");
        int i = 0;
        for (var it = ents.fields(); it.hasNext(); i++) {
            var e = it.next();
            JsonNode s = e.getValue();
            String id = s.hasNonNull("id") ? s.get("id").asText() : "u64";
            entities.put(e.getKey(), new Entity(e.getKey(), i, id, s.hasNonNull("enum") ? s.get("enum").asText() : null,
                    s.hasNonNull("owned_by") ? s.get("owned_by").asText() : null));
            names.add(e.getKey());
        }
        for (Entity e : entities.values()) {
            if (e.ownedBy() != null && !entities.containsKey(e.ownedBy())) {
                throw new IllegalArgumentException("entity " + e.name() + ": owned_by " + e.ownedBy() + " is not an entity");
            }
            owners(e.name());
        }
    }

    public static EntityRegistry load() {
        if (DEFAULT == null) {
            synchronized (EntityRegistry.class) {
                if (DEFAULT == null) DEFAULT = new EntityRegistry(Contracts.yaml("entities.yaml"));
            }
        }
        return DEFAULT;
    }

    public List<String> names() {
        return names;
    }

    public Entity get(String name) {
        Entity e = entities.get(name);
        if (e == null) throw new IllegalArgumentException("unknown entity " + name + " (contracts/entities.yaml)");
        return e;
    }

    public boolean contains(String name) {
        return entities.containsKey(name);
    }

    /** Owners of an entity, nearest first (card -> [customer]). */
    public List<String> owners(String name) {
        List<String> out = new ArrayList<>();
        Map<String, Boolean> seen = new HashMap<>();
        seen.put(name, true);
        String cur = get(name).ownedBy();
        while (cur != null) {
            if (seen.put(cur, true) != null) throw new IllegalArgumentException("entity ownership cycle through " + cur);
            out.add(cur);
            cur = get(cur).ownedBy();
        }
        return out;
    }

    /** Entities seen in one total order by a pipeline partitioned by {@code partition}: exact features. */
    public List<String> exact(String partition) {
        List<String> out = new ArrayList<>();
        for (String n : names) if (n.equals(partition) || owners(n).contains(partition)) out.add(n);
        return out;
    }
}
