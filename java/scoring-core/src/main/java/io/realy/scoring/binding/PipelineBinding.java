package io.realy.scoring.binding;

import com.fasterxml.jackson.databind.JsonNode;
import io.realy.common.Env;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A pipeline (contracts/pipelines/&lt;name&gt;.yaml; PIPELINE selects one, default payments): its event
 * types, partition entity, namespace and topics. Mirrors python/realy/bindings/pipeline.py.
 */
public final class PipelineBinding {
    public final String name;
    public final String namespace;
    public final String partitionKey;
    public final String defaultType;
    public final Map<String, String> topics = new LinkedHashMap<>();
    /** event type -> scored. */
    public final Map<String, Boolean> eventTypes = new LinkedHashMap<>();
    private final Map<String, EventBinding> bindings = new LinkedHashMap<>();
    private static volatile PipelineBinding DEFAULT;

    PipelineBinding(JsonNode r) {
        name = r.get("name").asText();
        namespace = r.path("namespace").asText("");
        partitionKey = r.path("partition_key").asText("customer");
        defaultType = r.path("default_type").asText("payment");
        r.path("topics").fields().forEachRemaining(e -> topics.put(e.getKey(), e.getValue().asText()));
        r.path("event_types").fields().forEachRemaining(e -> eventTypes.put(e.getKey(), e.getValue().path("scored").asBoolean(false)));
        for (String t : eventTypes.keySet()) bindings.put(t, EventBinding.load(t));
        EntityRegistry reg = EntityRegistry.load();
        if (!reg.contains(partitionKey)) throw new IllegalArgumentException("pipeline " + name + ": partition_key " + partitionKey);
        if (!eventTypes.containsKey(defaultType)) throw new IllegalArgumentException("pipeline " + name + ": default_type " + defaultType);
    }

    public static PipelineBinding load(String name) {
        return new PipelineBinding(Contracts.yaml("pipelines/" + name + ".yaml"));
    }

    /** The pipeline this process runs (PIPELINE, default payments). */
    public static PipelineBinding current() {
        if (DEFAULT == null) {
            synchronized (PipelineBinding.class) {
                if (DEFAULT == null) DEFAULT = load(Env.str("PIPELINE", "payments"));
            }
        }
        return DEFAULT;
    }

    public EventBinding binding(String type) {
        EventBinding b = bindings.get(type);
        if (b == null) throw new IllegalArgumentException("pipeline " + name + " has no event type " + type);
        return b;
    }

    public boolean scored(String type) {
        return eventTypes.getOrDefault(type, false);
    }

    public List<String> exactEntities() {
        return EntityRegistry.load().exact(partitionKey);
    }

    /** Redis key prefix of this namespace ("" = today's keys). */
    public String keyPrefix() {
        return namespace.isEmpty() ? "" : namespace + ":";
    }
}
