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
    /** The pipeline's feature schema (`features`, repository-relative, e.g. contracts/features/x.yaml), or null. */
    public final String features;
    /** The multi-message transactions whose parts arrive on the events topic. */
    public final List<String> transactions = new java.util.ArrayList<>();
    private final Map<String, Integer> typeOrdinals = new LinkedHashMap<>();
    private final Map<String, String> typeTopic = new LinkedHashMap<>();   // type -> topics key (default "events")
    private final Map<String, String> typeKey = new LinkedHashMap<>();     // type -> its topic's partition entity
    private volatile java.util.Set<String> partTypes;
    private final Map<String, EventBinding> bindings = new LinkedHashMap<>();
    private static volatile PipelineBinding DEFAULT;

    PipelineBinding(JsonNode r) {
        name = r.get("name").asText();
        namespace = r.path("namespace").asText("");
        partitionKey = r.path("partition_key").asText("customer");
        defaultType = r.path("default_type").asText("payment");
        r.path("topics").fields().forEachRemaining(e -> topics.put(e.getKey(), e.getValue().asText()));
        r.path("event_types").fields().forEachRemaining(e -> {
            eventTypes.put(e.getKey(), e.getValue().path("scored").asBoolean(false));
            typeTopic.put(e.getKey(), e.getValue().path("topic").asText("events"));
            typeKey.put(e.getKey(), e.getValue().path("partition_key").asText(r.path("partition_key").asText("customer")));
        });
        r.path("transactions").forEach(t -> transactions.add(t.asText()));
        for (String t : eventTypes.keySet()) typeOrdinals.put(t, typeOrdinals.size());
        if (typeOrdinals.size() > 64) throw new IllegalArgumentException("pipeline " + r.get("name").asText() + ": at most 64 event types");
        features = r.hasNonNull("features") ? r.get("features").asText() : null;
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

    private static final java.util.concurrent.ConcurrentHashMap<String, PipelineBinding> BY_NAME =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * The pipeline of this name, loaded once per process (null or empty = {@link #current()}). The Flink job
     * resolves its pipeline when it builds the graph and ships the name to every operator, so a TaskManager's
     * environment cannot select another one.
     */
    public static PipelineBinding of(String name) {
        if (name == null || name.isEmpty()) return current();
        PipelineBinding c = current();
        if (c.name.equals(name)) return c;
        return BY_NAME.computeIfAbsent(name, PipelineBinding::load);
    }

    public EventBinding binding(String type) {
        EventBinding b = bindings.get(type);
        if (b == null) throw new IllegalArgumentException("pipeline " + name + " has no event type " + type);
        return b;
    }

    public boolean scored(String type) {
        return eventTypes.getOrDefault(type, false);
    }

    /** The event type's position in the pipeline (its bit in a feature's `on` mask), or -1. */
    public int typeOrdinal(String type) {
        return typeOrdinals.getOrDefault(type, -1);
    }

    public int defaultOrdinal() {
        return typeOrdinals.get(defaultType);
    }

    /** Whether events of this type are decided (the default type always is). */
    public boolean scored(int ordinal) {
        String t = typeName(ordinal);
        return t != null && (t.equals(defaultType) || scored(t));
    }

    /** The Kafka topic events of this type travel on (its `topic`, else the events topic). */
    public String topicOf(String type) {
        String key = typeTopic.getOrDefault(type, "events");
        String t = topics.get(key);
        if (t == null) throw new IllegalArgumentException("pipeline " + name + ": event type " + type + "'s topic " + key + " is not one of its topics");
        return t;
    }

    /** The entity events of this type are keyed by on their topic. */
    public String partitionKeyOf(String type) {
        return typeKey.getOrDefault(type, partitionKey);
    }

    /** Topics other than the events topic, with the event types that travel on each. */
    public Map<String, List<String>> extraTopics() {
        Map<String, List<String>> out = new LinkedHashMap<>();
        String events = topics.get("events");
        for (String t : eventTypes.keySet()) {
            String topic = topicOf(t);
            if (!topic.equals(events)) out.computeIfAbsent(topic, k -> new java.util.ArrayList<>()).add(t);
        }
        return out;
    }

    /** Whether events of this type are parts of one of the pipeline's multi-message transactions. */
    public boolean isPart(String type) {
        java.util.Set<String> p = partTypes;
        if (p == null) {
            p = new java.util.HashSet<>();
            for (String n : transactions) p.addAll(TransactionContract.load(n).roles.values());
            partTypes = p;
        }
        return p.contains(type);
    }

    /** A type that is neither the default type nor a transaction part: it folds into features on its own. */
    public boolean standalone(String type) {
        return !type.equals(defaultType) && !isPart(type);
    }

    public String typeName(int ordinal) {
        for (var e : typeOrdinals.entrySet()) if (e.getValue() == ordinal) return e.getKey();
        return null;
    }

    public List<String> exactEntities() {
        return EntityRegistry.load().exact(partitionKey);
    }

    /** Redis key prefix of this namespace ("" = today's keys). */
    public String keyPrefix() {
        return namespace.isEmpty() ? "" : namespace + ":";
    }
}
