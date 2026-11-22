package io.realy.scoring.binding;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.Descriptors;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An event type binding (contracts/events/&lt;type&gt;.yaml): which protobuf message, which fields are its
 * id, event time, entities, location and amount. Mirrors python/realy/bindings/events.py.
 */
public final class EventBinding {
    public final String name;
    public final String message;
    public final String idField;
    public final boolean idHashed;
    public final String timeField;
    public final long timeUnitUs;
    /** registry entity -> field, in the binding's order. */
    public final Map<String, String> entities = new LinkedHashMap<>();
    public final String latField;
    public final String lonField;
    public final String amountField;

    EventBinding(JsonNode r) {
        name = r.get("name").asText();
        message = r.get("message").asText();
        JsonNode id = r.path("id");
        idField = id.isObject() ? id.get("field").asText() : id.asText("event_id");
        idHashed = id.isObject() && id.hasNonNull("hash");
        JsonNode t = r.path("time");
        timeField = t.isObject() ? t.get("field").asText() : t.asText("event_time_us");
        String unit = t.isObject() && t.hasNonNull("unit") ? t.get("unit").asText() : "us";
        timeUnitUs = switch (unit) {
            case "us" -> 1;
            case "ms" -> 1_000;
            case "s" -> 1_000_000;
            default -> throw new IllegalArgumentException("event type " + name + ": time unit " + unit);
        };
        r.path("entities").fields().forEachRemaining(e -> entities.put(e.getKey(), e.getValue().asText()));
        JsonNode loc = r.path("location");
        latField = loc.hasNonNull("lat") ? loc.get("lat").asText() : null;
        lonField = loc.hasNonNull("lon") ? loc.get("lon").asText() : null;
        amountField = r.hasNonNull("amount") ? r.get("amount").asText() : null;
    }

    public static EventBinding load(String type) {
        return new EventBinding(Contracts.yaml("events/" + type + ".yaml"));
    }

    public Descriptors.Descriptor descriptor() {
        return Contracts.message(message);
    }
}
