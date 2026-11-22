package io.realy.scoring.features;

import io.realy.contracts.payments.v1.TransactionEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The layout of an {@link EventRow} for one feature schema: which entity keys it carries (in entity
 * order) and which event fields the stream features read, each in its own slot. Values are extracted with
 * the same functions the aggregators used on the event itself ({@link EventView#numeric},
 * {@link EventView#valueKey}, {@link EventView#entityKey}), so a row-based aggregation is bit-identical.
 */
public final class RowPlan {
    public final List<String> entities;
    public final List<String> numFields;
    public final List<String> valFields;
    private final Map<String, Integer> entityIndex = new HashMap<>();
    private final Map<String, Integer> numIndex = new HashMap<>();
    private final Map<String, Integer> valIndex = new HashMap<>();

    RowPlan(FeatureSchema schema) {
        this.entities = FeatureSchema.ENTITY_ORDER;
        for (int i = 0; i < entities.size(); i++) entityIndex.put(entities.get(i), i);
        List<String> num = new ArrayList<>();
        List<String> val = new ArrayList<>();
        for (FeatureSpec s : schema.features) {
            if (!s.isStream() || s.value() == null) continue;
            switch (s.agg()) {
                case "sum", "max", "avg" -> { if (!num.contains(s.value())) num.add(s.value()); }
                case "distinct", "novel" -> { if (!val.contains(s.value())) val.add(s.value()); }
                default -> { }
            }
        }
        this.numFields = List.copyOf(num);
        this.valFields = List.copyOf(val);
        for (int i = 0; i < numFields.size(); i++) numIndex.put(numFields.get(i), i);
        for (int i = 0; i < valFields.size(); i++) valIndex.put(valFields.get(i), i);
    }

    public int entityOrdinal(String entity) {
        Integer i = entityIndex.get(entity);
        if (i == null) throw new IllegalArgumentException("unknown entity " + entity);
        return i;
    }

    /** Slot of a numeric field, or -1 when no stream feature reads it. */
    public int numSlot(String field) {
        return field == null ? -1 : numIndex.getOrDefault(field, -1);
    }

    /** Slot of a value-key field, or -1 when no stream feature reads it. */
    public int valSlot(String field) {
        return field == null ? -1 : valIndex.getOrDefault(field, -1);
    }

    /** Decode the values the keyed stages need, once. */
    public EventRow row(TransactionEvent e) {
        EventRow r = new EventRow();
        r.eventId = e.getEventId();
        r.timeUs = e.getEventTimeUs();
        r.lat = e.getLat();
        r.lon = e.getLon();
        r.keys = new long[entities.size()];
        for (int i = 0; i < r.keys.length; i++) r.keys[i] = EventView.entityKey(e, entities.get(i));
        r.num = new double[numFields.size()];
        for (int i = 0; i < r.num.length; i++) r.num[i] = EventView.numeric(e, numFields.get(i));
        r.val = new long[valFields.size()];
        for (int i = 0; i < r.val.length; i++) r.val[i] = EventView.valueKey(e, valFields.get(i));
        return r;
    }
}
