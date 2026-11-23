package io.realy.scoring.features;

import io.realy.common.Hashing;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.scoring.binding.EntityRegistry;
import io.realy.scoring.binding.EventBinding;
import io.realy.scoring.binding.WireCodec;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The layout of an {@link EventRow} for one feature schema: which entity keys it carries (in entity
 * order) and which event fields the stream features read, each in its own slot. Values are extracted with
 * the same functions the aggregators used on the event itself ({@link EventView#numeric},
 * {@link EventView#valueKey}, {@link EventView#entityKey}), so a row-based aggregation is bit-identical.
 * Any other bound message type is decoded by its {@link WireCodec} ({@link #row(EventBinding, WireCodec, Object[])}):
 * entity order is the registry's (contracts/entities.yaml).
 */
public final class RowPlan {
    public final List<String> entities;
    public final List<String> numFields;
    public final List<String> valFields;
    private final Map<String, Integer> entityIndex = new HashMap<>();
    private final Map<String, Integer> numIndex = new HashMap<>();
    private final Map<String, Integer> valIndex = new HashMap<>();

    RowPlan(FeatureSchema schema) {
        this.entities = EntityRegistry.load().names();
        for (int i = 0; i < entities.size(); i++) entityIndex.put(entities.get(i), i);
        List<String> num = new ArrayList<>();
        List<String> val = new ArrayList<>();
        for (FeatureSpec s : schema.features) {
            if (!s.isStream() || s.value() == null) continue;
            switch (Aggregations.get(s.agg()).input()) {   // what the aggregation reads (built-in or plugin)
                case NUMERIC -> { if (!num.contains(s.value())) num.add(s.value()); }
                case KEY -> { if (!val.contains(s.value())) val.add(s.value()); }
                default -> { }
            }
        }
        this.numFields = List.copyOf(num);
        this.valFields = List.copyOf(val);
        for (int i = 0; i < numFields.size(); i++) numIndex.put(numFields.get(i), i);
        for (int i = 0; i < valFields.size(); i++) valIndex.put(valFields.get(i), i);
        // fields another event type carries (a login's ip, a device report's risk_score): a payment reads 0
        this.numOnPayment = new boolean[numFields.size()];
        for (int i = 0; i < numOnPayment.length; i++) numOnPayment[i] = EventView.has(numFields.get(i));
        this.valOnPayment = new boolean[valFields.size()];
        for (int i = 0; i < valOnPayment.length; i++) valOnPayment[i] = EventView.has(valFields.get(i));
    }

    private final boolean[] numOnPayment;
    private final boolean[] valOnPayment;

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

    /** Where a bound message type's values are in its decoded fields (compiled once per type). */
    private record Bound(int id, boolean idHashed, int time, long unitUs, int lat, int lon, int[] keys, boolean[] keyHashed,
                         int[] num, int[] val) {}

    private final Map<String, Bound> bound = new ConcurrentHashMap<>();

    private Bound bind(EventBinding b, WireCodec c) {
        int[] keys = new int[entities.size()];
        boolean[] hashed = new boolean[entities.size()];
        EntityRegistry reg = EntityRegistry.load();
        for (int i = 0; i < keys.length; i++) {
            String f = b.entities.get(entities.get(i));
            keys[i] = f == null ? -1 : c.indexOf(f);
            hashed[i] = "string".equals(reg.get(entities.get(i)).id());
        }
        int[] num = new int[numFields.size()];
        for (int i = 0; i < num.length; i++) num[i] = c.indexOf(numFields.get(i));
        int[] val = new int[valFields.size()];
        for (int i = 0; i < val.length; i++) val[i] = c.indexOf(valFields.get(i));
        int time = c.indexOf(b.timeField);
        if (time < 0) throw new IllegalArgumentException("event type " + b.name + ": no time field " + b.timeField);
        return new Bound(c.indexOf(b.idField), b.idHashed, time, b.timeUnitUs, b.latField == null ? -1 : c.indexOf(b.latField),
                b.lonField == null ? -1 : c.indexOf(b.lonField), keys, hashed, num, val);
    }

    /**
     * The row of any bound message from its decoded values: entity keys from the binding's fields (a
     * string id hashed with FNV-1a 64), numeric slots as doubles, value slots as longs (strings hashed);
     * a field the type does not have reads 0.
     */
    public EventRow row(EventBinding b, WireCodec c, Object[] v) {
        Bound p = bound.computeIfAbsent(b.name, k -> bind(b, c));
        EventRow r = new EventRow();
        r.eventId = p.id < 0 ? 0 : p.idHashed ? Hashing.fnv1a64((String) v[p.id]) : ((Number) v[p.id]).longValue();
        r.timeUs = ((Number) v[p.time]).longValue() * p.unitUs;
        r.lat = p.lat < 0 ? 0 : ((Number) v[p.lat]).doubleValue();
        r.lon = p.lon < 0 ? 0 : ((Number) v[p.lon]).doubleValue();
        r.keys = new long[p.keys.length];
        for (int i = 0; i < r.keys.length; i++) {
            int j = p.keys[i];
            if (j < 0) continue;
            Object x = v[j];
            r.keys[i] = x instanceof String str ? (str.isEmpty() ? 0 : Hashing.fnv1a64(str)) : ((Number) x).longValue();
        }
        r.num = new double[p.num.length];
        // numbers as they are, an enum as its number, a bool as 1 / 0 (WireCodec.numeric; EventView.numeric)
        for (int i = 0; i < r.num.length; i++) r.num[i] = p.num[i] < 0 ? 0 : c.numeric(p.num[i], v[p.num[i]]);
        r.val = new long[p.val.length];
        for (int i = 0; i < r.val.length; i++) {
            int j = p.val[i];
            if (j < 0) continue;
            Object x = v[j];
            r.val[i] = x instanceof String str ? Hashing.fnv1a64(str) : x instanceof Boolean bo ? (bo ? 1 : 0)
                    : ((Number) x).longValue();
        }
        return r;
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
        for (int i = 0; i < r.num.length; i++) r.num[i] = numOnPayment[i] ? EventView.numeric(e, numFields.get(i)) : 0;
        r.val = new long[valFields.size()];
        for (int i = 0; i < r.val.length; i++) r.val[i] = valOnPayment[i] ? EventView.valueKey(e, valFields.get(i)) : 0;
        return r;
    }
}
