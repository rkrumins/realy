package io.realy.scoring.features;

/**
 * An event decoded once for the keyed feature stages: identity, time, location, the key of every
 * entity and the values the stream features read (numeric slots for sum / max / avg, value-key slots for
 * distinct / novel), laid out by the schema's {@link RowPlan}. The Flink job builds it in the
 * deserializer and carries it in the envelope, so no stage parses the event again.
 *
 * <p>A Flink POJO: public no-argument constructor and public fields of primitive (array) types.
 */
public class EventRow {
    public long eventId;
    public long timeUs;
    public double lat;
    public double lon;
    /** Key of each entity, indexed by {@link RowPlan#entityOrdinal}; 0 = the event does not carry it. */
    public long[] keys;
    /** Numeric slots, indexed by {@link RowPlan#numSlot}. */
    public double[] num;
    /** Value-key slots (strings hashed with FNV-1a 64), indexed by {@link RowPlan#valSlot}. */
    public long[] val;

    public EventRow() {}
}
