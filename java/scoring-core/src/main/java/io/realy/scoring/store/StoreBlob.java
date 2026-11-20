package io.realy.scoring.store;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Online-store blob (contracts/storage/redis.md), little-endian:
 * <pre>
 *   u16 ver, u16 n, i64 as_of, i64 updated_at, f32[n]                         values blob (v1)
 *   ... same, ver | 0x8000, then u64 last_event_id, u32 state_len, state[...]  state blob (v2)
 * </pre>
 * A state blob also carries the entity's full windowed aggregator state ({@code EntityAggregator}),
 * so the reader can re-window it at the event's own time instead of serving values computed at the
 * entity's last event. The extra fields come after the values, so a v1 reader still works.
 */
public record StoreBlob(int schemaVersion, long asOfUs, long updatedAtUs, float[] values, long lastEventId,
                        byte[] state) {
    public static final int HEADER = 2 + 2 + 8 + 8;
    public static final int STATE_FLAG = 0x8000;

    /** Values-only blob (v1). */
    public StoreBlob(int schemaVersion, long asOfUs, long updatedAtUs, float[] values) {
        this(schemaVersion, asOfUs, updatedAtUs, values, 0L, null);
    }

    public boolean hasState() {
        return state != null;
    }

    public int encodedSize() {
        return HEADER + 4 * values.length + (state == null ? 0 : 8 + 4 + state.length);
    }

    public byte[] encode() {
        ByteBuffer b = ByteBuffer.allocate(encodedSize()).order(ByteOrder.LITTLE_ENDIAN);
        int ver = state == null ? schemaVersion : (schemaVersion | STATE_FLAG);
        b.putShort((short) ver).putShort((short) values.length).putLong(asOfUs).putLong(updatedAtUs);
        for (float v : values) b.putFloat(v);
        if (state != null) b.putLong(lastEventId).putInt(state.length).put(state);
        return b.array();
    }

    public static StoreBlob decode(byte[] bytes) {
        if (bytes == null || bytes.length < HEADER) return null;
        ByteBuffer b = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int ver = Short.toUnsignedInt(b.getShort());
        int n = Short.toUnsignedInt(b.getShort());
        long asOf = b.getLong();
        long upd = b.getLong();
        if (bytes.length < HEADER + 4 * n) return null;
        float[] v = new float[n];
        for (int i = 0; i < n; i++) v[i] = b.getFloat();
        if ((ver & STATE_FLAG) == 0) return new StoreBlob(ver, asOf, upd, v);
        if (b.remaining() < 12) return new StoreBlob(ver & ~STATE_FLAG, asOf, upd, v);   // truncated: values only
        long lastEventId = b.getLong();
        int len = b.getInt();
        if (len < 0 || b.remaining() < len) return new StoreBlob(ver & ~STATE_FLAG, asOf, upd, v);
        byte[] state = new byte[len];
        b.get(state);
        return new StoreBlob(ver & ~STATE_FLAG, asOf, upd, v, lastEventId, state);
    }

    public static String streamKey(String entity, long id) {
        return "fs:" + entity + ":" + Long.toUnsignedString(id);
    }

    public static String profileKey(String entity, long id) {
        return "pf:" + entity + ":" + Long.toUnsignedString(id);
    }
}
