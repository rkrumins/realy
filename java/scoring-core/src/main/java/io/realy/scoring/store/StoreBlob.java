package io.realy.scoring.store;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Online-store blob (contracts/storage/redis.md): u16 ver, u16 n, i64 as_of, i64 updated_at, f32[n]. */
public record StoreBlob(int schemaVersion, long asOfUs, long updatedAtUs, float[] values) {
    public static final int HEADER = 2 + 2 + 8 + 8;

    public byte[] encode() {
        ByteBuffer b = ByteBuffer.allocate(HEADER + 4 * values.length).order(ByteOrder.LITTLE_ENDIAN);
        b.putShort((short) schemaVersion).putShort((short) values.length).putLong(asOfUs).putLong(updatedAtUs);
        for (float v : values) b.putFloat(v);
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
        return new StoreBlob(ver, asOf, upd, v);
    }

    public static String streamKey(String entity, long id) {
        return "fs:" + entity + ":" + Long.toUnsignedString(id);
    }

    public static String profileKey(String entity, long id) {
        return "pf:" + entity + ":" + Long.toUnsignedString(id);
    }
}
