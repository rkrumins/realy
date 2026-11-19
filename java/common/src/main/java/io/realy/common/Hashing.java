package io.realy.common;

import java.nio.charset.StandardCharsets;

/** Stable hashes shared with python/realy/common/hashing.py. */
public final class Hashing {
    private Hashing() {}

    /** FNV-1a 64-bit over UTF-8 bytes (used to key string values in distinct/novel state). */
    public static long fnv1a64(String s) {
        long h = 0xcbf29ce484222325L;
        for (byte b : s.getBytes(StandardCharsets.UTF_8)) {
            h ^= (b & 0xff);
            h *= 0x100000001b3L;
        }
        return h;
    }

    /** FNV-1a 64-bit over raw little-endian float bits (input-tensor hash for audit). */
    public static long fnv1a64(float[] values) {
        long h = 0xcbf29ce484222325L;
        for (float v : values) {
            int bits = Float.floatToRawIntBits(v);
            for (int i = 0; i < 4; i++) {
                h ^= (bits >>> (8 * i)) & 0xff;
                h *= 0x100000001b3L;
            }
        }
        return h;
    }
}
