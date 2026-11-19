package io.realy.common;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.zip.CRC32;

/**
 * Calibrated epoch-microsecond clock. The wall clock is read once (anchor) and then advanced with
 * the monotonic {@link System#nanoTime()}, so intervals measured inside one process are exact and
 * never jump with NTP slews. Every timestamp is tagged with this process' clock {@link #domain()}.
 */
public final class BenchClock {
    private static final long ANCHOR_WALL_US;
    private static final long ANCHOR_NANO;
    private static final int DOMAIN;
    private static final String DOMAIN_NAME;

    static {
        Instant now = Instant.now();
        ANCHOR_NANO = System.nanoTime();
        ANCHOR_WALL_US = now.getEpochSecond() * 1_000_000L + now.getNano() / 1_000L;
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "unknown";
        }
        DOMAIN_NAME = host + "/" + ManagementFactory.getRuntimeMXBean().getName();
        CRC32 crc = new CRC32();
        crc.update(DOMAIN_NAME.getBytes(StandardCharsets.UTF_8));
        DOMAIN = (int) crc.getValue();
    }

    private BenchClock() {}

    /** Epoch microseconds (anchored wall clock + monotonic delta). */
    public static long nowUs() {
        return ANCHOR_WALL_US + (System.nanoTime() - ANCHOR_NANO) / 1_000L;
    }

    public static int domain() {
        return DOMAIN;
    }

    public static String domainName() {
        return DOMAIN_NAME;
    }
}
