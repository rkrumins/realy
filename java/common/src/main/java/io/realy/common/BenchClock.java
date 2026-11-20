package io.realy.common;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.zip.CRC32;

/**
 * Epoch-microsecond clock for benchmark spans, tagged with this process' clock {@link #domain()}.
 *
 * <p>Reads the system wall clock directly (microsecond resolution on Linux), exactly like the Python
 * driver ({@code time.time_ns()}), so timestamps taken in different processes on the same host are
 * directly comparable (ingress/egress transit spans). An anchored-monotonic clock was used before;
 * it drifted from wall time whenever the host disciplined its clock (observed: ~10 ms within minutes
 * on a VM), which skewed every cross-process span while leaving in-process durations exact.
 * Across hosts, cross-process spans are only as good as the hosts' clock sync (chrony/PTP).
 */
public final class BenchClock {
    private static final Clock UTC = Clock.systemUTC();
    private static final int DOMAIN;
    private static final String DOMAIN_NAME;

    static {
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

    /** Epoch microseconds (system wall clock). */
    public static long nowUs() {
        Instant now = UTC.instant();
        return now.getEpochSecond() * 1_000_000L + now.getNano() / 1_000L;
    }

    public static int domain() {
        return DOMAIN;
    }

    public static String domainName() {
        return DOMAIN_NAME;
    }
}
