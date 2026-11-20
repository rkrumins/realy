package io.realy.common;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.realy.contracts.bench.v1.MetricsSnapshot;
import io.realy.contracts.bench.v1.StageStat;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-stage latency recording into (a) Prometheus timers with percentile histograms (Grafana) and
 * (b) HdrHistogram interval recorders published every second as a {@link MetricsSnapshot} on
 * {@code bench.metrics} (operator console live view).
 */
public final class StageMetrics implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(StageMetrics.class);
    private final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    private final Map<String, Recorder> recorders = new ConcurrentHashMap<>();
    private final Map<String, Timer> timers = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> counters = new ConcurrentHashMap<>();
    private final Map<String, Counter> promCounters = new ConcurrentHashMap<>();
    private final String source;
    private final String instance;
    private volatile String runId = Env.str("RUN_ID", "");
    private KafkaProducer<byte[], byte[]> producer;
    private Thread publisher;
    private volatile boolean running = true;

    public StageMetrics(String source, String instance) {
        this.source = source;
        this.instance = instance;
        // JVM health next to the stage latencies: GC pauses are a classic hidden tail-latency source.
        new io.micrometer.core.instrument.binder.jvm.JvmGcMetrics().bindTo(registry);
        new io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics().bindTo(registry);
        new io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics().bindTo(registry);
        new io.micrometer.core.instrument.binder.system.ProcessorMetrics().bindTo(registry);
    }

    public PrometheusMeterRegistry registry() {
        return registry;
    }

    public void setRunId(String runId) {
        this.runId = runId;
    }

    public void recordUs(String stage, long us) {
        if (us < 0) us = 0;
        recorders.computeIfAbsent(stage, s -> new Recorder(3_600_000_000L, 3)).recordValue(Math.min(us, 3_600_000_000L));
        timers.computeIfAbsent(stage, s -> Timer.builder("realy_stage_latency")
                        .tag("stage", s).tag("source", source)
                        .publishPercentileHistogram()
                        .minimumExpectedValue(Duration.ofNanos(50_000))
                        .maximumExpectedValue(Duration.ofSeconds(5))
                        .register(registry))
                .record(us, TimeUnit.MICROSECONDS);
    }

    public void inc(String counter) {
        add(counter, 1);
    }

    public void add(String counter, long n) {
        counters.computeIfAbsent(counter, c -> new LongAdder()).add(n);
        promCounters.computeIfAbsent(counter, c -> Counter.builder("realy_events_total")
                .tag("counter", c).tag("source", source).register(registry)).increment(n);
    }

    /** Start publishing 1 s snapshots to {@code bench.metrics} (no-op if Kafka is unreachable). */
    public StageMetrics startPublishing() {
        if (!Env.bool("METRICS_PUBLISH", true)) return this;
        try {
            var props = KafkaProps.producer(source + "-metrics");
            props.put("acks", "1");
            props.put("enable.idempotence", "false");
            props.put("linger.ms", "20");
            producer = new KafkaProducer<>(props);
        } catch (Exception e) {
            LOG.warn("metrics publishing disabled: {}", e.toString());
            return this;
        }
        String topic = Env.str("METRICS_TOPIC", "bench.metrics");
        publisher = Thread.ofPlatform().daemon().name("metrics-publisher").start(() -> {
            Map<String, Long> last = new ConcurrentHashMap<>();
            ProcessCpu cpu = new ProcessCpu();
            long next = System.currentTimeMillis() / 1000 * 1000 + 1000;
            while (running) {
                long sleep = next - System.currentTimeMillis();
                if (sleep > 0) {
                    try {
                        Thread.sleep(sleep);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
                next += 1000;
                var snap = MetricsSnapshot.newBuilder().setRunId(runId).setSource(source).setInstance(instance)
                        .setTsUs(BenchClock.nowUs()).setWindowMs(1000);
                counters.forEach((k, v) -> {
                    long now = v.sum();
                    long prev = last.getOrDefault(k, 0L);
                    last.put(k, now);
                    snap.putCounters(k, now - prev);
                    snap.putCounters(k + "_total", now);
                });
                cpu.sample(snap);
                recorders.forEach((k, r) -> {
                    Histogram h = r.getIntervalHistogram();
                    if (h.getTotalCount() == 0) return;
                    snap.addStages(StageStat.newBuilder().setName(k).setCount(h.getTotalCount())
                            .setP50Ms(h.getValueAtPercentile(50) / 1000.0)
                            .setP90Ms(h.getValueAtPercentile(90) / 1000.0)
                            .setP99Ms(h.getValueAtPercentile(99) / 1000.0)
                            .setP999Ms(h.getValueAtPercentile(99.9) / 1000.0)
                            .setMaxMs(h.getMaxValue() / 1000.0));
                });
                try {
                    producer.send(new ProducerRecord<>(topic, (source + ":" + instance).getBytes(StandardCharsets.UTF_8),
                            ProtoJson.print(snap).getBytes(StandardCharsets.UTF_8)));
                } catch (Exception e) {
                    LOG.debug("metrics publish failed: {}", e.toString());
                }
            }
        });
        return this;
    }

    @Override
    public void close() {
        running = false;
        if (producer != null) producer.close(Duration.ofSeconds(2));
    }

    /**
     * Resource cost of this process per 1 s window, published with the stage counters so that every
     * run records CPU per event per component (capacity sizing, `realy size`):
     * {@code cpu_ms} process CPU time used in the window, {@code cpus} CPUs the JVM may use
     * (container limit aware), {@code throttled_ms} CFS throttling in the window (cgroup v2 or v1), and
     * {@code heap_used_mb}.
     */
    static final class ProcessCpu {
        private final java.lang.management.OperatingSystemMXBean os =
                java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        private final java.lang.management.MemoryMXBean mem = java.lang.management.ManagementFactory.getMemoryMXBean();
        private long lastCpuNs = cpuNs();
        private long lastThrottledUs = throttledUs();

        private long cpuNs() {
            return os instanceof com.sun.management.OperatingSystemMXBean sun ? sun.getProcessCpuTime() : -1;
        }

        void sample(MetricsSnapshot.Builder snap) {
            long c = cpuNs();
            if (c >= 0 && lastCpuNs >= 0) snap.putCounters("cpu_ms", (c - lastCpuNs) / 1e6);
            lastCpuNs = c;
            snap.putCounters("cpus", Runtime.getRuntime().availableProcessors());
            long th = throttledUs();
            if (th >= 0 && lastThrottledUs >= 0) snap.putCounters("throttled_ms", (th - lastThrottledUs) / 1e3);
            lastThrottledUs = th;
            snap.putCounters("heap_used_mb", mem.getHeapMemoryUsage().getUsed() / 1048576.0);
        }

        /** Cumulative CFS throttled time in microseconds, or -1 when no cgroup cpu.stat is readable. */
        static long throttledUs() {
            for (String f : new String[] {"/sys/fs/cgroup/cpu.stat", "/sys/fs/cgroup/cpu/cpu.stat",
                    "/sys/fs/cgroup/cpu,cpuacct/cpu.stat"}) {
                try {
                    for (String line : java.nio.file.Files.readAllLines(java.nio.file.Path.of(f))) {
                        if (line.startsWith("throttled_usec ")) return Long.parseLong(line.substring(15).trim());
                        if (line.startsWith("throttled_time ")) return Long.parseLong(line.substring(15).trim()) / 1000;
                    }
                } catch (Exception ignored) {
                    // not this cgroup layout
                }
            }
            return -1;
        }
    }
}
