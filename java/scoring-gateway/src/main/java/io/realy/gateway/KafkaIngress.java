package io.realy.gateway;

import io.realy.common.BenchClock;
import io.realy.common.Env;
import io.realy.common.KafkaProps;
import io.realy.common.StageMetrics;
import io.realy.contracts.bench.v1.BenchTrace;
import io.realy.contracts.bench.v1.Stage;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.contracts.scoring.v1.IngressMode;
import io.realy.scoring.ScoringOrchestrator;
import io.realy.scoring.Spans;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Semaphore;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Asynchronous ingress: consumes {@code payments.events.raw}, scores with bounded in-flight
 * concurrency, and commits an offset only once every earlier record of that partition has a durable
 * decision (at-least-once; duplicates are counted by the collector).
 */
public final class KafkaIngress implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaIngress.class);
    private final Pipeline pipeline;
    private final StageMetrics metrics;
    private final Semaphore inflight = new Semaphore(Env.integer("MAX_INFLIGHT", 4096));
    private final List<Thread> threads = new java.util.ArrayList<>();
    private volatile boolean running = true;
    private volatile boolean enabled = Env.bool("INGRESS_KAFKA", false);
    private final java.util.concurrent.atomic.AtomicLong enableEpoch = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Runtime switch (bench.control IngressCommand).
     *
     * @param skipBacklog true for a LIVE topology switch: seek to the end so events of earlier runs
     *     (decided by another topology) are not re-scored. false when the state is being RESTORED
     *     (a restarted gateway replaying its control history): resume from the committed offsets, so
     *     every event that had no durable decision before the restart is scored now. Skipping there
     *     would silently drop fraud checks (measured before this fix: 3,052 of 27,006 events lost
     *     across a gateway restart).
     */
    public void setEnabled(boolean on, boolean skipBacklog) {
        if (on && !enabled && skipBacklog) enableEpoch.incrementAndGet();
        enabled = on;
        LOG.info("kafka ingress {}{}", on ? "enabled" : "disabled",
                on ? (skipBacklog ? " (live switch: skipping backlog)" : " (restored: resuming from committed offsets)") : "");
    }

    public void setEnabled(boolean on) {
        setEnabled(on, true);
    }

    public boolean enabled() {
        return enabled;
    }

    public KafkaIngress(Pipeline pipeline, StageMetrics metrics) {
        this.pipeline = pipeline;
        this.metrics = metrics;
    }

    public KafkaIngress start() {
        int n = Env.integer("KAFKA_CONSUMERS", 2);
        for (int i = 0; i < n; i++) {
            int id = i;
            threads.add(Thread.ofPlatform().name("ingress-" + i).start(() -> loop(id)));
        }
        return this;
    }

    private void loop(int id) {
        String topic = Env.str("EVENTS_TOPIC", "payments.events.raw");
        var props = KafkaProps.consumer(Env.str("GATEWAY_GROUP", "realy-gateway"), "gateway-ingress-" + id);
        Map<TopicPartition, ConcurrentSkipListSet<Long>> pending = new ConcurrentHashMap<>();
        Map<TopicPartition, Long> nextOffset = new ConcurrentHashMap<>();
        try (var consumer = new KafkaConsumer<byte[], byte[]>(props)) {
            consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> parts) {
                    commit(consumer, pending, nextOffset, true);
                    parts.forEach(p -> { pending.remove(p); nextOffset.remove(p); });
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> parts) { }
            });
            long lastCommit = System.currentTimeMillis();
            long seenEpoch = enableEpoch.get();
            boolean paused = false;
            while (running) {
                if (!enabled) {
                    if (!paused && !consumer.assignment().isEmpty()) {
                        consumer.pause(consumer.assignment());
                        paused = true;
                    }
                } else if (paused || seenEpoch != enableEpoch.get()) {
                    if (seenEpoch != enableEpoch.get()) consumer.seekToEnd(consumer.assignment());
                    seenEpoch = enableEpoch.get();
                    consumer.resume(consumer.assignment());
                    paused = false;
                }
                var records = consumer.poll(Duration.ofMillis(50));
                if (!enabled) {
                    // Never drop a polled record: the poll already advanced the position, so a record
                    // ignored here would never be scored and a later commit would skip past it. This
                    // happens on the first poll after a restart (ingress starts disabled until the
                    // control replay enables it) and on any disable. Rewind, then pause.
                    for (TopicPartition tp : records.partitions()) {
                        consumer.seek(tp, records.records(tp).get(0).offset());
                    }
                    if (!consumer.assignment().isEmpty()) {
                        consumer.pause(consumer.assignment());
                        paused = true;
                    }
                    continue;
                }
                long pollUs = BenchClock.nowUs();
                for (ConsumerRecord<byte[], byte[]> r : records) {
                    if (header(r, "x-mirrored") != null) continue; // sync-path mirror, already decided
                    if (header(r, "x-backfill") != null) continue; // warm-start history: features only
                    inflight.acquireUninterruptibly();
                    TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                    pending.computeIfAbsent(tp, k -> new ConcurrentSkipListSet<>()).add(r.offset());
                    nextOffset.merge(tp, r.offset() + 1, Math::max);
                    submit(r, tp, pollUs, pending);
                }
                if (System.currentTimeMillis() - lastCommit > 200) {
                    commit(consumer, pending, nextOffset, false);
                    lastCommit = System.currentTimeMillis();
                }
            }
            commit(consumer, pending, nextOffset, true);
        } catch (Exception e) {
            LOG.error("ingress loop {} died", id, e);
        }
    }

    private void submit(ConsumerRecord<byte[], byte[]> r, TopicPartition tp, long pollUs,
                        Map<TopicPartition, ConcurrentSkipListSet<Long>> pending) {
        long ingest = pollUs;
        BenchTrace.Builder trace;
        TransactionEvent ev;
        try {
            byte[] th = header(r, "x-bench-trace");
            trace = th != null ? BenchTrace.parseFrom(th).toBuilder() : BenchTrace.newBuilder();
            long dStart = BenchClock.nowUs();
            ev = TransactionEvent.parseFrom(r.value());
            Spans.point(trace, Stage.INGEST, IngressMode.ASYNC_KAFKA_GATEWAY_VALUE, ingest);
            Spans.add(trace, Stage.DESERIALIZE, 0, dStart, BenchClock.nowUs() - dStart);
        } catch (Exception e) {
            metrics.inc("poison_records");
            pending.get(tp).remove(r.offset());
            inflight.release();
            return;
        }
        var req = new ScoringOrchestrator.Request(ev, trace, ingest, IngressMode.ASYNC_KAFKA_GATEWAY, null, -1, 0,
                pipeline::publishFinal);
        pipeline.process(req).whenComplete((d, err) -> {
            if (err != null) metrics.inc("pipeline_errors");
            pending.get(tp).remove(r.offset());
            inflight.release();
        });
    }

    private static byte[] header(ConsumerRecord<byte[], byte[]> r, String name) {
        Header h = r.headers().lastHeader(name);
        return h == null ? null : h.value();
    }

    private static void commit(KafkaConsumer<byte[], byte[]> c, Map<TopicPartition, ConcurrentSkipListSet<Long>> pending,
                               Map<TopicPartition, Long> nextOffset, boolean sync) {
        Map<TopicPartition, OffsetAndMetadata> offs = new HashMap<>();
        nextOffset.forEach((tp, next) -> {
            var p = pending.get(tp);
            long safe = (p == null || p.isEmpty()) ? next : p.first();
            offs.put(tp, new OffsetAndMetadata(safe));
        });
        if (offs.isEmpty()) return;
        try {
            if (sync) c.commitSync(offs); else c.commitAsync(offs, null);
        } catch (Exception e) {
            LOG.debug("commit failed: {}", e.toString());
        }
    }

    @Override
    public void close() {
        running = false;
    }
}
