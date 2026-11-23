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

    // what the async rail scores: INGRESS_TOPIC (the assembled topic when Flink assembles multi-message
    // transactions), default EVENTS_TOPIC; the sync rail's mirror always goes to EVENTS_TOPIC
    private final String defaultTopic = Env.str("INGRESS_TOPIC", Env.str("EVENTS_TOPIC", "payments.events.raw"));
    private volatile String topic = defaultTopic;

    /** The topic consumed now. */
    public String topic() {
        return topic;
    }

    /** Consume another topic from the next poll ("" = EVENTS_TOPIC): the assembled transactions in flows runs. */
    public void setTopic(String t) {
        String want = t == null || t.isBlank() ? defaultTopic : t;
        if (!want.equals(topic)) {
            LOG.info("kafka ingress: {} -> {}", topic, want);
            topic = want;
        }
    }

    public KafkaIngress(Pipeline pipeline, StageMetrics metrics) {
        this.pipeline = pipeline;
        this.metrics = metrics;
    }

    public KafkaIngress start() {
        int n = Env.integer("KAFKA_CONSUMERS", 2);
        lastPollMs = new long[n];
        for (int i = 0; i < n; i++) {
            int id = i;
            threads.add(Thread.ofPlatform().name("ingress-" + i).start(() -> supervise(id)));
        }
        return this;
    }

    private volatile long[] lastPollMs = new long[0];
    private final java.util.concurrent.atomic.AtomicLong restarts = new java.util.concurrent.atomic.AtomicLong();
    private final int maxInflight = Env.integer("MAX_INFLIGHT", 4096);

    /**
     * Keep consumer {@code id} running: a loop that dies (a broker error, an unexpected exception) is
     * restarted with a fresh consumer after a backoff, from the committed offsets, so nothing is skipped.
     */
    private void supervise(int id) {
        long backoffMs = 500;
        while (running) {
            try {
                loop(id);
                backoffMs = 500;
            } catch (Throwable e) {   // noqa: everything: a dead ingress thread must never go unnoticed
                restarts.incrementAndGet();
                metrics.inc("ingress_restarts");
                LOG.error("ingress loop {} failed; restarting in {} ms", id, backoffMs, e);
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
                backoffMs = Math.min(backoffMs * 2, 30_000);
            }
        }
    }

    /** Every consumer polled within `withinMs` (or the ingress is disabled): the readiness signal. */
    public boolean healthy(long withinMs) {
        if (!enabled) return true;
        long now = System.currentTimeMillis();
        long[] lp = lastPollMs;
        for (long t : lp) if (t == 0 || now - t > withinMs) return false;
        return lp.length > 0;
    }

    public long restarts() {
        return restarts.get();
    }

    private void loop(int id) throws Exception {
        String subscribed = topic;
        boolean[] seekEndOnAssign = {false};
        var props = KafkaProps.consumer(Env.str("GATEWAY_GROUP", "realy-gateway"), "gateway-ingress-" + id);
        Map<TopicPartition, ConcurrentSkipListSet<Long>> pending = new ConcurrentHashMap<>();
        Map<TopicPartition, Long> nextOffset = new ConcurrentHashMap<>();
        try (var consumer = new KafkaConsumer<byte[], byte[]>(props)) {
            ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> parts) {
                    commit(consumer, pending, nextOffset, true);
                    parts.forEach(p -> { pending.remove(p); nextOffset.remove(p); });
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> parts) {
                    if (seekEndOnAssign[0]) {
                        consumer.seekToEnd(parts);   // a switched topic: this run's records only
                        seekEndOnAssign[0] = false;
                    }
                }
            };
            consumer.subscribe(List.of(subscribed), listener);
            long lastCommit = System.currentTimeMillis();
            long seenEpoch = enableEpoch.get();
            boolean paused = false;
            while (running) {
                if (!topic.equals(subscribed)) {
                    // another topic for this run (flows): commit what was scored, then start at its end
                    commit(consumer, pending, nextOffset, true);
                    consumer.unsubscribe();
                    pending.clear();
                    nextOffset.clear();
                    subscribed = topic;
                    seekEndOnAssign[0] = true;
                    consumer.subscribe(List.of(subscribed), listener);
                    paused = false;
                }
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
                if (id < lastPollMs.length) lastPollMs[id] = System.currentTimeMillis();
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
                    if (header(r, "x-mirrored") != null) {          // sync-path mirror, already decided
                        metrics.inc("skipped_mirrored");
                        continue;
                    }
                    if (header(r, "x-backfill") != null) {          // warm-start history: features only
                        metrics.inc("skipped_backfill");
                        continue;
                    }
                    byte[] type = header(r, "x-event-type");
                    if (type != null && !DEFAULT_TYPE.equals(new String(type, java.nio.charset.StandardCharsets.UTF_8))) {
                        metrics.inc("skipped_parts");               // a message of a multi-message transaction: Flink assembles it
                        continue;
                    }
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
            // shutting down: wait (bounded) for the decisions in flight, then commit what is durable
            long until = System.currentTimeMillis() + Env.lng("DRAIN_TIMEOUT_MS", 20_000);
            while (inflight.availablePermits() < maxInflight && System.currentTimeMillis() < until) Thread.sleep(20);
            commit(consumer, pending, nextOffset, true);
        }
    }

    private void submit(ConsumerRecord<byte[], byte[]> r, TopicPartition tp, long pollUs,
                        Map<TopicPartition, ConcurrentSkipListSet<Long>> pending) {
        boolean handedOff = false;
        try {
            long ingest = pollUs;
            BenchTrace.Builder trace;
            TransactionEvent ev;
            try {
                byte[] th = header(r, "x-bench-trace");
                trace = th != null ? BenchTrace.parseFrom(th).toBuilder() : BenchTrace.newBuilder();
                long dStart = BenchClock.nowUs();
                if (r.value() == null) throw new IllegalArgumentException("a record without a value (tombstone)");
                ev = TransactionEvent.parseFrom(r.value());
                String bad = invalid(ev);
                if (bad != null) throw new IllegalArgumentException(bad);
                Spans.point(trace, Stage.INGEST, IngressMode.ASYNC_KAFKA_GATEWAY_VALUE, ingest);
                Spans.add(trace, Stage.DESERIALIZE, 0, dStart, BenchClock.nowUs() - dStart);
            } catch (Exception e) {
                metrics.inc("poison_records");
                deadLetter.send(r, e);
                return;
            }
            io.realy.contracts.flows.v1.TxnContext txn = null;
            byte[] tx = header(r, "x-txn");
            if (tx != null) {
                try {
                    txn = io.realy.contracts.flows.v1.TxnContext.parseFrom(tx);
                } catch (Exception e) {
                    metrics.inc("poison_txn_context");
                }
            }
            var req = new ScoringOrchestrator.Request(ev, trace, ingest, IngressMode.ASYNC_KAFKA_GATEWAY, null, -1, 0,
                    pipeline::publishFinal, txn);
            // process() always completes with a durable decision (an error becomes a fail-safe one)
            pipeline.process(req).whenComplete((d, err) -> done(pending, tp, r.offset(), err));
            handedOff = true;
        } finally {
            if (!handedOff) done(pending, tp, r.offset(), null);
        }
    }

    /** The record is settled (durably decided, or dead-lettered): its offset may be committed past. */
    private void done(Map<TopicPartition, ConcurrentSkipListSet<Long>> pending, TopicPartition tp, long offset, Throwable err) {
        try {
            if (err != null) {
                metrics.inc("pipeline_errors");
                LOG.error("event at {}-{} has no durable decision: {}", tp, offset, err.toString());
            }
            var p = pending.get(tp);      // gone after a rebalance or a topic switch: nothing to commit for it here
            if (p != null) p.remove(offset);
        } finally {
            inflight.release();
        }
    }

    /** Why an event cannot be scored (null: it can): the ids every decision needs. */
    static String invalid(TransactionEvent ev) {
        if (ev.getCustomerId() == 0) return "customer_id is 0 (the partition key)";
        if (ev.getEventTimeUs() == 0) return "event_time_us is 0";
        return null;
    }

    private static final String DEFAULT_TYPE = io.realy.scoring.binding.PipelineBinding.current().defaultType;
    private final io.realy.common.DeadLetters deadLetter = new io.realy.common.DeadLetters("gateway");

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

    /** Stop polling, let the in-flight decisions finish (DRAIN_TIMEOUT_MS), commit, then return. */
    @Override
    public void close() {
        running = false;
        for (Thread t : threads) {
            try {
                t.join(Env.lng("DRAIN_TIMEOUT_MS", 20_000) + 5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        deadLetter.close();
    }
}
