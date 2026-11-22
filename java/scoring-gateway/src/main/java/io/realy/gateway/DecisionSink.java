package io.realy.gateway;

import io.realy.common.Env;
import io.realy.common.KafkaProps;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.contracts.scoring.v1.DecisionRecord;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The audit write-ahead log. Every DecisionRecord is produced to the decisions topic (acks=all,
 * idempotent) and the caller waits for the ack before it counts the decision as made (the async rail
 * releases the source offset, the sync rail replies).
 *
 * <p>When the broker cannot take it, the record is appended to a local spool and fsynced first: only
 * then is it durable. The spool is framed (length + CRC32) so a write torn by a crash is detected and cut
 * off, never misread. A background replayer ships it to the broker while the gateway runs: it rotates the
 * file before reading (new writes never mix in), persists its position after every ack (a crash re-sends
 * at most the one record in flight; consumers keep one decision per event_id and revision), and deletes the
 * file once it is all on the broker. When neither the broker nor the spool can take a record, {@link #persist}
 * keeps retrying and does not complete, so the source offset is held (at-least-once, nothing lost). The
 * spool belongs on a persistent volume (AUDIT_SPOOL): on a pod's scratch disk it dies with the pod.
 */
public final class DecisionSink implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DecisionSink.class);
    private final org.apache.kafka.clients.producer.Producer<byte[], byte[]> producer;
    private final String decisionsTopic = Env.str("DECISIONS_TOPIC", "payments.decisions");
    private final String eventsTopic = Env.str("EVENTS_TOPIC", "payments.events.raw");
    private final Path spoolPath;
    private final Path replayPath;
    private final Path posPath;
    private final ScheduledExecutorService bg = Executors.newScheduledThreadPool(1, r -> {
        Thread t = new Thread(r, "audit-spool");
        t.setDaemon(true);
        return t;
    });
    private FileChannel spool;
    private final AtomicLong spooled = new AtomicLong();
    private final AtomicLong replayed = new AtomicLong();
    private final AtomicLong spoolFailures = new AtomicLong();
    private volatile long lastAckMs = System.currentTimeMillis();
    private volatile long lastErrorMs = 0;

    public DecisionSink(String clientId) {
        this(new KafkaProducer<>(KafkaProps.producer(clientId)), Env.lng("AUDIT_SPOOL_REPLAY_MS", 5000),
                Path.of(Env.str("AUDIT_SPOOL", "/tmp/realy-audit.spool")));
    }

    /** With any producer (tests: Kafka's MockProducer); `replayEveryMs` 0 = replay only when called. */
    DecisionSink(org.apache.kafka.clients.producer.Producer<byte[], byte[]> producer, long replayEveryMs, Path spool) {
        this.producer = producer;
        this.spoolPath = spool;
        this.replayPath = Path.of(spool + ".replaying");
        this.posPath = Path.of(spool + ".replaying.pos");
        if (replayEveryMs > 0) bg.scheduleWithFixedDelay(this::replaySafely, 0, replayEveryMs, TimeUnit.MILLISECONDS);
    }

    /** Produce to the decisions topic; completes on the broker's ack. */
    public CompletableFuture<Void> write(DecisionRecord rec) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        byte[] key = Long.toUnsignedString(rec.getEvent().getCustomerId()).getBytes(StandardCharsets.UTF_8);
        try {
            producer.send(new ProducerRecord<>(decisionsTopic, key, rec.toByteArray()), (md, err) -> {
                if (err != null) {
                    lastErrorMs = System.currentTimeMillis();
                    f.completeExceptionally(err);
                } else {
                    lastAckMs = System.currentTimeMillis();
                    f.complete(null);
                }
            });
        } catch (RuntimeException e) {
            lastErrorMs = System.currentTimeMillis();
            f.completeExceptionally(e);
        }
        return f;
    }

    /**
     * Make `rec` durable: the spool when the broker failed, else keep retrying the broker (and the spool)
     * every second until one of them takes it. Completes only when the record is durable.
     */
    public CompletableFuture<Void> persist(DecisionRecord rec) {
        if (spool(rec)) return CompletableFuture.completedFuture(null);
        CompletableFuture<Void> done = new CompletableFuture<>();
        retry(rec, done, 1);
        return done;
    }

    private void retry(DecisionRecord rec, CompletableFuture<Void> done, int attempt) {
        bg.schedule(() -> write(rec).whenComplete((ok, err) -> {
            if (err == null || spool(rec)) {
                done.complete(null);
            } else {
                if (attempt % 30 == 1) LOG.error("audit record {} not durable after {} attempts: broker and spool unavailable",
                        rec.getEventId(), attempt);
                retry(rec, done, attempt + 1);
            }
        }), Math.min(1000L * attempt, 5000L), TimeUnit.MILLISECONDS);
    }

    /** Mirror a sync request to the raw-events topic so the Flink feature job sees it. */
    public void mirror(TransactionEvent e, byte[] traceHeader) {
        byte[] key = Long.toUnsignedString(e.getCustomerId()).getBytes(StandardCharsets.UTF_8);
        var rec = new ProducerRecord<byte[], byte[]>(eventsTopic, null, e.getEventTimeUs() / 1000, key, e.toByteArray());
        rec.headers().add(new RecordHeader("x-mirrored", new byte[] {1}));
        if (traceHeader != null) {
            rec.headers().add(new RecordHeader("x-bench-trace", traceHeader));
            try {
                String run = io.realy.contracts.bench.v1.BenchTrace.parseFrom(traceHeader).getRunId();
                rec.headers().add(new RecordHeader("x-run-id", run.getBytes(StandardCharsets.UTF_8)));
            } catch (com.google.protobuf.InvalidProtocolBufferException ignored) {
                // trace is optional
            }
        }
        producer.send(rec, (md, err) -> {
            if (err != null) mirrorFailures.incrementAndGet();
        });
    }

    private final AtomicLong mirrorFailures = new AtomicLong();

    /** Append to the spool and fsync; false when the spool cannot take it (the record is not durable). */
    public synchronized boolean spool(DecisionRecord rec) {
        try {
            if (spool == null) {
                Files.createDirectories(spoolPath.toAbsolutePath().getParent());
                spool = FileChannel.open(spoolPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            }
            byte[] b = rec.toByteArray();
            CRC32 crc = new CRC32();
            crc.update(b);
            ByteBuffer buf = ByteBuffer.allocate(8 + b.length).putInt(b.length).putInt((int) crc.getValue()).put(b).flip();
            while (buf.hasRemaining()) spool.write(buf);
            spool.force(false);
            spooled.incrementAndGet();
            return true;
        } catch (IOException e) {
            spoolFailures.incrementAndGet();
            LOG.error("audit spool write failed ({}): the record is not durable", spoolPath, e);
            try {
                if (spool != null) spool.close();
            } catch (IOException ignored) {
                // reopened on the next write
            }
            spool = null;
            return false;
        }
    }

    private void replaySafely() {
        try {
            replay();
        } catch (Exception e) {
            LOG.warn("audit spool replay paused: {}", e.toString());
        }
    }

    /** Ship the spool to the broker: rotate, replay from the persisted position, delete when done. */
    void replay() throws Exception {
        synchronized (this) {
            if (!Files.exists(replayPath)) {
                if (!Files.exists(spoolPath) || Files.size(spoolPath) == 0) return;
                if (spool != null) {
                    spool.close();
                    spool = null;
                }
                Files.move(spoolPath, replayPath, StandardCopyOption.ATOMIC_MOVE);
            }
        }
        long pos = Files.exists(posPath) ? Long.parseLong(Files.readString(posPath).trim()) : 0L;
        try (FileChannel in = FileChannel.open(replayPath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer head = ByteBuffer.allocate(8);
            while (pos < in.size()) {
                head.clear();
                if (readFully(in, head, pos) < 8) {
                    truncate(in, pos, "a torn record header");
                    break;
                }
                head.flip();
                int len = head.getInt();
                int crc = head.getInt();
                if (len < 0 || pos + 8 + len > in.size()) {
                    truncate(in, pos, "a torn record");
                    break;
                }
                ByteBuffer body = ByteBuffer.allocate(len);
                readFully(in, body, pos + 8);
                CRC32 c = new CRC32();
                c.update(body.array());
                if ((int) c.getValue() != crc) {
                    truncate(in, pos, "a record failing its CRC");
                    break;
                }
                write(DecisionRecord.parseFrom(body.array())).get(30, TimeUnit.SECONDS);
                pos += 8 + len;
                Files.writeString(posPath, Long.toString(pos));
                replayed.incrementAndGet();
            }
        }
        Files.deleteIfExists(replayPath);
        Files.deleteIfExists(posPath);
        LOG.info("audit spool replayed ({} records so far)", replayed.get());
    }

    private static int readFully(FileChannel ch, ByteBuffer b, long at) throws IOException {
        int n = 0;
        while (b.hasRemaining()) {
            int r = ch.read(b, at + n);
            if (r < 0) break;
            n += r;
        }
        return n;
    }

    private void truncate(FileChannel ch, long at, String why) throws IOException {
        LOG.warn("audit spool: {} at byte {} (a crash mid-write); cutting it off", why, at);
        ch.truncate(at);
    }

    /** For readiness and /admin/info: spool backlog and the broker's last answer. */
    public java.util.Map<String, Object> status() {
        long backlog = 0;
        try {
            backlog = (Files.exists(spoolPath) ? Files.size(spoolPath) : 0) + (Files.exists(replayPath) ? Files.size(replayPath) : 0);
        } catch (IOException ignored) {
            // reported as 0
        }
        return java.util.Map.of("spool", spoolPath.toString(), "spool_bytes", backlog, "spooled", spooled.get(),
                "replayed", replayed.get(), "spool_failures", spoolFailures.get(), "mirror_failures", mirrorFailures.get(),
                "last_ack_ms_ago", System.currentTimeMillis() - lastAckMs,
                "last_error_ms_ago", lastErrorMs == 0 ? -1 : System.currentTimeMillis() - lastErrorMs);
    }

    /** The broker failed more recently than it acknowledged (or never acknowledged after failing). */
    public boolean brokerFailing() {
        return lastErrorMs > lastAckMs;
    }

    @Override
    public void close() {
        bg.shutdown();
        producer.close(Duration.ofSeconds(Env.lng("AUDIT_CLOSE_TIMEOUT_S", 10)));
        synchronized (this) {
            try {
                if (spool != null) spool.close();
            } catch (IOException ignored) {
                // closing
            }
        }
    }
}
