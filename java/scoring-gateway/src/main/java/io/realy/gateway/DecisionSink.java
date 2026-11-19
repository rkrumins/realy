package io.realy.gateway;

import io.realy.common.Env;
import io.realy.common.KafkaProps;
import io.realy.contracts.scoring.v1.DecisionRecord;
import io.realy.contracts.payments.v1.TransactionEvent;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The audit write-ahead log: every DecisionRecord is produced to {@code payments.decisions}
 * (acks=all, idempotent) and the caller waits for the ack before answering. On failure the record is
 * appended to a local spool file (replayed on restart) so payments are never blocked by the log.
 */
public final class DecisionSink implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DecisionSink.class);
    private final KafkaProducer<byte[], byte[]> producer;
    private final String decisionsTopic = Env.str("DECISIONS_TOPIC", "payments.decisions");
    private final String eventsTopic = Env.str("EVENTS_TOPIC", "payments.events.raw");
    private final Path spoolPath = Path.of(Env.str("AUDIT_SPOOL", "/tmp/realy-audit.spool"));
    private DataOutputStream spool;

    public DecisionSink(String clientId) {
        producer = new KafkaProducer<>(KafkaProps.producer(clientId));
        replaySpool();
    }

    public CompletableFuture<Void> write(DecisionRecord rec) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        byte[] key = Long.toUnsignedString(rec.getEvent().getCustomerId()).getBytes(StandardCharsets.UTF_8);
        try {
            producer.send(new ProducerRecord<>(decisionsTopic, key, rec.toByteArray()), (md, err) -> {
                if (err != null) f.completeExceptionally(err); else f.complete(null);
            });
        } catch (RuntimeException e) {
            f.completeExceptionally(e);
        }
        return f;
    }

    /** Mirror a sync request to the raw-events topic so the Flink feature job sees it. */
    public void mirror(TransactionEvent e, byte[] traceHeader) {
        byte[] key = Long.toUnsignedString(e.getCustomerId()).getBytes(StandardCharsets.UTF_8);
        var rec = new ProducerRecord<byte[], byte[]>(eventsTopic, null, e.getEventTimeUs() / 1000, key, e.toByteArray());
        rec.headers().add(new RecordHeader("x-mirrored", new byte[] {1}));
        if (traceHeader != null) rec.headers().add(new RecordHeader("x-bench-trace", traceHeader));
        producer.send(rec);
    }

    public synchronized void spool(DecisionRecord rec) {
        try {
            if (spool == null) spool = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(spoolPath.toFile(), true)));
            byte[] b = rec.toByteArray();
            spool.writeInt(b.length);
            spool.write(b);
            spool.flush();
        } catch (IOException e) {
            LOG.error("audit spool write failed", e);
        }
    }

    private void replaySpool() {
        if (!Files.exists(spoolPath)) return;
        try (var in = new java.io.DataInputStream(Files.newInputStream(spoolPath))) {
            int n = 0;
            while (in.available() > 0) {
                byte[] b = new byte[in.readInt()];
                in.readFully(b);
                write(DecisionRecord.parseFrom(b)).get();
                n++;
            }
            Files.delete(spoolPath);
            LOG.info("replayed {} spooled audit records", n);
        } catch (Exception e) {
            LOG.warn("audit spool replay incomplete: {}", e.toString());
        }
    }

    @Override
    public void close() {
        producer.close(Duration.ofSeconds(5));
    }
}
