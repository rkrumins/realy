package io.realy.common;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dead letters: a record a service could not process goes to the dead-letter topic (DLQ_TOPIC, default
 * the pipeline's `dlq` topic) with its key, bytes and headers unchanged, plus x-dlq-error (why),
 * x-dlq-service and x-dlq-source (topic/partition/offset), so it can be inspected and replayed and is
 * never dropped silently. The producer is created on first use (a healthy pipeline never opens it).
 */
public final class DeadLetters implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DeadLetters.class);
    private final String service;
    private final String topic;
    private volatile KafkaProducer<byte[], byte[]> producer;
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();

    public DeadLetters(String service) {
        this(service, Env.str("DLQ_TOPIC", defaultTopic()));
    }

    public DeadLetters(String service, String topic) {
        this.service = service;
        this.topic = topic;
    }

    static String defaultTopic() {
        try {
            Class<?> pb = Class.forName("io.realy.scoring.binding.PipelineBinding");
            Object cur = pb.getMethod("current").invoke(null);
            @SuppressWarnings("unchecked")
            java.util.Map<String, String> t = (java.util.Map<String, String>) pb.getField("topics").get(cur);
            if (t.get("dlq") != null) return t.get("dlq");
        } catch (ReflectiveOperationException | RuntimeException ignored) {
            // not on the classpath here: the default below
        }
        return "payments.events.dlq";
    }

    public String topic() {
        return topic;
    }

    public void send(ConsumerRecord<byte[], byte[]> r, Throwable why) {
        send(r.key(), r.value(), r.headers(), r.topic() + "/" + r.partition() + "/" + r.offset(), why);
    }

    public void send(byte[] key, byte[] value, Iterable<Header> headers, String source, Throwable why) {
        String err = why.getClass().getSimpleName() + ": " + String.valueOf(why.getMessage());
        if (sent.get() + failed.get() < 20 || (sent.get() + failed.get()) % 1000 == 0) {
            LOG.warn("dead letter from {} ({}): {}", source, service, err);
        }
        try {
            var rec = new ProducerRecord<>(topic, key, value);
            if (headers != null) for (Header h : headers) rec.headers().add(h);
            rec.headers().add(new RecordHeader("x-dlq-error", err.getBytes(StandardCharsets.UTF_8)));
            rec.headers().add(new RecordHeader("x-dlq-service", service.getBytes(StandardCharsets.UTF_8)));
            rec.headers().add(new RecordHeader("x-dlq-source", source.getBytes(StandardCharsets.UTF_8)));
            producer().send(rec, (md, e) -> {
                if (e != null) {
                    failed.incrementAndGet();
                    LOG.error("dead letter could not be written to {}: {}", topic, e.toString());
                } else {
                    sent.incrementAndGet();
                }
            });
        } catch (RuntimeException e) {
            failed.incrementAndGet();
            LOG.error("dead letter could not be written to {}: {}", topic, e.toString());
        }
    }

    private KafkaProducer<byte[], byte[]> producer() {
        KafkaProducer<byte[], byte[]> p = producer;
        if (p == null) {
            synchronized (this) {
                if (producer == null) producer = new KafkaProducer<>(KafkaProps.producer("dlq-" + service));
                p = producer;
            }
        }
        return p;
    }

    public long sent() {
        return sent.get();
    }

    public long failed() {
        return failed.get();
    }

    @Override
    public void close() {
        if (producer != null) producer.close(Duration.ofSeconds(5));
    }
}
