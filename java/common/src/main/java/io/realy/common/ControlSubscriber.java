package io.realy.common;

import io.realy.contracts.bench.v1.ControlMessage;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Subscribes to the compacted {@code bench.control} topic (proto3 JSON ControlMessages). Replays it
 * from the beginning on start, so a restarted pod immediately picks up the current run config, and
 * then applies live changes (rate, delays, thresholds, rule sets) pushed by the operator console.
 */
public final class ControlSubscriber implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ControlSubscriber.class);
    private final Thread thread;
    private volatile boolean running = true;

    public ControlSubscriber(String role, Consumer<ControlMessage> listener) {
        String topic = Env.str("CONTROL_TOPIC", "bench.control");
        var props = KafkaProps.consumer("control-" + role + "-" + UUID.randomUUID(), role + "-control");
        props.put("auto.offset.reset", "earliest");
        props.put("fetch.max.wait.ms", "100");
        thread = Thread.ofPlatform().daemon().name("control-" + role).start(() -> {
            try (var consumer = new KafkaConsumer<byte[], byte[]>(props)) {
                List<TopicPartition> parts = null;
                while (running && parts == null) {
                    try {
                        var infos = consumer.partitionsFor(topic, Duration.ofSeconds(5));
                        if (infos != null && !infos.isEmpty()) {
                            parts = infos.stream().map(i -> new TopicPartition(topic, i.partition())).collect(Collectors.toList());
                        }
                    } catch (Exception e) {
                        LOG.debug("control topic not ready: {}", e.toString());
                    }
                    if (parts == null) sleep(2000);
                }
                if (parts == null) return;
                consumer.assign(parts);
                consumer.seekToBeginning(parts);
                while (running) {
                    for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofMillis(250))) {
                        if (r.value() == null) continue;
                        try {
                            var msg = ProtoJson.mergeJson(new String(r.value(), StandardCharsets.UTF_8), ControlMessage.newBuilder()).build();
                            String t = msg.getTarget();
                            if (t.isEmpty() || t.equals("*") || t.equals(role)) {
                                listener.accept(msg);
                            }
                        } catch (Exception e) {
                            LOG.warn("bad control message at offset {}: {}", r.offset(), e.toString());
                        }
                    }
                }
            } catch (Exception e) {
                LOG.warn("control subscriber stopped: {}", e.toString());
            }
        });
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        running = false;
    }
}
