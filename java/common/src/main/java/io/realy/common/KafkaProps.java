package io.realy.common;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Latency-tuned Kafka client settings (contracts/kafka/topics.yaml: client_latency_settings).
 * Defaults that silently destroy tail latency (fetch.max.wait.ms=500, linger) are overridden here.
 */
public final class KafkaProps {
    private KafkaProps() {}

    public static String bootstrap() {
        return Env.str("KAFKA_BOOTSTRAP", "localhost:9092");
    }

    public static Properties producer(String clientId) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        p.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, Env.str("KAFKA_ACKS", "all"));
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Env.str("KAFKA_IDEMPOTENCE", "true"));
        p.put(ProducerConfig.LINGER_MS_CONFIG, Env.str("KAFKA_LINGER_MS", "1"));
        p.put(ProducerConfig.BATCH_SIZE_CONFIG, Env.str("KAFKA_BATCH_SIZE", "131072"));
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, Env.str("KAFKA_COMPRESSION", "lz4"));
        p.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        p.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Long.toString(256L << 20));
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Env.str("KAFKA_DELIVERY_TIMEOUT_MS", "30000"));
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        return p;
    }

    public static Properties consumer(String groupId, String clientId) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Env.str("KAFKA_OFFSET_RESET", "latest"));
        p.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        p.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, Env.str("KAFKA_FETCH_MAX_WAIT_MS", "5"));
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Env.str("KAFKA_MAX_POLL_RECORDS", "2000"));
        p.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, Integer.toString(4 << 20));
        return p;
    }
}
