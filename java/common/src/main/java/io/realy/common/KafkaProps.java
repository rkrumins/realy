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

    /** Environment variable -> Kafka client property, for a cluster you bring (TLS / SASL). The Python
     *  side reads the same variables (realy/common/kafka.py SECURITY_ENV). Unset: PLAINTEXT. */
    private static final String[][] SECURITY = {
        {"KAFKA_SECURITY_PROTOCOL", "security.protocol"},
        {"KAFKA_SASL_MECHANISM", "sasl.mechanism"},
        {"KAFKA_SASL_JAAS_CONFIG", "sasl.jaas.config"},
        {"KAFKA_SSL_TRUSTSTORE_LOCATION", "ssl.truststore.location"},
        {"KAFKA_SSL_TRUSTSTORE_PASSWORD", "ssl.truststore.password"},
        {"KAFKA_SSL_TRUSTSTORE_TYPE", "ssl.truststore.type"},
        {"KAFKA_SSL_KEYSTORE_LOCATION", "ssl.keystore.location"},
        {"KAFKA_SSL_KEYSTORE_PASSWORD", "ssl.keystore.password"},
        {"KAFKA_SSL_KEY_PASSWORD", "ssl.key.password"},
        {"KAFKA_SSL_ENDPOINT_IDENTIFICATION", "ssl.endpoint.identification.algorithm"},
    };

    /** The security properties set in the environment (with SASL user / password composed into a JAAS line). */
    public static Properties security() {
        return security(System.getenv());
    }

    static Properties security(java.util.Map<String, String> env) {
        Properties p = new Properties();
        for (String[] kv : SECURITY) {
            String v = env.get(kv[0]);
            if (v != null && !v.isBlank()) p.put(kv[1], v);
        }
        String user = env.get("KAFKA_SASL_USERNAME");
        if (user != null && !user.isBlank() && !p.containsKey("sasl.jaas.config")) {
            String mech = p.getProperty("sasl.mechanism", "PLAIN");
            String module = mech.startsWith("SCRAM") ? "org.apache.kafka.common.security.scram.ScramLoginModule"
                    : "org.apache.kafka.common.security.plain.PlainLoginModule";
            p.put("sasl.jaas.config", module + " required username=\"" + user + "\" password=\""
                    + saslPassword(env) + "\";");
        }
        return p;
    }

    /** KAFKA_SASL_PASSWORD_FILE (a mounted secret) or KAFKA_SASL_PASSWORD. */
    static String saslPassword(java.util.Map<String, String> env) {
        String f = env.get("KAFKA_SASL_PASSWORD_FILE");
        if (f != null && !f.isBlank()) {
            try {
                return java.nio.file.Files.readString(java.nio.file.Path.of(f)).trim();
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException("KAFKA_SASL_PASSWORD_FILE", e);
            }
        }
        return env.getOrDefault("KAFKA_SASL_PASSWORD", "");
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
        // send() blocks for metadata or buffer space at most this long (the default 60 s would stall the
        // threads completing decisions while a broker is unreachable)
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Env.str("KAFKA_MAX_BLOCK_MS", "5000"));
        p.putAll(security());
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
        // fetch.min.bytes=1: the broker completes a waiting fetch as soon as a record is appended, so a
        // long max wait costs no latency (only fetch.min.bytes > 1 would) while avoiding empty-fetch churn.
        p.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, Env.str("KAFKA_FETCH_MAX_WAIT_MS", "100"));
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Env.str("KAFKA_MAX_POLL_RECORDS", "2000"));
        p.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, Integer.toString(4 << 20));
        p.putAll(security());
        return p;
    }
}
