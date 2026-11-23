package io.realy.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

class KafkaPropsTest {
    @Test
    void securityComesFromTheEnvironment() {
        assertThat(KafkaProps.security(Map.of())).isEmpty();
        var p = KafkaProps.security(Map.of("KAFKA_SECURITY_PROTOCOL", "SASL_SSL", "KAFKA_SASL_MECHANISM", "SCRAM-SHA-512",
                "KAFKA_SASL_USERNAME", "bench", "KAFKA_SASL_PASSWORD", "s3cret", "KAFKA_SSL_TRUSTSTORE_LOCATION", "/etc/kafka/ca.jks"));
        assertThat(p.getProperty("security.protocol")).isEqualTo("SASL_SSL");
        assertThat(p.getProperty("ssl.truststore.location")).isEqualTo("/etc/kafka/ca.jks");
        assertThat(p.getProperty("sasl.jaas.config")).contains("ScramLoginModule").contains("username=\"bench\"");
        var explicit = KafkaProps.security(Map.of("KAFKA_SASL_USERNAME", "u", "KAFKA_SASL_JAAS_CONFIG", "custom;"));
        assertThat(explicit.getProperty("sasl.jaas.config")).isEqualTo("custom;");
    }
}
