"""Kafka security for a cluster you bring: librdkafka settings from the environment."""
import secrets

from realy.common import kafka


def test_security_config_from_env(monkeypatch):
    assert kafka.security_config({}) == {}
    pw = secrets.token_hex(8)                  # drawn per run: no credential-shaped literal in the repository
    env = {"KAFKA_SECURITY_PROTOCOL": "SASL_SSL", "KAFKA_SASL_MECHANISM": "SCRAM-SHA-512", "KAFKA_SASL_USERNAME": "bench",
           "KAFKA_SASL_PASSWORD": pw, "KAFKA_SSL_CA_LOCATION": "/etc/kafka/ca.pem"}
    assert kafka.security_config(env) == {"security.protocol": "SASL_SSL", "sasl.mechanism": "SCRAM-SHA-512",
                                          "sasl.username": "bench", "sasl.password": pw,
                                          "ssl.ca.location": "/etc/kafka/ca.pem"}
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    assert kafka.producer_conf()["security.protocol"] == "SASL_SSL"
    assert kafka.consumer_conf("g")["sasl.username"] == "bench"
