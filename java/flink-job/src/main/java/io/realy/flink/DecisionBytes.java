package io.realy.flink;

/** Kafka-ready decision: key (customer id) + serialized DecisionRecord. */
public class DecisionBytes {
    public byte[] key;
    public byte[] value;

    public DecisionBytes() {}

    public DecisionBytes(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }
}
