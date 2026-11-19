package io.realy.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** proto3-JSON / YAML <-> protobuf. YAML scenario sections and admin payloads share one schema. */
public final class ProtoJson {
    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final JsonFormat.Parser PARSER = JsonFormat.parser().ignoringUnknownFields();
    private static final JsonFormat.Printer PRINTER =
            JsonFormat.printer().preservingProtoFieldNames().omittingInsignificantWhitespace();

    private ProtoJson() {}

    public static <B extends Message.Builder> B mergeJson(String json, B builder) {
        try {
            PARSER.merge(json, builder);
            return builder;
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("invalid " + builder.getDescriptorForType().getFullName() + ": " + e.getMessage(), e);
        }
    }

    public static <B extends Message.Builder> B mergeYaml(String yaml, B builder) {
        try {
            Object tree = YAML.readValue(yaml, Object.class);
            return mergeJson(JSON.writeValueAsString(tree), builder);
        } catch (IOException e) {
            throw new IllegalArgumentException("invalid YAML: " + e.getMessage(), e);
        }
    }

    public static <B extends Message.Builder> B mergeFile(Path path, B builder) throws IOException {
        String text = Files.readString(path);
        return path.toString().endsWith(".json") ? mergeJson(text, builder) : mergeYaml(text, builder);
    }

    public static String print(com.google.protobuf.MessageOrBuilder m) {
        try {
            return PRINTER.print(m);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(e);
        }
    }

    public static ObjectMapper yamlMapper() {
        return YAML;
    }

    public static ObjectMapper jsonMapper() {
        return JSON;
    }
}
