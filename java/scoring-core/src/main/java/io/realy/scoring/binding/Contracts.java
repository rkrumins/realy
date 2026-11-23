package io.realy.scoring.binding;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import io.realy.common.Env;
import io.realy.common.ProtoJson;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The binding contracts on the classpath (copied from contracts/ by contracts-java): YAML files and the
 * descriptor set of every contract message, so any bound message can be decoded by name. A directory in
 * REALY_CONTRACTS overrides the classpath copy (tests, a user's own contracts).
 */
public final class Contracts {
    private Contracts() {}

    private static final Map<String, Descriptors.Descriptor> MESSAGES = new ConcurrentHashMap<>();
    private static volatile Map<String, Descriptors.FileDescriptor> files;

    /** The text of contracts/&lt;rel&gt; (REALY_CONTRACTS directory, else the classpath copy). */
    public static String text(String rel) {
        String dir = Env.str("REALY_CONTRACTS", "");
        try {
            if (!dir.isBlank()) return Files.readString(Path.of(dir, rel));
            try (InputStream in = Contracts.class.getClassLoader().getResourceAsStream("realy/contracts/" + rel)) {
                if (in == null) throw new IllegalArgumentException("contracts/" + rel + " not on the classpath");
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static JsonNode yaml(String rel) {
        try {
            return ProtoJson.yamlMapper().readTree(text(rel));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The descriptor of a contract message by its full protobuf name (realy.payments.v1.TransactionEvent). */
    public static Descriptors.Descriptor message(String fullName) {
        return MESSAGES.computeIfAbsent(fullName, n -> {
            for (Descriptors.FileDescriptor f : files().values()) {
                for (Descriptors.Descriptor d : f.getMessageTypes()) {
                    if (d.getFullName().equals(n)) return d;
                }
            }
            throw new IllegalArgumentException("no protobuf message " + n + " in the contracts (contracts/proto)");
        });
    }

    private static Map<String, Descriptors.FileDescriptor> files() {
        if (files == null) {
            synchronized (Contracts.class) {
                if (files == null) files = load();
            }
        }
        return files;
    }

    private static Map<String, Descriptors.FileDescriptor> load() {
        try (InputStream in = Contracts.class.getClassLoader().getResourceAsStream("realy/contracts/descriptors.binpb")) {
            if (in == null) throw new IllegalStateException("realy/contracts/descriptors.binpb not on the classpath");
            FileDescriptorSet set = FileDescriptorSet.parseFrom(in);
            Map<String, FileDescriptorProto> protos = new HashMap<>();
            for (FileDescriptorProto p : set.getFileList()) protos.put(p.getName(), p);
            Map<String, Descriptors.FileDescriptor> built = new HashMap<>();
            for (String name : protos.keySet()) build(name, protos, built);
            return built;
        } catch (IOException | Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException("contract descriptors", e);
        }
    }

    private static Descriptors.FileDescriptor build(String name, Map<String, FileDescriptorProto> protos,
                                                    Map<String, Descriptors.FileDescriptor> built)
            throws Descriptors.DescriptorValidationException {
        Descriptors.FileDescriptor have = built.get(name);
        if (have != null) return have;
        FileDescriptorProto p = protos.get(name);
        if (p == null) throw new IllegalStateException("descriptor set lacks " + name);
        Descriptors.FileDescriptor[] deps = new Descriptors.FileDescriptor[p.getDependencyCount()];
        for (int i = 0; i < deps.length; i++) deps[i] = build(p.getDependency(i), protos, built);
        Descriptors.FileDescriptor f = Descriptors.FileDescriptor.buildFrom(p, deps);
        built.put(name, f);
        return f;
    }
}
