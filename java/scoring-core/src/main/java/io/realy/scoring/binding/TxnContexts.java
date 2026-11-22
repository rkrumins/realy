package io.realy.scoring.binding;

import com.google.protobuf.Descriptors.FieldDescriptor;
import io.realy.contracts.flows.v1.TxnContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code event.txn} in CEL: the context of the multi-message transaction an event was assembled from, typed
 * per its contract (enums and strings as String, bools as Boolean, integers and counts as Long, floats and
 * elapsed seconds as Double), on top of the defaults of every contract, so an expression over it never
 * fails on a one-message payment. Mirrors {@code default_context} / {@code Assembler._ctx} in Python.
 */
public final class TxnContexts {
    private TxnContexts() {}

    private static volatile Map<String, Object> DEFAULT;
    private static volatile Map<String, String> KINDS;   // context name -> string | bool | int | float

    /** Every transaction contract on the classpath (contracts/transactions/*.yaml), by name. */
    public static List<String> contractNames() {
        List<String> out = new ArrayList<>();
        try (InputStream in = TxnContexts.class.getClassLoader().getResourceAsStream("realy/contracts/transactions.index")) {
            if (in != null) {
                for (String l : new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).split("\n")) {
                    if (!l.isBlank()) out.add(l.trim());
                }
                return out;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out;
    }

    private static synchronized void init() {
        if (DEFAULT != null) return;
        Map<String, Object> d = new LinkedHashMap<>();
        Map<String, String> k = new HashMap<>();
        d.put("transaction", "");
        d.put("stage", "");
        d.put("partial", Boolean.FALSE);
        d.put("parts", 0L);
        for (String name : contractNames()) {
            TransactionContract t = TransactionContract.load(name);
            for (TransactionContract.Context c : t.context) {
                String kind = kind(t, c);
                k.putIfAbsent(c.name(), kind);
                d.putIfAbsent(c.name(), defaultOf(t, c, kind));
            }
        }
        KINDS = k;
        DEFAULT = Collections.unmodifiableMap(d);
    }

    static String kind(TransactionContract t, TransactionContract.Context c) {
        return switch (c.kind()) {
            case "count" -> "int";
            case "changed" -> "bool";
            case "elapsed" -> "float";
            default -> {
                FieldDescriptor f = WireCodec.of(EventBinding.load(t.roles.get(c.role())).message).descriptor.findFieldByName(c.field());
                yield switch (f.getJavaType()) {
                    case ENUM, STRING -> "string";
                    case BOOLEAN -> "bool";
                    case DOUBLE, FLOAT -> "float";
                    default -> "int";
                };
            }
        };
    }

    private static Object defaultOf(TransactionContract t, TransactionContract.Context c, String kind) {
        if (kind.equals("string") && c.kind().equals("from")) {
            FieldDescriptor f = WireCodec.of(EventBinding.load(t.roles.get(c.role())).message).descriptor.findFieldByName(c.field());
            if (f.getJavaType() == FieldDescriptor.JavaType.ENUM) {
                var v = f.getEnumType().findValueByNumber(0);
                return v != null ? v.getName() : "0";
            }
            return "";
        }
        return switch (kind) {
            case "bool" -> Boolean.FALSE;
            case "int" -> 0L;
            case "float" -> 0.0d;
            default -> "";
        };
    }

    /** The default context (a one-message payment). */
    public static Map<String, Object> defaults() {
        if (DEFAULT == null) init();
        return DEFAULT;
    }

    /** The CEL view of a transaction's context (null: the defaults). */
    public static Map<String, Object> view(TxnContext c) {
        if (c == null) return defaults();
        if (DEFAULT == null) init();
        Map<String, Object> m = new HashMap<>(DEFAULT);
        m.put("transaction", c.getTransaction());
        m.put("stage", c.getStage());
        m.put("partial", c.getPartial());
        m.put("parts", Integer.toUnsignedLong(c.getPartsPresent()));
        c.getStringsMap().forEach(m::put);
        c.getNumbersMap().forEach((name, x) -> m.put(name, switch (KINDS.getOrDefault(name, "float")) {
            case "bool" -> x != 0;
            case "int" -> (long) (double) x;
            default -> x;
        }));
        return m;
    }
}
