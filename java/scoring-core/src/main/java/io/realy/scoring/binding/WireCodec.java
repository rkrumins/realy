package io.realy.scoring.binding;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.WireFormat;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Any protobuf message, decoded in one pass over its bytes by a field-number plan compiled once per type
 * (no generated class, no reflection per event). Mirrors python/realy/bindings/codec.py.
 *
 * <p>Every singular scalar reachable through singular sub-messages has a slot of its own, named by its
 * dotted path ({@code card_info.status}, {@code merchant_info.address.country}); {@link #indexOf} finds
 * it, so bindings, features and rows address nested fields like flat ones. A sub-message is decoded in
 * place (no copy, no second pass); one sent twice merges, the last scalar wins. Up to {@link #MAX_DEPTH}
 * levels are inlined; deeper (or recursive) sub-messages are skipped.
 *
 * <p>Values: integers as Long (uint32 unsigned, uint64 as the signed long), bools, doubles (floats
 * widened), strings, enums by name (a number without a name as its decimal string); Timestamp and
 * Duration as Long microseconds; wrapper types (Int64Value...) unwrapped. A repeated field is one slot
 * holding a List (packed and unpacked encodings both accepted; a repeated message: a list of its
 * views); a map field a Map. Bytes, Any, Struct and Value are skipped. An absent field reads its proto3
 * default; an absent sub-message reads as all defaults (so presence of a sub-message is not visible:
 * test a field of it, e.g. {@code event.card_info.id != 0}).
 *
 * <p>The CEL {@code event} view nests: {@code event.card_info} is a map over the same values array.
 */
public final class WireCodec {
    enum Kind { INT32, INT64, SINT32, SINT64, SFIXED32, SFIXED64, UINT32, FIXED32, UINT64, FIXED64, BOOL, DOUBLE, FLOAT, STRING, ENUM,
        TIMESTAMP, LIST, MAP }

    /** Sub-messages inlined below the top level (a deeper or recursive one is skipped). */
    public static final int MAX_DEPTH = 4;
    /** A message with more slots than this is refused (a contract that big is a mistake, not a payload). */
    public static final int MAX_SLOTS = 1024;
    private static final Set<String> TIMES = Set.of("google.protobuf.Timestamp", "google.protobuf.Duration");
    private static final Set<String> WRAPPERS = Set.of("google.protobuf.DoubleValue", "google.protobuf.FloatValue",
            "google.protobuf.Int64Value", "google.protobuf.UInt64Value", "google.protobuf.Int32Value",
            "google.protobuf.UInt32Value", "google.protobuf.BoolValue", "google.protobuf.StringValue");
    private static final Set<String> SKIPPED = Set.of("google.protobuf.Any", "google.protobuf.Struct", "google.protobuf.Value",
            "google.protobuf.ListValue", "google.protobuf.BytesValue", "google.protobuf.Empty");

    public final Descriptors.Descriptor descriptor;
    final String[] names;
    final Map<String, Integer> index = new HashMap<>();
    private final Kind[] kinds;
    private final Object[] defaults;
    private final Map<Integer, String>[] enumNames;      // per slot: an enum's names by number (scalar or list of enums)
    private final Map<String, Integer>[] enumNumbers;    // per slot: the reverse (numeric reads of an enum)
    private final Node root;
    private static final Map<String, WireCodec> CODECS = new ConcurrentHashMap<>();

    /** One message type at one path: its fields by number, and its children for the CEL view. */
    static final class Node {
        Field[] byNumber = new Field[0];
        final Map<String, Object> children = new LinkedHashMap<>();   // name -> Integer slot | Node
    }

    /** How one field of a node is read. */
    static final class Field {
        int slot = -1;
        Kind kind;                 // scalar kind; a list's element kind; a map's value kind
        int wire;                  // expected wire type of a singular scalar / unpacked element
        Node child;                // a singular sub-message, inlined
        boolean wrapped;           // a wrapper type: the value is its field 1
        boolean list;
        boolean map;
        Kind keyKind;
        String elemType;           // a repeated message's (or a map's message value's) type
        volatile WireCodec elem;   // its codec, resolved on first use (a recursive type cannot recurse at construction)
        Map<Integer, String> enums;
    }

    private final List<String> nameList = new ArrayList<>();
    private final List<Kind> kindList = new ArrayList<>();
    private final List<Object> defaultList = new ArrayList<>();
    private final List<Map<Integer, String>> enumList = new ArrayList<>();

    @SuppressWarnings("unchecked")
    WireCodec(Descriptors.Descriptor d) {
        this.descriptor = d;
        this.root = compile(d, "", 0);
        int n = nameList.size();
        if (n > MAX_SLOTS) throw new IllegalArgumentException(d.getFullName() + ": " + n + " fields (more than " + MAX_SLOTS + ")");
        names = nameList.toArray(new String[0]);
        kinds = kindList.toArray(new Kind[0]);
        defaults = defaultList.toArray();
        enumNames = enumList.toArray(new Map[0]);
        enumNumbers = new Map[n];
        for (int i = 0; i < n; i++) {
            index.put(names[i], i);
            if (enumNames[i] != null) {
                Map<String, Integer> r = new HashMap<>();
                enumNames[i].forEach((num, name) -> r.putIfAbsent(name, num));
                enumNumbers[i] = r;
            }
        }
    }

    private int slot(String path, Kind kind, Object def, Map<Integer, String> enums) {
        nameList.add(path);
        kindList.add(kind);
        defaultList.add(def);
        enumList.add(enums);
        return nameList.size() - 1;
    }

    private Node compile(Descriptors.Descriptor d, String prefix, int depth) {
        Node n = new Node();
        Map<Integer, Field> fields = new HashMap<>();
        int max = 0;
        for (FieldDescriptor f : d.getFields()) {
            String path = prefix + f.getName();
            Field x = new Field();
            if (f.isMapField()) {
                FieldDescriptor k = f.getMessageType().findFieldByNumber(1), v = f.getMessageType().findFieldByNumber(2);
                if (v.getJavaType() == FieldDescriptor.JavaType.BYTE_STRING) continue;
                x.map = true;
                x.keyKind = kind(k);
                if (v.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                    if (unsupported(v.getMessageType())) continue;
                    x.elemType = v.getMessageType().getFullName();
                } else {
                    x.kind = kind(v);
                    x.enums = enumsOf(v);
                }
                x.slot = slot(path, Kind.MAP, Map.of(), x.enums);
                n.children.put(f.getName(), x.slot);
            } else if (f.isRepeated()) {
                if (f.getJavaType() == FieldDescriptor.JavaType.BYTE_STRING) continue;
                x.list = true;
                if (f.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                    if (unsupported(f.getMessageType())) continue;
                    x.elemType = f.getMessageType().getFullName();
                } else {
                    x.kind = kind(f);
                    x.wire = f.getLiteType().getWireType();
                    x.enums = enumsOf(f);
                }
                x.slot = slot(path, Kind.LIST, List.of(), x.enums);
                n.children.put(f.getName(), x.slot);
            } else if (f.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                String type = f.getMessageType().getFullName();
                if (TIMES.contains(type)) {
                    x.kind = Kind.TIMESTAMP;
                    x.slot = slot(path, Kind.TIMESTAMP, 0L, null);
                    n.children.put(f.getName(), x.slot);
                } else if (WRAPPERS.contains(type)) {
                    FieldDescriptor vf = f.getMessageType().findFieldByNumber(1);
                    x.wrapped = true;
                    x.kind = kind(vf);
                    x.wire = vf.getLiteType().getWireType();
                    x.slot = slot(path, x.kind, defaultOf(x.kind, null), null);
                    n.children.put(f.getName(), x.slot);
                } else if (SKIPPED.contains(type) || depth >= MAX_DEPTH) {
                    continue;
                } else {
                    x.child = compile(f.getMessageType(), path + ".", depth + 1);
                    n.children.put(f.getName(), x.child);
                }
            } else if (f.getJavaType() == FieldDescriptor.JavaType.BYTE_STRING) {
                continue;
            } else {
                x.kind = kind(f);
                x.wire = f.getLiteType().getWireType();
                x.enums = enumsOf(f);
                x.slot = slot(path, x.kind, defaultOf(x.kind, x.enums), x.enums);
                n.children.put(f.getName(), x.slot);
            }
            fields.put(f.getNumber(), x);
            max = Math.max(max, f.getNumber());
        }
        n.byNumber = new Field[max + 1];
        fields.forEach((num, x) -> n.byNumber[num] = x);
        return n;
    }

    private static boolean unsupported(Descriptors.Descriptor d) {
        return SKIPPED.contains(d.getFullName());
    }

    private static Map<Integer, String> enumsOf(FieldDescriptor f) {
        if (f.getJavaType() != FieldDescriptor.JavaType.ENUM) return null;
        Map<Integer, String> m = new HashMap<>();
        for (var v : f.getEnumType().getValues()) m.putIfAbsent(v.getNumber(), v.getName());
        return m;
    }

    private static Object defaultOf(Kind k, Map<Integer, String> enums) {
        return switch (k) {
            case BOOL -> Boolean.FALSE;
            case DOUBLE, FLOAT -> 0.0d;
            case STRING -> "";
            case ENUM -> enumName(enums, 0);
            default -> 0L;
        };
    }

    /** The codec of a contract message, compiled once. */
    public static WireCodec of(String fullName) {
        WireCodec c = CODECS.get(fullName);
        if (c == null) {
            c = new WireCodec(Contracts.message(fullName));
            WireCodec prev = CODECS.putIfAbsent(fullName, c);
            if (prev != null) c = prev;
        }
        return c;
    }

    public static WireCodec of(Descriptors.Descriptor d) {
        WireCodec c = CODECS.get(d.getFullName());
        if (c == null) {
            c = new WireCodec(d);
            WireCodec prev = CODECS.putIfAbsent(d.getFullName(), c);
            if (prev != null) c = prev;
        }
        return c;
    }

    private WireCodec elem(Field f) {
        WireCodec e = f.elem;
        if (e == null) {
            e = of(findType(f.elemType));
            f.elem = e;
        }
        return e;
    }

    private Descriptors.Descriptor findType(String fullName) {
        // the element type lives in this message's file or one it imports
        Descriptors.Descriptor d = findIn(descriptor.getFile(), fullName, new java.util.HashSet<>());
        return d != null ? d : Contracts.message(fullName);
    }

    private static Descriptors.Descriptor findIn(Descriptors.FileDescriptor file, String fullName, Set<String> seen) {
        if (!seen.add(file.getName())) return null;
        String pkg = file.getPackage();
        String rel = pkg.isEmpty() ? fullName : fullName.startsWith(pkg + ".") ? fullName.substring(pkg.length() + 1) : null;
        if (rel != null) {
            String[] parts = rel.split("\\.");
            Descriptors.Descriptor d = file.findMessageTypeByName(parts[0]);
            for (int i = 1; d != null && i < parts.length; i++) d = d.findNestedTypeByName(parts[i]);
            if (d != null) return d;
        }
        for (Descriptors.FileDescriptor dep : file.getDependencies()) {
            Descriptors.Descriptor d = findIn(dep, fullName, seen);
            if (d != null) return d;
        }
        return null;
    }

    private static Kind kind(FieldDescriptor f) {
        return switch (f.getType()) {
            case INT32 -> Kind.INT32;
            case INT64 -> Kind.INT64;
            case SINT32 -> Kind.SINT32;
            case SINT64 -> Kind.SINT64;
            case SFIXED32 -> Kind.SFIXED32;
            case SFIXED64 -> Kind.SFIXED64;
            case UINT32 -> Kind.UINT32;
            case FIXED32 -> Kind.FIXED32;
            case UINT64 -> Kind.UINT64;
            case FIXED64 -> Kind.FIXED64;
            case BOOL -> Kind.BOOL;
            case DOUBLE -> Kind.DOUBLE;
            case FLOAT -> Kind.FLOAT;
            case STRING -> Kind.STRING;
            case ENUM -> Kind.ENUM;
            default -> throw new IllegalArgumentException("unsupported field type " + f.getType());
        };
    }

    static String enumName(Map<Integer, String> names, int number) {
        String s = names == null ? null : names.get(number);
        return s != null ? s : Integer.toString(number);
    }

    public int size() {
        return names.length;
    }

    /** Index of a field (a dotted path for a nested one) in the decoded values, or -1. */
    public int indexOf(String field) {
        Integer i = index.get(field);
        return i == null ? -1 : i;
    }

    /** The dotted path of every slot, in index order. */
    public List<String> paths() {
        return List.of(names);
    }

    /** Decode one message: values indexed like {@link #indexOf}. */
    public Object[] decode(byte[] bytes) throws IOException {
        Object[] v = defaults.clone();
        read(CodedInputStream.newInstance(bytes), root, v);
        return v;
    }

    private Object[] decodeFrom(CodedInputStream in) throws IOException {
        Object[] v = defaults.clone();
        read(in, root, v);
        return v;
    }

    private void read(CodedInputStream in, Node n, Object[] v) throws IOException {
        while (true) {
            int tag = in.readTag();
            if (tag == 0) return;
            int num = WireFormat.getTagFieldNumber(tag);
            int wt = WireFormat.getTagWireType(tag);
            Field f = num < n.byNumber.length ? n.byNumber[num] : null;
            if (f == null) {
                in.skipField(tag);
                continue;
            }
            if (f.child != null || f.kind == Kind.TIMESTAMP || f.wrapped || f.map) {
                if (wt != WireFormat.WIRETYPE_LENGTH_DELIMITED) {
                    in.skipField(tag);
                    continue;
                }
                int old = in.pushLimit(in.readRawVarint32());
                if (f.child != null) read(in, f.child, v);
                else if (f.kind == Kind.TIMESTAMP) v[f.slot] = readTime(in);
                else if (f.wrapped) v[f.slot] = readWrapped(in, f);
                else readEntry(in, f, v);
                in.skipRawBytes(in.getBytesUntilLimit());
                in.popLimit(old);
                continue;
            }
            if (f.list) {
                List<Object> l = list(v, f.slot);
                if (f.elemType != null) {
                    if (wt != WireFormat.WIRETYPE_LENGTH_DELIMITED) {
                        in.skipField(tag);
                        continue;
                    }
                    WireCodec e = elem(f);
                    int old = in.pushLimit(in.readRawVarint32());
                    l.add(e.view(e.decodeFrom(in)));
                    in.popLimit(old);
                } else if (wt == WireFormat.WIRETYPE_LENGTH_DELIMITED && f.kind != Kind.STRING) {
                    int old = in.pushLimit(in.readRawVarint32());     // packed
                    while (in.getBytesUntilLimit() > 0) l.add(scalar(in, f.kind, f.enums));
                    in.popLimit(old);
                } else if (wt == f.wire) {
                    l.add(scalar(in, f.kind, f.enums));
                } else {
                    in.skipField(tag);
                }
                continue;
            }
            if (wt != f.wire) {
                in.skipField(tag);
                continue;
            }
            v[f.slot] = scalar(in, f.kind, f.enums);
        }
    }

    private static Object scalar(CodedInputStream in, Kind k, Map<Integer, String> enums) throws IOException {
        return switch (k) {
            case INT32 -> (long) in.readInt32();
            case INT64 -> in.readInt64();
            case SINT32 -> (long) in.readSInt32();
            case SINT64 -> in.readSInt64();
            case SFIXED32 -> (long) in.readSFixed32();
            case SFIXED64 -> in.readSFixed64();
            case UINT32 -> Integer.toUnsignedLong(in.readUInt32());
            case FIXED32 -> Integer.toUnsignedLong(in.readFixed32());
            case UINT64 -> in.readUInt64();
            case FIXED64 -> in.readFixed64();
            case BOOL -> in.readBool();
            case DOUBLE -> in.readDouble();
            case FLOAT -> (double) in.readFloat();
            case STRING -> in.readString();
            case ENUM -> enumName(enums, in.readEnum());
            default -> throw new IllegalStateException(k.name());
        };
    }

    /** A Timestamp or Duration: seconds (1) and nanos (2), as microseconds. */
    private static Long readTime(CodedInputStream in) throws IOException {
        long s = 0, nanos = 0;
        while (true) {
            int tag = in.readTag();
            if (tag == 0) break;
            int num = WireFormat.getTagFieldNumber(tag);
            if (num == 1 && WireFormat.getTagWireType(tag) == WireFormat.WIRETYPE_VARINT) s = in.readInt64();
            else if (num == 2 && WireFormat.getTagWireType(tag) == WireFormat.WIRETYPE_VARINT) nanos = in.readInt32();
            else in.skipField(tag);
        }
        return s * 1_000_000L + nanos / 1_000L;
    }

    private static Object readWrapped(CodedInputStream in, Field f) throws IOException {
        Object out = defaultOf(f.kind, null);
        while (true) {
            int tag = in.readTag();
            if (tag == 0) break;
            if (WireFormat.getTagFieldNumber(tag) == 1 && WireFormat.getTagWireType(tag) == f.wire) out = scalar(in, f.kind, null);
            else in.skipField(tag);
        }
        return out;
    }

    private void readEntry(CodedInputStream in, Field f, Object[] v) throws IOException {
        Object key = defaultOf(f.keyKind, null);
        Object val = f.elemType != null ? null : defaultOf(f.kind, f.enums);
        while (true) {
            int tag = in.readTag();
            if (tag == 0) break;
            int num = WireFormat.getTagFieldNumber(tag);
            if (num == 1) {
                key = scalar(in, f.keyKind, null);
            } else if (num == 2 && f.elemType != null && WireFormat.getTagWireType(tag) == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
                WireCodec e = elem(f);
                int old = in.pushLimit(in.readRawVarint32());
                val = e.view(e.decodeFrom(in));
                in.popLimit(old);
            } else if (num == 2 && f.elemType == null) {
                val = scalar(in, f.kind, f.enums);
            } else {
                in.skipField(tag);
            }
        }
        if (val == null) {
            WireCodec e = elem(f);
            val = e.view(e.defaults.clone());
        }
        map(v, f.slot).put(key, val);
    }

    @SuppressWarnings("unchecked")
    private static List<Object> list(Object[] v, int slot) {
        Object o = v[slot];
        if (o instanceof ArrayList<?> l) return (List<Object>) l;
        ArrayList<Object> l = new ArrayList<>();
        v[slot] = l;
        return l;
    }

    @SuppressWarnings("unchecked")
    private static Map<Object, Object> map(Object[] v, int slot) {
        Object o = v[slot];
        if (o instanceof LinkedHashMap<?, ?> m) return (Map<Object, Object>) m;
        LinkedHashMap<Object, Object> m = new LinkedHashMap<>();
        v[slot] = m;
        return m;
    }

    /**
     * A slot's value as a number, the way stream features read it: numbers as they are, a bool as 1 / 0,
     * an enum as its number (its name in the decoded values), 0 for anything else.
     */
    public double numeric(int slot, Object value) {
        if (value instanceof Number n) return n.doubleValue();
        if (value instanceof Boolean b) return b ? 1 : 0;
        if (value instanceof String s && enumNumbers[slot] != null) {
            Integer num = enumNumbers[slot].get(s);
            if (num != null) return num;
            try {
                return Integer.parseInt(s);      // a number the contract does not name
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        return 0;
    }

    /** The CEL {@code event} view of decoded values: nested maps over one values array, no copies. */
    public Map<String, Object> view(Object[] values) {
        return new TypeView(this, values, root);
    }

    public Map<String, Object> view(byte[] bytes) throws IOException {
        return view(decode(bytes));
    }

    /** Read-only map over one message (or sub-message) of a decoded event. */
    static final class TypeView extends AbstractMap<String, Object> {
        private final WireCodec c;
        private final Object[] v;
        private final Node node;

        TypeView(WireCodec c, Object[] v, Node node) {
            this.c = c;
            this.v = v;
            this.node = node;
        }

        private Object value(Object e) {
            if (e instanceof Integer i) return v[i];
            if (e instanceof Node n) return new TypeView(c, v, n);
            return null;
        }

        @Override
        public Object get(Object key) {
            return value(node.children.get(key));
        }

        @Override
        public boolean containsKey(Object key) {
            return node.children.containsKey(key);
        }

        @Override
        public int size() {
            return node.children.size();
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return new AbstractSet<>() {
                @Override
                public Iterator<Entry<String, Object>> iterator() {
                    Iterator<Map.Entry<String, Object>> it = node.children.entrySet().iterator();
                    return new Iterator<>() {
                        @Override
                        public boolean hasNext() {
                            return it.hasNext();
                        }

                        @Override
                        public Entry<String, Object> next() {
                            Map.Entry<String, Object> e = it.next();
                            return new SimpleImmutableEntry<>(e.getKey(), value(e.getValue()));
                        }
                    };
                }

                @Override
                public int size() {
                    return node.children.size();
                }
            };
        }
    }
}
