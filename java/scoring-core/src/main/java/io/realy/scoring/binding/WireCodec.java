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
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Any protobuf message, decoded in one pass over its bytes by a field-number plan compiled once per type
 * (no generated class, no reflection per event): the values of its flat fields (scalars and enums;
 * nested and repeated fields are skipped) as the CEL {@code event} view. Values follow EventView's types:
 * integers as Long (uint32 unsigned, uint64 as the signed long), bools, doubles (floats widened), strings,
 * enums by name (a number without a name as its decimal string). An absent field reads its proto3 default.
 * Mirrors python/realy/bindings/codec.py; GoldenPaymentsTest-style tests keep it equal to EventView.
 */
public final class WireCodec {
    enum Kind { INT32, INT64, SINT32, SINT64, SFIXED32, SFIXED64, UINT32, FIXED32, UINT64, FIXED64, BOOL, DOUBLE, FLOAT, STRING, ENUM }

    public final Descriptors.Descriptor descriptor;
    final String[] names;
    final Map<String, Integer> index = new HashMap<>();
    private final Kind[] kinds;
    private final int[] wireTypes;
    private final Object[] defaults;
    private final int[] byNumber;
    private final Map<Integer, String>[] enumNames;
    private static final Map<String, WireCodec> CODECS = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    WireCodec(Descriptors.Descriptor d) {
        this.descriptor = d;
        List<FieldDescriptor> flat = new ArrayList<>();
        int maxNumber = 0;
        for (FieldDescriptor f : d.getFields()) {
            if (f.isRepeated() || f.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                    || f.getJavaType() == FieldDescriptor.JavaType.BYTE_STRING) continue;
            flat.add(f);
            maxNumber = Math.max(maxNumber, f.getNumber());
        }
        int n = flat.size();
        names = new String[n];
        kinds = new Kind[n];
        wireTypes = new int[n];
        defaults = new Object[n];
        enumNames = new Map[n];
        byNumber = new int[maxNumber + 1];
        java.util.Arrays.fill(byNumber, -1);
        for (int i = 0; i < n; i++) {
            FieldDescriptor f = flat.get(i);
            names[i] = f.getName();
            index.put(f.getName(), i);
            byNumber[f.getNumber()] = i;
            kinds[i] = kind(f);
            wireTypes[i] = f.getLiteType().getWireType();
            if (kinds[i] == Kind.ENUM) {
                Map<Integer, String> m = new HashMap<>();
                for (var v : f.getEnumType().getValues()) m.putIfAbsent(v.getNumber(), v.getName());
                enumNames[i] = m;
                defaults[i] = enumName(m, 0);
            } else {
                defaults[i] = switch (kinds[i]) {
                    case BOOL -> Boolean.FALSE;
                    case DOUBLE, FLOAT -> 0.0d;
                    case STRING -> "";
                    default -> 0L;
                };
            }
        }
    }

    /** The codec of a contract message, compiled once. */
    public static WireCodec of(String fullName) {
        return CODECS.computeIfAbsent(fullName, n -> new WireCodec(Contracts.message(n)));
    }

    public static WireCodec of(Descriptors.Descriptor d) {
        return CODECS.computeIfAbsent(d.getFullName(), n -> new WireCodec(d));
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
        String s = names.get(number);
        return s != null ? s : Integer.toString(number);
    }

    public int size() {
        return names.length;
    }

    /** Index of a field in the decoded values, or -1. */
    public int indexOf(String field) {
        Integer i = index.get(field);
        return i == null ? -1 : i;
    }

    /** Decode the flat fields of one message: values indexed like {@link #indexOf}. */
    public Object[] decode(byte[] bytes) throws IOException {
        Object[] v = defaults.clone();
        CodedInputStream in = CodedInputStream.newInstance(bytes);
        while (true) {
            int tag = in.readTag();
            if (tag == 0) break;
            int num = WireFormat.getTagFieldNumber(tag);
            int i = num < byNumber.length ? byNumber[num] : -1;
            if (i < 0 || WireFormat.getTagWireType(tag) != wireTypes[i]) {
                in.skipField(tag);
                continue;
            }
            v[i] = switch (kinds[i]) {
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
                case ENUM -> enumName(enumNames[i], in.readEnum());
            };
        }
        return v;
    }

    /** The CEL {@code event} view of decoded values: one shared index, no per-event map nodes. */
    public Map<String, Object> view(Object[] values) {
        return new TypeView(this, values);
    }

    public Map<String, Object> view(byte[] bytes) throws IOException {
        return view(decode(bytes));
    }

    /** Read-only map over a decoded event's values. */
    static final class TypeView extends AbstractMap<String, Object> {
        private final WireCodec c;
        private final Object[] v;

        TypeView(WireCodec c, Object[] v) {
            this.c = c;
            this.v = v;
        }

        @Override
        public Object get(Object key) {
            Integer i = c.index.get(key);
            return i == null ? null : v[i];
        }

        @Override
        public boolean containsKey(Object key) {
            return c.index.containsKey(key);
        }

        @Override
        public int size() {
            return v.length;
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return new AbstractSet<>() {
                @Override
                public Iterator<Entry<String, Object>> iterator() {
                    return new Iterator<>() {
                        int i = 0;

                        @Override
                        public boolean hasNext() {
                            return i < v.length;
                        }

                        @Override
                        public Entry<String, Object> next() {
                            if (i >= v.length) throw new NoSuchElementException();
                            int k = i++;
                            return new SimpleImmutableEntry<>(c.names[k], v[k]);
                        }
                    };
                }

                @Override
                public int size() {
                    return v.length;
                }
            };
        }
    }
}
