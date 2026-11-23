package io.realy.scoring.features;

import com.google.protobuf.Descriptors.FieldDescriptor;
import io.realy.contracts.payments.v1.TransactionEvent;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CEL binding of a TransactionEvent: field name -> value, enums by name, integers as Long. Must stay
 * in sync with {@code event_view} in python/realy/features/derived.py (`verify` compares them). Shared
 * rules for both languages: an enum number without a name reads as its decimal string; a uint64 reads as
 * the signed 64-bit value (Java's long).
 */
public final class EventView {
    private EventView() {}

    public static Map<String, Object> of(TransactionEvent e) {
        return of(e, io.realy.scoring.binding.TxnContexts.defaults());
    }

    /**
     * The view with {@code event.txn}: the context of the multi-message transaction the event was
     * assembled from ({@link io.realy.scoring.binding.TxnContexts#view}), or the defaults for a
     * one-message payment.
     */
    public static Map<String, Object> of(TransactionEvent e, Map<String, Object> txn) {
        Map<String, Object> m = new HashMap<>(48);
        m.put("txn", txn);
        m.put("event_id", e.getEventId());
        m.put("event_time_us", e.getEventTimeUs());
        m.put("payment_type", name(e.getPaymentType(), e.getPaymentTypeValue()));
        m.put("customer_id", e.getCustomerId());
        m.put("account_id", e.getAccountId());
        m.put("card_id", e.getCardId());
        m.put("card_type", name(e.getCardType(), e.getCardTypeValue()));
        m.put("amount_minor", e.getAmountMinor());
        m.put("currency", e.getCurrency());
        m.put("amount_usd_minor", e.getAmountUsdMinor());
        m.put("merchant_id", e.getMerchantId());
        m.put("mcc", (long) e.getMcc());
        m.put("merchant_country", e.getMerchantCountry());
        m.put("terminal_id", e.getTerminalId());
        m.put("channel", name(e.getChannel(), e.getChannelValue()));
        m.put("entry_mode", name(e.getEntryMode(), e.getEntryModeValue()));
        m.put("card_present", e.getCardPresent());
        m.put("lat", e.getLat());
        m.put("lon", e.getLon());
        m.put("device_id", e.getDeviceId());
        m.put("ip_v4", Integer.toUnsignedLong(e.getIpV4()));
        m.put("ip_country", e.getIpCountry());
        m.put("ip_asn", Integer.toUnsignedLong(e.getIpAsn()));
        m.put("payee_id", e.getPayeeId());
        m.put("cvv_match", e.getCvvMatch());
        m.put("avs_match", e.getAvsMatch());
        m.put("recurring", e.getRecurring());
        m.put("credit_limit_usd", Integer.toUnsignedLong(e.getCreditLimitUsd()));
        m.put("three_ds", name(e.getThreeDs(), e.getThreeDsValue()));
        m.put("pin_verified", e.getPinVerified());
        m.put("cryptogram_valid", e.getCryptogramValid());
        m.put("scheme_score", Integer.toUnsignedLong(e.getSchemeScore()));
        // the rich blocks (sub-messages, repeated and map fields): read by the generic codec, so the view is
        // the same as any bound message's; a payment that carries none gets the shared default views
        boolean any = false;
        for (FieldDescriptor f : BLOCKS) {
            if (f.isRepeated() ? e.getRepeatedFieldCount(f) > 0 : e.hasField(f)) {
                any = true;
                break;
            }
        }
        if (!any) {
            m.putAll(DEFAULT_BLOCKS);
        } else {
            try {
                Map<String, Object> v = CODEC.view(e.toByteArray());
                for (FieldDescriptor f : BLOCKS) m.put(f.getName(), v.get(f.getName()));
            } catch (java.io.IOException x) {
                throw new IllegalStateException(x);
            }
        }
        return m;
    }

    private static final io.realy.scoring.binding.WireCodec CODEC = io.realy.scoring.binding.WireCodec.of(TransactionEvent.getDescriptor());
    /** TransactionEvent's sub-message, repeated and map fields (the rich blocks), in field order. */
    private static final List<FieldDescriptor> BLOCKS = TransactionEvent.getDescriptor().getFields().stream()
            .filter(f -> f.isRepeated() || f.getJavaType() == FieldDescriptor.JavaType.MESSAGE).toList();
    private static final Map<String, Object> DEFAULT_BLOCKS = defaultBlocks();

    private static Map<String, Object> defaultBlocks() {
        try {
            Map<String, Object> v = CODEC.view(new byte[0]);
            Map<String, Object> out = new HashMap<>();
            for (FieldDescriptor f : BLOCKS) out.put(f.getName(), v.get(f.getName()));
            return Map.copyOf(out);
        } catch (java.io.IOException x) {
            throw new IllegalStateException(x);
        }
    }

    /** Enum value name; a number the contract does not name (a newer producer) reads as its decimal string. */
    static String name(com.google.protobuf.ProtocolMessageEnum v, int number) {
        return "UNRECOGNIZED".equals(((Enum<?>) v).name()) ? Integer.toString(number) : ((Enum<?>) v).name();
    }

    /** Per pipeline: its payment binding's field of each entity, where it is not the canonical flat one. */
    private static final Map<String, Map<String, FieldDescriptor[]>> BOUND_KEYS = new java.util.concurrent.ConcurrentHashMap<>();

    private static Map<String, FieldDescriptor[]> boundKeys(io.realy.scoring.binding.PipelineBinding pipe) {
        return BOUND_KEYS.computeIfAbsent(pipe.name, n -> {
            Map<String, FieldDescriptor[]> out = new HashMap<>();
            var b = pipe.binding(pipe.defaultType);
            if (b.message.equals(TransactionEvent.getDescriptor().getFullName())) {
                b.entities.forEach((ent, field) -> {
                    if (!field.equals(CANONICAL.get(ent))) out.put(ent, chain(field));
                });
            }
            return Map.copyOf(out);
        });
    }

    private static final Map<String, String> CANONICAL = Map.of("customer", "customer_id", "card", "card_id", "device", "device_id",
            "ip", "ip_v4", "merchant", "merchant_id", "terminal", "terminal_id", "payee", "payee_id", "account", "account_id");

    /**
     * Entity key (0 = entity absent on this event), from the field the pipeline's payment binding names for
     * it (contracts/events/<type>.yaml): the flat id fields read directly, a nested path (card: card_info.id)
     * through its field chain; a string id is hashed with FNV-1a 64.
     */
    public static long entityKey(TransactionEvent e, String entity) {
        return entityKey(e, entity, io.realy.scoring.binding.PipelineBinding.current());
    }

    /** {@link #entityKey(TransactionEvent, String)} through the payment binding of pipeline {@code pipe}. */
    public static long entityKey(TransactionEvent e, String entity, io.realy.scoring.binding.PipelineBinding pipe) {
        FieldDescriptor[] bound = boundKeys(pipe).get(entity);
        if (bound != null) {
            Object v = read(e, bound);
            return v instanceof String str ? (str.isEmpty() ? 0 : io.realy.common.Hashing.fnv1a64(str)) : ((Number) v).longValue();
        }
        return switch (entity) {
            case "customer" -> e.getCustomerId();
            case "card" -> e.getCardId();
            case "device" -> e.getDeviceId();
            case "ip" -> Integer.toUnsignedLong(e.getIpV4());
            case "merchant" -> e.getMerchantId();
            case "terminal" -> e.getTerminalId();
            case "payee" -> e.getPayeeId();
            case "account" -> e.getAccountId();
            // a registry entity the payment event does not carry (txn): absent; anything else is a mistake
            default -> {
                if (!io.realy.scoring.binding.EntityRegistry.load().contains(entity)) {
                    throw new IllegalArgumentException("unknown entity " + entity);
                }
                yield 0L;
            }
        };
    }

    /** Numeric value of an event field used by sum/max/avg. */
    public static double numeric(TransactionEvent e, String field) {
        return switch (field) {
            case "amount_usd_minor" -> e.getAmountUsdMinor();
            case "amount_minor" -> e.getAmountMinor();
            default -> {
                // any other field, nested ones by dotted path: an enum as its number, a bool as 1 / 0
                Object v = field(e, field, "numeric");
                yield v instanceof Boolean b ? (b ? 1 : 0) : v instanceof Number n ? n.doubleValue()
                        : v instanceof com.google.protobuf.Descriptors.EnumValueDescriptor ev ? ev.getNumber() : 0;
            }
        };
    }

    /** Key of an event field used by distinct/novel (strings hashed with FNV-1a 64). */
    public static long valueKey(TransactionEvent e, String field) {
        return switch (field) {
            case "merchant_id" -> e.getMerchantId();
            case "device_id" -> e.getDeviceId();
            case "payee_id" -> e.getPayeeId();
            case "card_id" -> e.getCardId();
            case "customer_id" -> e.getCustomerId();
            case "terminal_id" -> e.getTerminalId();
            case "mcc" -> e.getMcc();
            case "merchant_country" -> io.realy.common.Hashing.fnv1a64(e.getMerchantCountry());
            case "ip_country" -> io.realy.common.Hashing.fnv1a64(e.getIpCountry());
            case "ip_v4" -> Integer.toUnsignedLong(e.getIpV4());
            default -> {
                // any other flat field, read as the generic codec reads it (WireCodec, RowPlan.row): strings
                // and enum names hashed, booleans 1 / 0, unsigned 32-bit ints unsigned
                Object v = field(e, field, "key");
                yield v instanceof String str ? io.realy.common.Hashing.fnv1a64(str)
                        : v instanceof com.google.protobuf.Descriptors.EnumValueDescriptor ev ? io.realy.common.Hashing.fnv1a64(ev.getName())
                        : v instanceof Boolean b ? (b ? 1 : 0) : ((Number) v).longValue();
            }
        };
    }

    private static final Map<String, FieldDescriptor[]> CHAINS = new java.util.concurrent.ConcurrentHashMap<>();

    /** Whether a payment field (a dotted path for a nested one) is a scalar the event carries. */
    public static boolean has(String path) {
        try {
            chain(path);
            return true;
        } catch (IllegalArgumentException x) {
            return false;
        }
    }

    /** The field chain of a dotted path (card_info.status), compiled once. */
    static FieldDescriptor[] chain(String path) {
        FieldDescriptor[] c = CHAINS.get(path);
        if (c != null) return c;
        String[] parts = path.split("\\.");
        FieldDescriptor[] out = new FieldDescriptor[parts.length];
        com.google.protobuf.Descriptors.Descriptor d = TransactionEvent.getDescriptor();
        for (int i = 0; i < parts.length; i++) {
            FieldDescriptor f = d == null ? null : d.findFieldByName(parts[i]);
            boolean last = i == parts.length - 1;
            if (f == null || f.isRepeated() || (last == (f.getJavaType() == FieldDescriptor.JavaType.MESSAGE)))
                throw new IllegalArgumentException("not a scalar field of the payment event: " + path);
            out[i] = f;
            d = last ? null : f.getMessageType();
        }
        CHAINS.put(path, out);
        return out;
    }

    private static Object read(TransactionEvent e, FieldDescriptor[] chain) {
        com.google.protobuf.Message m = e;
        for (int i = 0; i < chain.length - 1; i++) m = (com.google.protobuf.Message) m.getField(chain[i]);   // absent: defaults
        FieldDescriptor f = chain[chain.length - 1];
        Object v = m.getField(f);
        var t = f.getType();
        if (v instanceof Integer i && (t == FieldDescriptor.Type.UINT32 || t == FieldDescriptor.Type.FIXED32)) return Integer.toUnsignedLong(i);
        if (v instanceof Integer i) return (long) i;
        if (v instanceof Float x) return (double) x;
        return v;
    }

    private static Object field(TransactionEvent e, String name, String use) {
        try {
            return read(e, chain(name));
        } catch (IllegalArgumentException x) {
            throw new IllegalArgumentException("unsupported " + use + " field " + name);
        }
    }
}
