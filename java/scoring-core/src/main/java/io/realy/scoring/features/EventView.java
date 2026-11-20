package io.realy.scoring.features;

import io.realy.contracts.payments.v1.TransactionEvent;
import java.util.HashMap;
import java.util.Map;

/**
 * CEL binding of a TransactionEvent: field name -> value, enums by name, integers as Long. Must stay
 * in sync with python/realy/features/eventview.py.
 */
public final class EventView {
    private EventView() {}

    public static Map<String, Object> of(TransactionEvent e) {
        Map<String, Object> m = new HashMap<>(48);
        m.put("event_id", e.getEventId());
        m.put("event_time_us", e.getEventTimeUs());
        m.put("payment_type", e.getPaymentType().name());
        m.put("customer_id", e.getCustomerId());
        m.put("account_id", e.getAccountId());
        m.put("card_id", e.getCardId());
        m.put("card_type", e.getCardType().name());
        m.put("amount_minor", e.getAmountMinor());
        m.put("currency", e.getCurrency());
        m.put("amount_usd_minor", e.getAmountUsdMinor());
        m.put("merchant_id", e.getMerchantId());
        m.put("mcc", (long) e.getMcc());
        m.put("merchant_country", e.getMerchantCountry());
        m.put("terminal_id", e.getTerminalId());
        m.put("channel", e.getChannel().name());
        m.put("entry_mode", e.getEntryMode().name());
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
        m.put("three_ds", e.getThreeDs().name());
        m.put("pin_verified", e.getPinVerified());
        m.put("cryptogram_valid", e.getCryptogramValid());
        m.put("scheme_score", Integer.toUnsignedLong(e.getSchemeScore()));
        return m;
    }

    /** Entity key (0 = entity absent on this event). */
    public static long entityKey(TransactionEvent e, String entity) {
        return switch (entity) {
            case "customer" -> e.getCustomerId();
            case "card" -> e.getCardId();
            case "device" -> e.getDeviceId();
            case "ip" -> Integer.toUnsignedLong(e.getIpV4());
            case "merchant" -> e.getMerchantId();
            case "terminal" -> e.getTerminalId();
            case "payee" -> e.getPayeeId();
            default -> throw new IllegalArgumentException("unknown entity " + entity);
        };
    }

    /** Numeric value of an event field used by sum/max/avg. */
    public static double numeric(TransactionEvent e, String field) {
        return switch (field) {
            case "amount_usd_minor" -> e.getAmountUsdMinor();
            case "amount_minor" -> e.getAmountMinor();
            default -> throw new IllegalArgumentException("unsupported numeric field " + field);
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
            default -> throw new IllegalArgumentException("unsupported key field " + field);
        };
    }
}
