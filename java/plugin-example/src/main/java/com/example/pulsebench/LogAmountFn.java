package com.example.pulsebench;

import io.realy.scoring.features.DerivedFn;
import java.util.Map;

/**
 * An example plugin derived function ({@code fn: log_amount}): log(1 + amount in USD), computed at scoring
 * time from the event's view. Its Python twin is {@code log_amount} in python/realy/plugin_example.py, and
 * both pass contracts/testdata/derived/log_amount.json. Registered by
 * META-INF/services/io.realy.scoring.features.DerivedFn.
 */
public final class LogAmountFn implements DerivedFn {
    @Override
    public String name() {
        return "log_amount";
    }

    @Override
    public double apply(Map<String, Object> event, Map<String, Double> f) {
        Object a = event.get("amount_usd_minor");
        long minor = a instanceof Number n ? n.longValue() : 0L;
        return Math.log1p(Math.max(0L, minor) / 100.0);
    }
}
