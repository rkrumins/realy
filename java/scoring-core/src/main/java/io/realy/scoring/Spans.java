package io.realy.scoring;

import io.realy.common.BenchClock;
import io.realy.contracts.bench.v1.BenchTrace;
import io.realy.contracts.bench.v1.Span;
import io.realy.contracts.bench.v1.Stage;

/** Appends spans in this process' clock domain. */
public final class Spans {
    private Spans() {}

    public static void add(BenchTrace.Builder t, Stage stage, int attr, long startUs, long durUs) {
        if (t == null) return;
        t.addSpans(Span.newBuilder().setStage(stage).setAttr(attr).setDomain(BenchClock.domain())
                .setStartUs(startUs).setDurUs((int) Math.max(Math.min(durUs, Integer.MAX_VALUE), Integer.MIN_VALUE)));
    }

    public static void point(BenchTrace.Builder t, Stage stage, int attr, long atUs) {
        add(t, stage, attr, atUs, 0);
    }
}
