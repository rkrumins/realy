package io.realy.scoring;

import io.realy.common.LatencyModel;
import io.realy.common.ProtoJson;
import io.realy.contracts.bench.v1.ScoringConfig;
import io.realy.scoring.rules.RuleSet;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Current scoring config + compiled rules/latency models; swapped atomically on hot reload. */
public final class ScoringConfigHolder {
    private static final Logger LOG = LoggerFactory.getLogger(ScoringConfigHolder.class);

    public record Active(ScoringConfig cfg, RuleSet rules, LatencyModel featureDelay, LatencyModel modelDelay) {}

    private volatile Active active;
    private volatile Consumer<Active> listener = a -> { };

    public ScoringConfigHolder(ScoringConfig initial) {
        this.active = compile(initial);
    }

    public static ScoringConfig defaults() {
        return ScoringConfig.newBuilder().setVersion("default").setBudgetMs(100).setFeatureDeadlineMs(30)
                .setModelDeadlineMs(80).setFeatureSource("redis").setFeatureSamplePermille(1000)
                .setSelfInclusion(true).build();
    }

    private static Active compile(ScoringConfig c) {
        RuleSet rules = c.getRuleSetYaml().isBlank() ? RuleSet.bundled("baseline") : RuleSet.parse(c.getRuleSetYaml());
        // live threshold tuning without shipping a whole rule set (0 = keep the rule set's thresholds)
        rules = rules.withThresholds(c.getReviewThreshold(), c.getDeclineThreshold());
        return new Active(c, rules, new LatencyModel(c.getFeatureLookupDelay()), new LatencyModel(c.getModelExtraDelay()));
    }

    public Active get() {
        return active;
    }

    public void onChange(Consumer<Active> l) {
        this.listener = l;
    }

    /** Merge a (partial) proto3-JSON ScoringConfig onto the current one. */
    public synchronized String update(String json) {
        set(ProtoJson.overlay(active.cfg(), json));
        return ProtoJson.print(active.cfg());
    }

    public synchronized void set(ScoringConfig c) {
        Active a = compile(c);
        active = a;
        LOG.info("scoring config {} active (rules {}, thresholds review {} / decline {}, budget {} ms, hedge {} ms)",
                c.getVersion(), a.rules().version, a.rules().reviewThreshold, a.rules().declineThreshold, c.getBudgetMs(),
                c.getHedgeAfterMs());
        listener.accept(a);
    }
}
