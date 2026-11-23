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
        String refused = productionRefusal(initial);
        if (refused != null) throw new IllegalArgumentException(refused);
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
        rules = rules.withThresholds(c.getReviewThreshold(), c.getDeclineThreshold(), c.getChallengeThreshold());
        return new Active(c, rules, new LatencyModel(c.getFeatureLookupDelay()), new LatencyModel(c.getModelExtraDelay()));
    }

    /**
     * In the production profile (REALY_PROFILE=production) the benchmark-only settings are refused: they
     * would approve without scoring, add latency or errors on purpose, or score without a feature store.
     */
    static String productionRefusal(ScoringConfig c) {
        if (!io.realy.common.Profile.production()) return null;
        if (c.getEcho()) return "echo is a calibration mode: refused in the production profile";
        if (c.getFeatureSource().equals("mock")) return "feature_source mock: refused in the production profile";
        if (c.hasFeatureLookupDelay() || c.hasModelExtraDelay()) {
            return "injected latency (feature_lookup_delay / model_extra_delay): refused in the production profile";
        }
        return null;
    }

    /**
     * The initial config: defaults, then SCORING_CONFIG_FILE (a mounted proto3-JSON file), then the
     * SCORING_CONFIG env value; RULES_FILE (a rule set YAML) replaces the bundled baseline.
     */
    public static ScoringConfig initial() {
        ScoringConfig c = defaults();
        String file = io.realy.common.Env.str("SCORING_CONFIG_FILE", "");
        try {
            if (!file.isBlank()) c = ProtoJson.overlay(c, java.nio.file.Files.readString(java.nio.file.Path.of(file)));
            String env = io.realy.common.Env.str("SCORING_CONFIG", "");
            if (!env.isBlank()) c = ProtoJson.overlay(c, env);
            String rules = io.realy.common.Env.str("RULES_FILE", "");
            if (!rules.isBlank()) c = c.toBuilder().setRuleSetYaml(java.nio.file.Files.readString(java.nio.file.Path.of(rules))).build();
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException("scoring config / rules file", e);
        }
        return c;
    }

    /**
     * Reload SCORING_CONFIG_FILE and RULES_FILE when they change (a ConfigMap update): polled every
     * CONFIG_WATCH_MS (default 2 s). A file that does not parse is logged and the active config kept.
     */
    public void watchFiles() {
        String cfgFile = io.realy.common.Env.str("SCORING_CONFIG_FILE", "");
        String rulesFile = io.realy.common.Env.str("RULES_FILE", "");
        if (cfgFile.isBlank() && rulesFile.isBlank()) return;
        long every = io.realy.common.Env.lng("CONFIG_WATCH_MS", 2000);
        Thread.ofPlatform().daemon().name("config-watch").start(() -> {
            long seen = stamp(cfgFile) + stamp(rulesFile);
            while (true) {
                try {
                    Thread.sleep(every);
                    long now = stamp(cfgFile) + stamp(rulesFile);
                    if (now != seen) {
                        seen = now;
                        set(initial());
                        LOG.info("scoring config reloaded from files");
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (RuntimeException e) {
                    LOG.error("config files changed but could not be applied (keeping the active config): {}", e.toString());
                }
            }
        });
    }

    private static long stamp(String f) {
        if (f.isBlank()) return 0;
        try {
            // a ConfigMap update swaps a symlink: the target's modification time (and size) changes
            var p = java.nio.file.Path.of(f);
            return java.nio.file.Files.getLastModifiedTime(p).toMillis() * 31 + java.nio.file.Files.size(p);
        } catch (java.io.IOException e) {
            return -1;
        }
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
        String refused = productionRefusal(c);
        if (refused != null) throw new IllegalArgumentException(refused);
        Active a = compile(c);
        active = a;
        LOG.info("scoring config {} active (rules {}, thresholds review {} / decline {}, budget {} ms, hedge {} ms)",
                c.getVersion(), a.rules().version, a.rules().reviewThreshold, a.rules().declineThreshold, c.getBudgetMs(),
                c.getHedgeAfterMs());
        listener.accept(a);
    }
}
