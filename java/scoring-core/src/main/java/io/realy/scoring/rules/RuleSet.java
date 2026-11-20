package io.realy.scoring.rules;

import com.fasterxml.jackson.databind.JsonNode;
import io.realy.common.ProtoJson;
import io.realy.contracts.scoring.v1.Outcome;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A versioned CEL rule set (rules/*.yaml) plus score thresholds. Immutable; hot-swapped as a whole.
 *
 * <p>Step-up (CHALLENGE): a rule may answer CHALLENGE, and an optional {@code thresholds.challenge}
 * opens a score band that steps the payment up (3DS / SCA challenge online, a PIN prompt in store,
 * in-app confirmation of a transfer) instead of reviewing it. {@code challenge.when} (CEL over
 * {@code event}) says which payments CAN be stepped up (not merchant-initiated, not an ATM, not
 * magstripe...): a CHALLENGE rule on a payment that cannot be falls back to REVIEW, and the score
 * band does not apply to it. Mirrored by python/realy/policy.py; verified on every run.
 */
public final class RuleSet {
    public record Rule(String id, Outcome action, String reason, Cel.Predicate when) {}

    public record Hit(String id, Outcome action, String reason) {}

    public final String version;
    public final float reviewThreshold;
    public final float declineThreshold;
    /** NaN: no challenge band (rule sets before step-up). */
    public final float challengeThreshold;
    /** null: every payment can be stepped up. */
    public final Cel.Predicate challengeable;
    public final List<Rule> rules;

    private RuleSet(String version, float review, float decline, float challenge, Cel.Predicate challengeable, List<Rule> rules) {
        this.version = version;
        this.reviewThreshold = review;
        this.declineThreshold = decline;
        this.challengeThreshold = challenge;
        this.challengeable = challengeable;
        this.rules = List.copyOf(rules);
    }

    public static RuleSet parse(String yaml) {
        try {
            JsonNode root = ProtoJson.yamlMapper().readTree(yaml);
            String version = root.path("version").asText("unversioned");
            JsonNode th = root.path("thresholds");
            float review = (float) th.path("review").asDouble(0.5);
            float decline = (float) th.path("decline").asDouble(0.9);
            float challenge = th.hasNonNull("challenge") && th.path("challenge").asDouble() > 0
                    ? (float) th.path("challenge").asDouble() : Float.NaN;
            String when = root.path("challenge").path("when").asText("").trim();
            Cel.Predicate challengeable = when.isEmpty() ? null : Cel.compile(when);
            List<Rule> rules = new ArrayList<>();
            for (JsonNode r : root.path("rules")) {
                String action = r.path("action").asText("REVIEW");
                Outcome o = switch (action) {
                    case "DECLINE" -> Outcome.DECLINE;
                    case "REVIEW" -> Outcome.REVIEW;
                    case "CHALLENGE" -> Outcome.CHALLENGE;
                    case "ALLOW", "APPROVE" -> Outcome.APPROVE;
                    default -> throw new IllegalArgumentException("unknown action " + action);
                };
                rules.add(new Rule(r.get("id").asText(), o, r.path("reason").asText(""), Cel.compile(r.get("when").asText())));
            }
            return new RuleSet(version, review, decline, challenge, challengeable, rules);
        } catch (IOException e) {
            throw new IllegalArgumentException("invalid rule set: " + e.getMessage(), e);
        }
    }

    /**
     * The same rules with score thresholds overridden (live tuning: ScoringConfig.review_threshold /
     * decline_threshold / challenge_threshold). A value {@code <= 0} keeps the rule set's own threshold.
     */
    public RuleSet withThresholds(float review, float decline) {
        return withThresholds(review, decline, 0);
    }

    public RuleSet withThresholds(float review, float decline, float challenge) {
        float r = review > 0 ? review : reviewThreshold;
        float d = decline > 0 ? decline : declineThreshold;
        float c = challenge > 0 ? challenge : challengeThreshold;
        if (r == reviewThreshold && d == declineThreshold && Float.compare(c, challengeThreshold) == 0) return this;
        if (r > d) throw new IllegalArgumentException("review threshold " + r + " > decline threshold " + d);
        if (!Float.isNaN(c) && c > d) throw new IllegalArgumentException("challenge threshold " + c + " > decline threshold " + d);
        String tag = "+t" + r + "/" + d + (Float.isNaN(c) ? "" : "/c" + c);
        return new RuleSet(version + tag, r, d, c, challengeable, rules);
    }

    public static RuleSet bundled(String name) {
        try (InputStream in = RuleSet.class.getClassLoader().getResourceAsStream("realy/rules/" + name + ".yaml")) {
            if (in == null) throw new IllegalArgumentException("rule set " + name + " not on classpath");
            return parse(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Evaluate every rule (all hits are recorded for audit, not just the first). */
    public List<Hit> evaluate(Map<String, Double> features, Map<String, Object> event) {
        Map<String, Object> vars = Map.of("f", features, "event", event);
        List<Hit> hits = new ArrayList<>(2);
        for (Rule r : rules) {
            if (r.when().test(vars)) hits.add(new Hit(r.id(), r.action(), r.reason()));
        }
        return hits;
    }

    /** Whether this payment can be stepped up ({@code challenge.when}; true when the rule set has none). */
    public boolean challengeable(Map<String, Object> event) {
        return challengeable == null || challengeable.test(Map.of("event", event));
    }

    public boolean usesChallenge() {
        if (!Float.isNaN(challengeThreshold)) return true;
        for (Rule r : rules) if (r.action() == Outcome.CHALLENGE) return true;
        return false;
    }

    /** Precedence for a payment that can be stepped up (rule sets without step-up). */
    public Outcome decide(List<Hit> hits, float score) {
        return decide(hits, score, true);
    }

    /**
     * Precedence: DECLINE rule > ALLOW rule > score >= decline > CHALLENGE rule (REVIEW when the payment
     * cannot be stepped up) > score >= challenge (challengeable only) > REVIEW rule > score >= review >
     * APPROVE. {@code score} NaN means no model score (rules-only fallback path).
     */
    public Outcome decide(List<Hit> hits, float score, boolean challengeable) {
        boolean decline = false;
        boolean allow = false;
        boolean review = false;
        boolean challenge = false;
        for (Hit h : hits) {
            switch (h.action()) {
                case DECLINE -> decline = true;
                case APPROVE -> allow = true;
                case REVIEW -> review = true;
                case CHALLENGE -> challenge = true;
                default -> { }
            }
        }
        if (decline) return Outcome.DECLINE;
        if (allow) return Outcome.APPROVE;
        boolean hasScore = !Float.isNaN(score);
        if (hasScore && score >= declineThreshold) return Outcome.DECLINE;
        if (challenge) return challengeable ? Outcome.CHALLENGE : Outcome.REVIEW;
        if (hasScore && challengeable && !Float.isNaN(challengeThreshold) && score >= challengeThreshold) return Outcome.CHALLENGE;
        if (review) return Outcome.REVIEW;
        if (hasScore && score >= reviewThreshold) return Outcome.REVIEW;
        return Outcome.APPROVE;
    }
}
