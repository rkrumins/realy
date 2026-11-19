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

/** A versioned CEL rule set (rules/*.yaml) plus score thresholds. Immutable; hot-swapped as a whole. */
public final class RuleSet {
    public record Rule(String id, Outcome action, String reason, Cel.Predicate when) {}

    public record Hit(String id, Outcome action, String reason) {}

    public final String version;
    public final float reviewThreshold;
    public final float declineThreshold;
    public final List<Rule> rules;

    private RuleSet(String version, float review, float decline, List<Rule> rules) {
        this.version = version;
        this.reviewThreshold = review;
        this.declineThreshold = decline;
        this.rules = List.copyOf(rules);
    }

    public static RuleSet parse(String yaml) {
        try {
            JsonNode root = ProtoJson.yamlMapper().readTree(yaml);
            String version = root.path("version").asText("unversioned");
            float review = (float) root.path("thresholds").path("review").asDouble(0.5);
            float decline = (float) root.path("thresholds").path("decline").asDouble(0.9);
            List<Rule> rules = new ArrayList<>();
            for (JsonNode r : root.path("rules")) {
                String action = r.path("action").asText("REVIEW");
                Outcome o = switch (action) {
                    case "DECLINE" -> Outcome.DECLINE;
                    case "REVIEW" -> Outcome.REVIEW;
                    case "ALLOW", "APPROVE" -> Outcome.APPROVE;
                    default -> throw new IllegalArgumentException("unknown action " + action);
                };
                rules.add(new Rule(r.get("id").asText(), o, r.path("reason").asText(""), Cel.compile(r.get("when").asText())));
            }
            return new RuleSet(version, review, decline, rules);
        } catch (IOException e) {
            throw new IllegalArgumentException("invalid rule set: " + e.getMessage(), e);
        }
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

    /**
     * Precedence: DECLINE rule > ALLOW rule > score >= decline > REVIEW rule > score >= review > APPROVE.
     * {@code score} NaN means no model score (rules-only fallback path).
     */
    public Outcome decide(List<Hit> hits, float score) {
        boolean decline = false;
        boolean allow = false;
        boolean review = false;
        for (Hit h : hits) {
            switch (h.action()) {
                case DECLINE -> decline = true;
                case APPROVE -> allow = true;
                case REVIEW -> review = true;
                default -> { }
            }
        }
        if (decline) return Outcome.DECLINE;
        if (allow) return Outcome.APPROVE;
        boolean hasScore = !Float.isNaN(score);
        if (hasScore && score >= declineThreshold) return Outcome.DECLINE;
        if (review) return Outcome.REVIEW;
        if (hasScore && score >= reviewThreshold) return Outcome.REVIEW;
        return Outcome.APPROVE;
    }
}
