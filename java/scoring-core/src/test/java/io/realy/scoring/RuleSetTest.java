package io.realy.scoring;

import static org.assertj.core.api.Assertions.assertThat;

import io.realy.contracts.payments.v1.EntryMode;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.contracts.scoring.v1.Outcome;
import io.realy.scoring.features.EventView;
import io.realy.scoring.features.FeatureMap;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.rules.RuleSet;
import java.util.List;
import org.junit.jupiter.api.Test;

class RuleSetTest {
    @Test
    void baselineCompilesAndPrecedenceHolds() {
        var schema = FeatureSchema.load();
        var rules = RuleSet.bundled("baseline");
        assertThat(rules.rules).hasSizeGreaterThan(10);
        float[] v = new float[schema.size()];
        var ev = TransactionEvent.newBuilder().setAmountUsdMinor(150).setEntryMode(EntryMode.ECOM_3DS).build();
        var view = EventView.of(ev);
        assertThat(rules.decide(rules.evaluate(new FeatureMap(schema, v), view), 0.1f)).isEqualTo(Outcome.APPROVE);
        // v4: the review band of a payment that can be stepped up (an online card payment) is a CHALLENGE
        assertThat(rules.challengeable(view)).isTrue();
        assertThat(rules.decide(rules.evaluate(new FeatureMap(schema, v), view), 0.6f, true)).isEqualTo(Outcome.CHALLENGE);
        assertThat(rules.decide(rules.evaluate(new FeatureMap(schema, v), view), 0.6f, false)).isEqualTo(Outcome.REVIEW);
        assertThat(rules.decide(rules.evaluate(new FeatureMap(schema, v), view), 0.95f)).isEqualTo(Outcome.DECLINE);
        v[schema.indexOf("card_small_cnp_count_10m")] = 3f;
        var hits = rules.evaluate(new FeatureMap(schema, v), view);
        assertThat(hits).extracting(RuleSet.Hit::id).contains("CARD_TESTING");
        assertThat(rules.decide(hits, 0.0f)).isEqualTo(Outcome.DECLINE);
        assertThat(rules.decide(hits, Float.NaN)).isEqualTo(Outcome.DECLINE);
    }

    private static final String STEP_UP = """
            version: t
            thresholds: {review: 0.5, challenge: 0.6, decline: 0.9}
            challenge:
              when: "!event.recurring && event.entry_mode in ['ECOM_3DS', 'ECOM_NON_3DS', 'CHIP']"
            rules:
              - {id: D, action: DECLINE, when: "f.x >= 3.0"}
              - {id: C, action: CHALLENGE, when: "f.x >= 2.0"}
              - {id: R, action: REVIEW, when: "f.x >= 1.0"}
            """;

    @Test
    void stepUpPrecedenceAndChallengeability() {
        var rules = RuleSet.parse(STEP_UP);
        var ecom = EventView.of(TransactionEvent.newBuilder().setEntryMode(EntryMode.ECOM_3DS).build());
        var cof = EventView.of(TransactionEvent.newBuilder().setEntryMode(EntryMode.ECOM_3DS).setRecurring(true).build());
        var magstripe = EventView.of(TransactionEvent.newBuilder().setEntryMode(EntryMode.MAGSTRIPE).build());
        assertThat(rules.usesChallenge()).isTrue();
        assertThat(rules.challengeable(ecom)).isTrue();
        assertThat(rules.challengeable(cof)).isFalse();          // merchant-initiated: nobody to challenge
        assertThat(rules.challengeable(magstripe)).isFalse();
        var c = List.of(new RuleSet.Hit("C", Outcome.CHALLENGE, ""));
        var r = List.of(new RuleSet.Hit("R", Outcome.REVIEW, ""));
        var dc = List.of(new RuleSet.Hit("D", Outcome.DECLINE, ""), new RuleSet.Hit("C", Outcome.CHALLENGE, ""));
        assertThat(rules.decide(dc, 0.1f, true)).isEqualTo(Outcome.DECLINE);                // DECLINE rules win
        assertThat(rules.decide(c, 0.1f, true)).isEqualTo(Outcome.CHALLENGE);
        assertThat(rules.decide(c, 0.1f, false)).isEqualTo(Outcome.REVIEW);                 // cannot step up: review
        assertThat(rules.decide(c, 0.95f, true)).isEqualTo(Outcome.DECLINE);                // score decline first
        assertThat(rules.decide(List.of(), 0.7f, true)).isEqualTo(Outcome.CHALLENGE);       // challenge band
        assertThat(rules.decide(List.of(), 0.7f, false)).isEqualTo(Outcome.REVIEW);         // band: challengeable only
        assertThat(rules.decide(r, 0.7f, true)).isEqualTo(Outcome.CHALLENGE);               // step-up beats a review rule
        assertThat(rules.decide(r, 0.1f, true)).isEqualTo(Outcome.REVIEW);
        assertThat(rules.decide(List.of(), 0.55f, true)).isEqualTo(Outcome.REVIEW);         // below the band: review
        assertThat(rules.decide(c, Float.NaN, true)).isEqualTo(Outcome.CHALLENGE);          // rules-only fallback
        assertThat(rules.decide(List.of(), Float.NaN, true)).isEqualTo(Outcome.APPROVE);
        // live tuning moves the band; a rule set without step-up has no band
        assertThat(rules.withThresholds(0, 0, 0.8f).decide(List.of(), 0.7f, true)).isEqualTo(Outcome.REVIEW);
        var plain = RuleSet.bundled("baseline-v3");
        assertThat(plain.usesChallenge()).isFalse();
        assertThat(plain.decide(List.of(), 0.7f, true)).isEqualTo(Outcome.REVIEW);
    }
}
