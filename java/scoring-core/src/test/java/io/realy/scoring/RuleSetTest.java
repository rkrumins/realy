package io.realy.scoring;

import static org.assertj.core.api.Assertions.assertThat;

import io.realy.contracts.payments.v1.EntryMode;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.contracts.scoring.v1.Outcome;
import io.realy.scoring.features.EventView;
import io.realy.scoring.features.FeatureMap;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.rules.RuleSet;
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
        assertThat(rules.decide(rules.evaluate(new FeatureMap(schema, v), view), 0.6f)).isEqualTo(Outcome.REVIEW);
        assertThat(rules.decide(rules.evaluate(new FeatureMap(schema, v), view), 0.95f)).isEqualTo(Outcome.DECLINE);
        v[schema.indexOf("card_small_cnp_count_10m")] = 3f;
        var hits = rules.evaluate(new FeatureMap(schema, v), view);
        assertThat(hits).extracting(RuleSet.Hit::id).contains("CARD_TESTING");
        assertThat(rules.decide(hits, 0.0f)).isEqualTo(Outcome.DECLINE);
        assertThat(rules.decide(hits, Float.NaN)).isEqualTo(Outcome.DECLINE);
    }
}
