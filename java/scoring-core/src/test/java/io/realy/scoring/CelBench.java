package io.realy.scoring;

import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.scoring.features.EventView;
import io.realy.scoring.features.FeatureMap;
import io.realy.scoring.features.FeatureSchema;
import io.realy.scoring.rules.Filters;
import io.realy.scoring.rules.RuleSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Hot-path micro-benchmark: CEL filters + rules per event (run with -Dbench=true). */
class CelBench {
    @Test
    @Tag("bench")
    void celCostPerEvent() {
        if (!Boolean.getBoolean("bench")) return;
        var schema = FeatureSchema.load();
        var filters = new Filters(schema);
        var rules = RuleSet.bundled("baseline");
        var e = TransactionEvent.newBuilder().setAmountUsdMinor(1500).setCardId(3).setMerchantCountry("GB").build();
        float[] v = new float[schema.size()];
        long t0 = 0;
        int n = 200_000;
        for (int round = 0; round < 2; round++) {
            t0 = System.nanoTime();
            for (int i = 0; i < n; i++) {
                var view = EventView.of(e);
                filters.mask(view);
                rules.evaluate(new FeatureMap(schema, v), view);
            }
        }
        System.out.printf("CEL per event (8 filters + %d rules): %.2f us%n", rules.rules.size(), (System.nanoTime() - t0) / 1e3 / n);
    }
}
