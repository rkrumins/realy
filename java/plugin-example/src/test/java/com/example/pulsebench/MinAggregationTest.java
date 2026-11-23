package com.example.pulsebench;

import static org.assertj.core.api.Assertions.assertThat;

import io.realy.scoring.features.AggregationConformance;
import io.realy.scoring.features.Aggregations;
import io.realy.scoring.features.DerivedFnConformance;
import io.realy.scoring.features.DerivedFns;
import io.realy.scoring.features.FeatureSchema;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/** The plugins are discovered through ServiceLoader and agree with their Python twins' fixtures. */
class MinAggregationTest {
    static Path fixture(String name) {
        Path p = Path.of("").toAbsolutePath();
        while (p != null && !Files.isDirectory(p.resolve("contracts/testdata/aggregations"))) p = p.getParent();
        assertThat(p).as("contracts/testdata above the working directory").isNotNull();
        return p.resolve("contracts/testdata").resolve(name);
    }

    @Test
    void registeredThroughServiceLoader() {
        assertThat(Aggregations.has("min")).isTrue();
        assertThat(Aggregations.builtin("min")).isFalse();
        assertThat(Aggregations.extensions()).contains("min");
    }

    @Test
    void conformsToThePythonTwin() throws Exception {
        assertThat(AggregationConformance.check(fixture("aggregations/min.json"))).isEmpty();
    }

    @Test
    void derivedFunctionRegisteredAndConformant() throws Exception {
        assertThat(DerivedFns.has("log_amount")).isTrue();
        assertThat(DerivedFns.extensions()).contains("log_amount");
        assertThat(DerivedFnConformance.check(fixture("derived/log_amount.json"))).isEmpty();
    }

    @Test
    void usableInTheFeatureSchema() {
        String yaml = """
                schema_version: 1
                entities: {customer: {key: customer_id}}
                filters: {}
                countries: [US]
                features:
                  - {name: cust_min_amount_1h, source: stream, entity: customer, agg: min, value: amount_usd,
                     window: 1h, granularity: 1m, default: 0}
                  - {name: log_amount, source: derived, fn: log_amount}
                """;
        FeatureSchema s = FeatureSchema.parse(yaml);
        assertThat(s.features.get(0).agg()).isEqualTo("min");
        assertThat(s.features.get(1).fn()).isEqualTo("log_amount");
    }
}
