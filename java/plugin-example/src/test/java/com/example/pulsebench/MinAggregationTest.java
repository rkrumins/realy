package com.example.pulsebench;

import static org.assertj.core.api.Assertions.assertThat;

import io.realy.scoring.features.AggregationConformance;
import io.realy.scoring.features.Aggregations;
import io.realy.scoring.features.FeatureSchema;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/** The plugin is discovered through ServiceLoader and agrees with its Python twin's fixture. */
class MinAggregationTest {
    static Path fixture(String name) {
        Path p = Path.of("").toAbsolutePath();
        while (p != null && !Files.isDirectory(p.resolve("contracts/testdata/aggregations"))) p = p.getParent();
        assertThat(p).as("contracts/testdata/aggregations above the working directory").isNotNull();
        return p.resolve("contracts/testdata/aggregations").resolve(name);
    }

    @Test
    void registeredThroughServiceLoader() {
        assertThat(Aggregations.has("min")).isTrue();
        assertThat(Aggregations.builtin("min")).isFalse();
        assertThat(Aggregations.extensions()).contains("min");
    }

    @Test
    void conformsToThePythonTwin() throws Exception {
        assertThat(AggregationConformance.check(fixture("min.json"))).isEmpty();
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
                """;
        FeatureSchema s = FeatureSchema.parse(yaml);
        assertThat(s.features.get(0).agg()).isEqualTo("min");
    }
}
