package io.realy.scoring.features;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * The Java aggregations agree with their Python twins on the shared fixtures
 * (contracts/testdata/aggregations/conformance.json, written by python/realy/features/aggregations.py).
 */
class AggregationConformanceTest {
    static Path fixtures() {
        Path p = Path.of("").toAbsolutePath();
        while (p != null && !Files.isDirectory(p.resolve("contracts/testdata/aggregations"))) p = p.getParent();
        assertThat(p).as("contracts/testdata/aggregations above " + Path.of("").toAbsolutePath()).isNotNull();
        return p.resolve("contracts/testdata/aggregations");
    }

    @Test
    void builtinsMatchThePythonTwins() throws Exception {
        assertThat(AggregationConformance.check(fixtures().resolve("conformance.json"))).isEmpty();
    }

    @Test
    void everyBuiltinIsCoveredByTheFixture() throws Exception {
        String json = Files.readString(fixtures().resolve("conformance.json"));
        for (String n : new String[] {"count", "sum", "avg", "max", "distinct", "novel", "time_since_last", "geo_speed_kmh"}) {
            assertThat(Aggregations.builtin(n)).as(n).isTrue();
            assertThat(json).as(n).contains("\"agg\":\"" + n + "\"");
        }
    }

    @Test
    void unknownAggregationIsRefusedByTheSchema() {
        String yaml = """
                schema_version: 1
                entities: {customer: {key: customer_id}}
                filters: {}
                countries: [US]
                features:
                  - {name: f, source: stream, entity: customer, agg: median, value: amount_usd, window: 1h, granularity: 1m}
                """;
        assertThatThrownBy(() -> FeatureSchema.parse(yaml)).hasMessageContaining("unknown aggregation median");
    }

    @Test
    void numericAggregationNeedsAValue() {
        String yaml = """
                schema_version: 1
                entities: {customer: {key: customer_id}}
                filters: {}
                countries: [US]
                features:
                  - {name: f, source: stream, entity: customer, agg: sum, window: 1h, granularity: 1m}
                """;
        assertThatThrownBy(() -> FeatureSchema.parse(yaml)).hasMessageContaining("needs `value:`");
    }

    @Test
    void aNameIsRegisteredOnce() {
        assertThatThrownBy(() -> Aggregations.register(new Aggregations.BucketAgg("sum", Aggregation.Input.NUMERIC, Double::sum, true)))
                .hasMessageContaining("already registered");
    }
}
