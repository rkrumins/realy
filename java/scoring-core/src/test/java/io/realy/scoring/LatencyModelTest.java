package io.realy.scoring;

import static org.assertj.core.api.Assertions.assertThat;

import io.realy.common.LatencyModel;
import io.realy.common.SplitMix64;
import io.realy.contracts.bench.v1.LatencyDist;
import io.realy.contracts.bench.v1.LogNormal;
import io.realy.contracts.bench.v1.Uniform;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class LatencyModelTest {
    @Test
    void lognormalHitsMedianAndP99() {
        var m = new LatencyModel(LatencyDist.newBuilder().setLognormal(LogNormal.newBuilder().setMedianMs(5).setP99Ms(40)).build());
        long[] v = new long[200_000];
        for (int i = 0; i < v.length; i++) v[i] = m.sample(SplitMix64.of(7, i, 1)).delayUs();
        Arrays.sort(v);
        assertThat(v[v.length / 2] / 1000.0).isBetween(4.8, 5.2);
        assertThat(v[(int) (v.length * 0.99)] / 1000.0).isBetween(37.0, 43.0);
    }

    @Test
    void deterministicPerEventAndUniformBounds() {
        var m = new LatencyModel(LatencyDist.newBuilder().setUniform(Uniform.newBuilder().setMinMs(30).setMaxMs(100))
                .setErrorRate(0.1).build());
        int errors = 0;
        for (int i = 0; i < 10_000; i++) {
            var a = m.sample(SplitMix64.of(1, i, 2));
            var b = m.sample(SplitMix64.of(1, i, 2));
            assertThat(a).isEqualTo(b);
            assertThat(a.delayUs()).isBetween(30_000L, 100_000L);
            if (a.kind() == LatencyModel.Kind.ERROR) errors++;
        }
        assertThat(errors).isBetween(850, 1150);
    }
}
