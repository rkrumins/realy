package io.realy.common;

import io.realy.contracts.bench.v1.LatencyDist;

/**
 * Samples latency (microseconds) and failure mode from a {@link LatencyDist}. Pure function of the
 * provided random stream, so a given (seed, event, stage) always yields the same outcome.
 */
public final class LatencyModel {
    public enum Kind { OK, ERROR, TIMEOUT }

    public record Sample(long delayUs, Kind kind) {}

    private static final double Z99 = 2.3263478740408408; // inverse normal CDF at 0.99

    private final LatencyDist dist;
    private final double mu;
    private final double sigma;

    public LatencyModel(LatencyDist dist) {
        this.dist = dist == null ? LatencyDist.getDefaultInstance() : dist;
        double median;
        double p99;
        switch (this.dist.getDistCase()) {
            case LOGNORMAL -> { median = this.dist.getLognormal().getMedianMs(); p99 = this.dist.getLognormal().getP99Ms(); }
            case BIMODAL -> { median = this.dist.getBimodal().getMedianMs(); p99 = this.dist.getBimodal().getP99Ms(); }
            default -> { median = 1; p99 = 1; }
        }
        median = Math.max(median, 1e-6);
        p99 = Math.max(p99, median);
        this.mu = Math.log(median);
        this.sigma = (Math.log(p99) - Math.log(median)) / Z99;
    }

    public static LatencyModel none() {
        return new LatencyModel(LatencyDist.getDefaultInstance());
    }

    public boolean isZero() {
        return dist.getDistCase() == LatencyDist.DistCase.DIST_NOT_SET
                && dist.getErrorRate() == 0 && dist.getTimeoutRate() == 0;
    }

    public Sample sample(SplitMix64 rng) {
        double u = rng.nextDouble();
        Kind kind = Kind.OK;
        if (u < dist.getErrorRate()) {
            kind = Kind.ERROR;
        } else if (u < dist.getErrorRate() + dist.getTimeoutRate()) {
            kind = Kind.TIMEOUT;
        }
        double ms = switch (dist.getDistCase()) {
            case FIXED -> dist.getFixed().getMs();
            case UNIFORM -> dist.getUniform().getMinMs()
                    + rng.nextDouble() * (dist.getUniform().getMaxMs() - dist.getUniform().getMinMs());
            case NORMAL -> {
                var n = dist.getNormal();
                double v = n.getMeanMs() + n.getSdMs() * rng.nextGaussian();
                double lo = n.getMinMs();
                double hi = n.getMaxMs() > 0 ? n.getMaxMs() : Double.MAX_VALUE;
                yield Math.min(hi, Math.max(lo, v));
            }
            case LOGNORMAL -> cap(Math.exp(mu + sigma * rng.nextGaussian()), dist.getLognormal().getMaxMs());
            case BIMODAL -> {
                var b = dist.getBimodal();
                if (rng.nextDouble() < b.getTailProb()) {
                    yield b.getTailMinMs() + rng.nextDouble() * (b.getTailMaxMs() - b.getTailMinMs());
                }
                yield Math.exp(mu + sigma * rng.nextGaussian());
            }
            case EMPIRICAL -> empirical(dist, rng.nextDouble());
            case DIST_NOT_SET -> 0.0;
        };
        return new Sample(Math.max(0L, Math.round(ms * 1000.0)), kind);
    }

    private static double cap(double v, double max) {
        return max > 0 ? Math.min(v, max) : v;
    }

    private static double empirical(LatencyDist d, double u) {
        var e = d.getEmpirical();
        int n = Math.min(e.getQuantilesCount(), e.getMsCount());
        if (n == 0) return 0;
        if (u <= e.getQuantiles(0)) return e.getMs(0);
        for (int i = 1; i < n; i++) {
            double q0 = e.getQuantiles(i - 1);
            double q1 = e.getQuantiles(i);
            if (u <= q1) {
                double t = q1 == q0 ? 0 : (u - q0) / (q1 - q0);
                return e.getMs(i - 1) + t * (e.getMs(i) - e.getMs(i - 1));
            }
        }
        return e.getMs(n - 1);
    }
}
