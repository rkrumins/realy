package io.realy.common;

/**
 * Tiny, fast, deterministic PRNG. Used to derive per-(event, stage) random streams so injected
 * delays and errors are reproducible run to run: {@code SplitMix64.of(seed, eventId, salt)}.
 * The same algorithm is implemented in python/realy/common/rng.py.
 */
public final class SplitMix64 {
    private long state;

    public SplitMix64(long seed) {
        this.state = seed;
    }

    public static SplitMix64 of(long seed, long eventId, long salt) {
        return new SplitMix64(mix(seed ^ mix(eventId + 0x9E3779B97F4A7C15L * (salt + 1))));
    }

    public static long mix(long z) {
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    public long nextLong() {
        state += 0x9E3779B97F4A7C15L;
        return mix(state);
    }

    /** Uniform double in [0, 1) with 53 bits of randomness. */
    public double nextDouble() {
        return (nextLong() >>> 11) * 0x1.0p-53;
    }

    /** Standard normal via Box-Muller (one value per call; deterministic). */
    public double nextGaussian() {
        double u1 = nextDouble();
        double u2 = nextDouble();
        if (u1 < 1e-300) u1 = 1e-300;
        return Math.sqrt(-2.0 * Math.log(u1)) * Math.cos(2.0 * Math.PI * u2);
    }
}
