import { describe, expect, it } from "vitest";
import { defaultDist, empiricalAt, preview, sampleOne, rng } from "../lib/dist";
import { rateAt, rateCurve } from "../lib/rate";

describe("latency distributions (mirror of java LatencyModel)", () => {
  it("lognormal hits its median and p99", () => {
    const p = preview({ lognormal: { median_ms: 5, p99_ms: 40 } }, 100_000)!;
    expect(p.p50).toBeGreaterThan(4.8);
    expect(p.p50).toBeLessThan(5.2);
    expect(p.p99).toBeGreaterThan(37);
    expect(p.p99).toBeLessThan(43);
  });
  it("uniform stays in bounds and error rate is honoured", () => {
    const r = rng(1);
    let err = 0;
    for (let i = 0; i < 20000; i++) {
      const v = sampleOne({ uniform: { min_ms: 30, max_ms: 100 }, error_rate: 0.1 }, r);
      if (Number.isNaN(v)) err++;
      else {
        expect(v).toBeGreaterThanOrEqual(30);
        expect(v).toBeLessThanOrEqual(100);
      }
    }
    expect(err / 20000).toBeGreaterThan(0.09);
    expect(err / 20000).toBeLessThan(0.11);
  });
  it("bimodal tail shows up above p99", () => {
    const p = preview({ bimodal: { median_ms: 10, p99_ms: 30, tail_prob: 0.02, tail_min_ms: 150, tail_max_ms: 400 } }, 50_000)!;
    expect(p.p99).toBeGreaterThan(100);
    expect(p.p50).toBeLessThan(12);
  });
  it("empirical interpolates the inverse CDF", () => {
    expect(empiricalAt([0, 0.5, 1], [0, 10, 20], 0.25)).toBeCloseTo(5);
    expect(empiricalAt([0, 0.5, 1], [0, 10, 20], 0.75)).toBeCloseTo(15);
  });
  it("fixed and defaults", () => {
    expect(preview({ fixed: { ms: 12 } })!.p99).toBe(12);
    expect(defaultDist("none")).toBeNull();
    expect(defaultDist("lognormal")).toEqual({ lognormal: { median_ms: 8, p99_ms: 25 } });
  });
});

describe("rate profiles (mirror of python RateProfile.rate_at)", () => {
  it("step", () => {
    const p = { kind: "step" as const, start_tps: 1000, step_tps: 1000, step_duration_s: 30, tps: 1000 };
    expect(rateAt(p, 0, 300)).toBe(1000);
    expect(rateAt(p, 29.9, 300)).toBe(1000);
    expect(rateAt(p, 30, 300)).toBe(2000);
    expect(rateAt(p, 299, 300)).toBe(10000);
  });
  it("ramp, spike, sine, constant", () => {
    expect(rateAt({ kind: "ramp", start_tps: 0, end_tps: 100 }, 50, 100)).toBe(50);
    expect(rateAt({ kind: "spike", tps: 400, spike_at_s: 60, spike_duration_s: 60, spike_multiplier: 3 }, 61, 180)).toBe(1200);
    expect(rateAt({ kind: "spike", tps: 400, spike_at_s: 60, spike_duration_s: 60, spike_multiplier: 3 }, 121, 180)).toBe(400);
    expect(rateAt({ kind: "sine", tps: 100, sine_period_s: 100, sine_amplitude: 0.5 }, 25, 100)).toBeCloseTo(150);
    expect(rateAt({ tps: 500 }, 10, 60)).toBe(500);
  });
  it("curve totals", () => {
    const c = rateCurve({ tps: 500 }, 60);
    expect(c.events).toBeCloseTo(30000, -2);
    expect(c.peak).toBe(500);
  });
});
