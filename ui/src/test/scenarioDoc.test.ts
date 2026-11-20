import { describe, expect, it } from "vitest";
import { getIn, modelCapacity, parseYaml, setIn, toYaml } from "@/lib/scenarioDoc";
import { heat, resolveP99, resolveRate } from "@/lib/topology";
import { fmtMs, fmtCompact, runStamp } from "@/lib/format";

describe("scenario doc", () => {
  it("sets nested values immutably", () => {
    const a = { traffic: { profile: { tps: 100 } } };
    const b = setIn(a, ["traffic", "profile", "tps"], 200);
    expect(getIn(b, ["traffic", "profile", "tps"])).toBe(200);
    expect(a.traffic.profile.tps).toBe(100);
    const c = setIn(b, ["world", "customers"], 5);
    expect(getIn(c, ["world", "customers"])).toBe(5);
  });
  it("deletes and prunes empty parents", () => {
    const a = { name: "x", budget: { e2e_p99_ms: 150 } };
    const b = setIn(a, ["budget", "e2e_p99_ms"], undefined);
    expect("budget" in b).toBe(false);
    expect(b.name).toBe("x");
  });
  it("round-trips YAML with key order", () => {
    const y = toYaml({ topology: "gateway-kafka", name: "t", traffic: { duration_s: 60 } });
    expect(y.indexOf("name")).toBeLessThan(y.indexOf("traffic"));
    const { doc, error } = parseYaml(y);
    expect(error).toBeNull();
    expect(doc).toEqual({ name: "t", traffic: { duration_s: 60 }, topology: "gateway-kafka" });
  });
  it("reports parse errors", () => {
    expect(parseYaml("a: [1, 2").error).toBeTruthy();
    expect(parseYaml("- 1\n- 2").error).toMatch(/mapping/);
  });
  it("estimates model capacity", () => {
    expect(modelCapacity(4, 8, 10)).toBeCloseTo(3200);
    expect(modelCapacity(0, 8, 10)).toBeNull();
  });
});

describe("topology metrics", () => {
  const snap = (source: string, counters: Record<string, number>, stages: { name: string; count: number; p99_ms: number }[] = [], window_ms = 1000) => ({
    run_id: "r", source, instance: `${source}-1`, ts_us: 1, window_ms, counters,
    stages: stages.map((s) => ({ ...s, p50_ms: 0, p90_ms: 0, p999_ms: 0, max_ms: 0 })),
  });
  it("sums rates across instances and normalises per window", () => {
    const snaps = [snap("emitter", { emitted: 500 }), { ...snap("emitter", { emitted: 250 }, [], 500), instance: "e2" }];
    expect(resolveRate({ source: "emitter", counter: "emitted" }, snaps)).toBe(1000);
    expect(resolveRate({ source: "model", counter: "requests" }, snaps)).toBeNull();
  });
  it("takes the worst p99 and grades heat by budget share", () => {
    const snaps = [snap("gateway", {}, [{ name: "hot_path", count: 10, p99_ms: 40 }]), { ...snap("gateway", {}, [{ name: "hot_path", count: 10, p99_ms: 90 }]), instance: "g2" }];
    expect(resolveP99({ source: "gateway", stage: "hot_path" }, snaps)).toBe(90);
    expect(heat(90, 100, "hot_path")).toBe("warn");
    expect(heat(90, 100, "feature_lookup")).toBe("hot");
    expect(heat(10, 100, "feature_lookup")).toBe("good");
    expect(heat(null, 100)).toBe("idle");
  });
});

describe("format", () => {
  it("formats latencies and counts", () => {
    expect(fmtMs(1.234)).toBe("1.23 ms");
    expect(fmtMs(150)).toBe("150 ms");
    expect(fmtMs(12_500)).toBe("12.5 s");
    expect(fmtCompact(25_000)).toBe("25.0k");
    expect(runStamp("smoke-20261119-075353")).toBe("19/11 07:53");
  });
});
