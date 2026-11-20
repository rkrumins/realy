import { describe, expect, it } from "vitest";
import { fmtProgress } from "@/components/steps";
import { fmtCompact, fmtDuration, fmtMs, fmtPct } from "@/lib/format";
import { level, steadyLevel, steadyLevelOf, type Level } from "@/lib/state";

describe("level against a budget", () => {
  it("is idle without data, then good / warn / hot by budget share", () => {
    expect(level(null, 100)).toBe("idle");
    expect(level(50, 100)).toBe("good");
    expect(level(85, 100)).toBe("warn");
    expect(level(120, 100)).toBe("hot");
  });
});

describe("hysteresis", () => {
  it("only escalates after three consecutive samples agree, de-escalates at once", () => {
    const h: Level[] = [];
    expect(steadyLevel(h, "hot")).toBe("good");
    expect(steadyLevel(h, "hot")).toBe("good");
    expect(steadyLevel(h, "hot")).toBe("hot");
    expect(steadyLevel(h, "good")).toBe("good");
    expect(steadyLevel(h, "hot")).toBe("good"); // one slow second after recovering: not red
    expect(steadyLevel([], "idle")).toBe("idle");
  });

  it("works over a backfilled series too (a page opened mid-incident is red at once)", () => {
    expect(steadyLevelOf([50, 260, 250, 270], 120)).toBe("hot");
    expect(steadyLevelOf([260, 250, 60], 120)).toBe("good"); // recovered: de-escalate at once
    expect(steadyLevelOf([260], 120)).toBe("good"); // unconfirmed escalation
    expect(steadyLevelOf([null, null], 120)).toBe("idle");
    expect(steadyLevelOf([260, 250, 270], null)).toBe("idle");
  });
});

describe("formatting", () => {
  it("is unit-aware and never prints NaN", () => {
    expect(fmtMs(4.2)).toBe("4.20 ms");
    expect(fmtMs(1530)).toBe("1.53 s");
    expect(fmtMs(undefined)).toBe("—");
    expect(fmtMs(0)).toBe("0 ms");
    expect(fmtCompact(12_345)).toBe("12.3k");
    expect(fmtPct(0.987, 1)).toBe("98.7%");
    expect(fmtDuration(3725)).toBe("1h 2m");
  });

  it("formats step progress like a log line", () => {
    expect(fmtProgress({ done: 412330, total: 583063, unit: "items", rate: 9800, eta_s: 17.4 }))
      .toBe("71% (412,330/583,063 items) · 9,800/s · ETA 17 s");
    expect(fmtProgress(null)).toBe("");
  });
});
