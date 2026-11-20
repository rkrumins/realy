import { describe, expect, it } from "vitest";
import { heat, steadyHeat, type Heat } from "@/lib/topology";
import { fmtStartupProgress } from "@/components/startup";

describe("topology heat", () => {
  it("judges online-store freshness against its own SLO, not the e2e budget", () => {
    // 400 ms of freshness used to be 'hot' (>50% of 150 ms); against a 1 s SLO it is a warning
    expect(heat(400, 150, "store_update_lag")).toBe("hot");
    expect(heat(400, 150, "store_update_lag", 1000)).toBe("warn");
    expect(heat(120, 150, "store_update_lag", 1000)).toBe("good");
    expect(heat(1500, 150, "store_update_lag", 1000)).toBe("hot");
  });

  it("only escalates after three consecutive samples agree, de-escalates at once", () => {
    const h: Heat[] = [];
    expect(steadyHeat(h, "hot")).toBe("good");
    expect(steadyHeat(h, "hot")).toBe("good");
    expect(steadyHeat(h, "hot")).toBe("hot");
    expect(steadyHeat(h, "good")).toBe("good");
    expect(steadyHeat(h, "hot")).toBe("good");     // one slow second after recovering: not red
    const idle: Heat[] = [];
    expect(steadyHeat(idle, "idle")).toBe("idle");
  });
});

describe("startup progress", () => {
  it("formats like the runner's log lines", () => {
    expect(fmtStartupProgress({ done: 412330, total: 583063, unit: "events", rate: 9800, eta_s: 17.4 }))
      .toBe("71% (412,330/583,063 events) · 9,800/s · ETA 17 s");
    expect(fmtStartupProgress(null)).toBe("");
  });
});
