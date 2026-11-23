import { describe, expect, it } from "vitest";
import { segments, totals } from "@/lib/timeline";
import type { RunEta, RunTimeline } from "@/lib/types";

const T0 = 1_800_000_000;

function tl(over: Partial<RunTimeline> = {}): RunTimeline {
  return {
    stage: "done", live: false, aborted: false, launched_at: T0, traffic_start: T0 + 100, measure_start: T0 + 110,
    emission_end: T0 + 160, drain_end: T0 + 165, report_end: T0 + 168, verify_start: null, verify_end: null,
    finished_at: T0 + 168, configured: { duration_s: 60, warmup_s: 10, measure_s: 50 },
    durations: { prepare_s: 100, warmup_s: 10, measured_s: 50, drain_s: 5, report_s: 3, verify_s: null, total_s: 168 },
    ...over,
  };
}

const by = (xs: ReturnType<typeof segments>) => Object.fromEntries(xs.map((s) => [s.id, s]));

describe("run lifecycle segments", () => {
  it("a finished run: every stage at its real duration, warm-up inside the traffic window", () => {
    const s = segments(tl(), null, T0 + 10_000);
    expect(s.map((x) => x.id)).toEqual(["prepare", "warmup", "measure", "drain", "report"]);
    expect(by(s).prepare.elapsed_s).toBe(100);
    expect(by(s).warmup.elapsed_s + by(s).measure.elapsed_s).toBe(60);
    expect(totals(s)).toEqual({ elapsed_s: 168, remaining_s: 0 });
    expect(s.every((x) => x.state === "done")).toBe(true);
  });

  it("preparing: elapsed so far plus the estimate of every step still to come", () => {
    const eta: RunEta = { remaining_s: 0, finish_at: 0, basis: "3 earlier smoke run(s)", late: [],
      per_step: { history: 30, catchup: 20, warmup: 10, measure: 50, drain: 5, report: 3, verify: 40 } };
    const t = tl({ stage: "prepare", live: true, traffic_start: null, measure_start: null, emission_end: null, drain_end: null,
      report_end: null, finished_at: null });
    const s = by(segments(t, eta, T0 + 70, true));
    expect(s.prepare).toMatchObject({ state: "active", elapsed_s: 70, remaining_s: 50 });
    expect(s.warmup).toMatchObject({ state: "pending", remaining_s: 10 });
    expect(s.measure.remaining_s).toBe(50);
    expect(s.verify).toMatchObject({ state: "pending", remaining_s: 40 });
    expect(totals(Object.values(s)).remaining_s).toBe(50 + 10 + 50 + 5 + 3 + 40);
  });

  it("measuring: warm-up done, the measured window counts down exactly", () => {
    const eta: RunEta = { remaining_s: 0, finish_at: 0, basis: "", late: [], per_step: { measure: 20, drain: 5, report: 3 } };
    const t = tl({ stage: "measure", live: true, emission_end: null, drain_end: null, report_end: null, finished_at: null });
    const s = by(segments(t, eta, T0 + 140));
    expect(s.prepare.state).toBe("done");
    expect(s.warmup).toMatchObject({ state: "done", elapsed_s: 10 });
    expect(s.measure).toMatchObject({ state: "active", elapsed_s: 30, remaining_s: 20 });
    expect(s.verify).toBeUndefined();          // not requested
  });

  it("aborted while preparing: the traffic stages are skipped, not shown as zero-length progress", () => {
    const t = tl({ aborted: true, traffic_start: null, measure_start: null, emission_end: null, drain_end: null, report_end: null,
      finished_at: T0 + 83, durations: { prepare_s: 80, warmup_s: null, measured_s: null, drain_s: null, report_s: null, verify_s: null, total_s: 83 } });
    const s = by(segments(t, null, T0 + 10_000));
    expect(s.prepare).toMatchObject({ state: "done", elapsed_s: 80 });
    expect(s.warmup.state).toBe("skipped");
    expect(s.measure.state).toBe("skipped");
  });

  it("verifying: the run is over but not finished", () => {
    const t = tl({ stage: "verify", live: true, verify_start: T0 + 170, finished_at: null });
    const s = by(segments(t, { remaining_s: 0, finish_at: 0, basis: "", late: [], per_step: { verify: 25 } }, T0 + 185, true));
    expect(s.report.state).toBe("done");
    expect(s.verify).toMatchObject({ state: "active", elapsed_s: 15, remaining_s: 25 });
  });
});
