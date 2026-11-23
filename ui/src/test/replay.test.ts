import { describe, expect, it } from "vitest";
import { ghostSeries, replayRun, startupAt } from "@/components/replay";
import { LiveStore, type Telemetry } from "@/lib/live";

const T0 = 1_900_000_000;
const snap = (src: string, sec: number, counters: Record<string, number>, stages: { name: string; p99_ms: number }[] = []) => ({
  source: src, instance: src === "collector" ? "c0" : "e0", run_id: "r", ts_us: (T0 + sec) * 1e6, window_ms: 1000, counters,
  stages: stages.map((s) => ({ name: s.name, count: 1, p50_ms: s.p99_ms / 2, p90_ms: s.p99_ms, p99_ms: s.p99_ms, p999_ms: s.p99_ms, max_ms: s.p99_ms })),
});

function telemetry(offset = 0, p99 = 20): Telemetry {
  const metrics: Record<string, ReturnType<typeof snap>[]> = { "emitter:e0": [], "collector:c0": [] };
  for (let s = 20; s < 50; s++) {
    metrics["emitter:e0"].push(snap("emitter", s + offset, { emitted: 100, emitted_total: (s - 19) * 100 }));
    metrics["collector:c0"].push(snap("collector", s + offset, { decided: 100, decided_total: (s - 19) * 100 }, [{ name: "e2e", p99_ms: p99 }]));
  }
  return {
    run: { run_id: "r", scenario: "x", topology: "gateway-kafka", budget: { e2e_p99_ms: 150 }, base_tps: 100, warmup_s: 5, duration_s: 30,
      started_at: T0 + offset, measure_started_at: T0 + offset + 20, ended_at: T0 + offset + 50, verdict: "PASS", complete: true,
      first_us: (T0 + offset + 20) * 1e6, last_us: (T0 + offset + 49) * 1e6 },
    metrics: metrics as unknown as Telemetry["metrics"], logs: [{ run_id: "r", line: "hello", ts: T0 + offset + 1 }],
    startup: { started_at: T0 + offset, steps: [
      { id: "world", label: "World loaded", index: 1, status: "done", detail: "", started_at: T0 + offset, ended_at: T0 + offset + 2 },
      { id: "history", label: "History replay", index: 8, status: "done", detail: "", started_at: T0 + offset + 2, ended_at: T0 + offset + 15 },
      { id: "measure", label: "Measure", index: 13, status: "done", detail: "", started_at: T0 + offset + 25, ended_at: T0 + offset + 50 },
    ] },
    markers: [{ t: T0 + offset, kind: "phase", label: "prepare" }, { t: T0 + offset + 20, kind: "phase", label: "warmup" },
      { t: T0 + offset + 25, kind: "phase", label: "measure" }],
    decisions: [],
  };
}

describe("replay", () => {
  it("reads the run as of its clock", () => {
    const st = new LiveStore({ replay: true });
    st.load(telemetry());
    expect(st.clock).toBe(T0 + 20);                     // starts at the start of traffic
    st.setClock(T0 + 10);
    expect(replayRun(st.telemetry!, st.clock).phase).toBe("PREPARE");
    const steps = startupAt(st.telemetry!.startup, st.clock)!.steps;
    expect(steps.map((s) => s.status)).toEqual(["done", "running", "pending"]);
    st.setClock(T0 + 30);
    expect(replayRun(st.telemetry!, st.clock).phase).toBe("MEASURE");
  });

  it("aligns a previous run on the start of traffic", () => {
    const prev = new LiveStore({ replay: true });
    prev.load(telemetry(-3600, 90));                     // an hour earlier, slower
    const g = ghostSeries(prev, T0 - 3600 + 20, T0 + 20);
    expect(g.size).toBe(30);                             // the whole run, whatever the ghost's own cursor
    expect(g.get(T0 + 30)?.e2e_p99).toBe(90);
    expect(prev.clock).toBe(T0 - 3600 + 20);             // its cursor is left as it was
  });
});
