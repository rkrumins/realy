/**
 * A run's lifecycle as segments: prepare, warm-up, measure, drain, report, verify. Built from the
 * server's timeline (runner/timeline.py) and ETA (report/eta.py); finished segments show their real
 * duration, the active one its elapsed time plus the estimate, later ones their estimate. The scenario's
 * `duration_s` is the whole traffic window: warm-up is inside it, never added to it.
 */
import { useEffect, useRef, useState } from "react";
import type { RunEta, RunTimeline } from "./types";

export type StageId = "prepare" | "warmup" | "measure" | "drain" | "report" | "verify";
export const STAGE_LABEL: Record<StageId | "done" | "queued", string> = {
  queued: "Queued", prepare: "Preparing", warmup: "Warm-up", measure: "Measuring", drain: "Draining",
  report: "Building the report", verify: "Verifying", done: "Finished",
};
const ORDER: StageId[] = ["prepare", "warmup", "measure", "drain", "report", "verify"];
// ETA keys that are not startup steps: everything else belongs to the preparation
const NOT_PREPARE = new Set(["warmup", "measure", "drain", "report", "verify"]);

export interface Segment {
  id: StageId;
  label: string;
  /** seconds spent so far (the whole segment once it is done) */
  elapsed_s: number;
  /** estimated seconds still to come (0 once done) */
  remaining_s: number;
  state: "done" | "active" | "pending" | "skipped";
}

function remainingOf(id: StageId, tl: RunTimeline, eta: RunEta | null | undefined): number {
  const per = eta?.per_step ?? {};
  if (id === "prepare") {
    return Object.entries(per).filter(([k]) => !NOT_PREPARE.has(k)).reduce((a, [, v]) => a + v, 0);
  }
  if (per[id] !== undefined) return per[id];
  if (!eta) {   // no estimate (a replay): the traffic window is still known exactly
    if (id === "warmup") return tl.configured.warmup_s;
    if (id === "measure") return tl.configured.measure_s;
  }
  return 0;
}

/** The lifecycle at `now` (the server's clock): one segment per stage. */
export function segments(tl: RunTimeline, eta: RunEta | null | undefined, now: number, verify = false): Segment[] {
  const bounds: Record<StageId, [number | null, number | null]> = {
    prepare: [tl.launched_at, tl.traffic_start],
    warmup: [tl.traffic_start, tl.measure_start],
    measure: [tl.measure_start, tl.emission_end],
    drain: [tl.emission_end, tl.drain_end],
    report: [tl.drain_end, tl.report_end],
    verify: [tl.verify_start, tl.verify_end],
  };
  const done: Record<StageId, number | null> = {
    prepare: tl.durations.prepare_s, warmup: tl.durations.warmup_s, measure: tl.durations.measured_s,
    drain: tl.durations.drain_s, report: tl.durations.report_s, verify: tl.durations.verify_s,
  };
  const active = tl.live ? (tl.stage as StageId) : null;
  const ai = active ? ORDER.indexOf(active) : ORDER.length;
  const show = ORDER.filter((id) => id !== "verify" || verify || tl.verify_start !== null || tl.stage === "verify");
  return show.map((id) => {
    const i = ORDER.indexOf(id);
    const [a, b] = bounds[id];
    const label = STAGE_LABEL[id];
    if (i < ai || (!tl.live && a !== null)) {
      const el = a !== null && b !== null ? b - a : done[id];
      return { id, label, elapsed_s: Math.max(0, el ?? 0), remaining_s: 0, state: el === null ? "skipped" : "done" };
    }
    if (i === ai) {
      const start = a ?? (id === "report" ? tl.emission_end : null);
      const el = start !== null ? Math.max(0, now - start) : Math.max(0, done[id] ?? 0);
      return { id, label, elapsed_s: el, remaining_s: remainingOf(id, tl, eta), state: "active" };
    }
    if (!tl.live) return { id, label, elapsed_s: 0, remaining_s: 0, state: "skipped" };
    return { id, label, elapsed_s: 0, remaining_s: remainingOf(id, tl, eta), state: "pending" };
  });
}

export function totals(segs: Segment[]): { elapsed_s: number; remaining_s: number } {
  return segs.reduce((a, s) => ({ elapsed_s: a.elapsed_s + s.elapsed_s, remaining_s: a.remaining_s + s.remaining_s }),
    { elapsed_s: 0, remaining_s: 0 });
}

/** The server's clock, ticking every second: `serverNow` is when the server sent its snapshot. */
export function useServerNow(serverNow: number | null | undefined, frozen?: number | null): number {
  const offset = useRef(0);
  const [now, setNow] = useState(() => Date.now() / 1000);
  useEffect(() => {
    if (serverNow) offset.current = serverNow - Date.now() / 1000;
  }, [serverNow]);
  useEffect(() => {
    if (frozen !== undefined && frozen !== null) return;
    const t = setInterval(() => setNow(Date.now() / 1000), 1000);
    return () => clearInterval(t);
  }, [frozen]);
  return frozen !== undefined && frozen !== null ? frozen : now + offset.current;
}
