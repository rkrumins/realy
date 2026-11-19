/** Mirror of python/realy/scenario.py RateProfile.rate_at (wall-clock target TPS at time t). */
export interface RateProfile {
  kind?: "constant" | "ramp" | "step" | "spike" | "sine";
  tps?: number;
  start_tps?: number | null;
  end_tps?: number | null;
  step_tps?: number | null;
  step_duration_s?: number | null;
  spike_at_s?: number | null;
  spike_duration_s?: number | null;
  spike_multiplier?: number;
  sine_period_s?: number;
  sine_amplitude?: number;
}

const nn = (v: number | null | undefined) => v !== null && v !== undefined;

export function rateAt(p: RateProfile, t: number, durationS: number): number {
  const tps = p.tps ?? 1000;
  switch (p.kind ?? "constant") {
    case "constant":
      return tps;
    case "ramp": {
      const s = nn(p.start_tps) ? (p.start_tps as number) : 0;
      const e = nn(p.end_tps) ? (p.end_tps as number) : tps;
      return s + (e - s) * Math.min(1, Math.max(0, t / Math.max(durationS, 1e-9)));
    }
    case "step": {
      const s = nn(p.start_tps) ? (p.start_tps as number) : tps;
      const inc = nn(p.step_tps) ? (p.step_tps as number) : s;
      const d = p.step_duration_s || 60;
      return s + inc * Math.floor(t / d);
    }
    case "spike": {
      const at = p.spike_at_s || 0;
      const dur = p.spike_duration_s || 30;
      return tps * (at <= t && t < at + dur ? (p.spike_multiplier ?? 3) : 1);
    }
    case "sine":
      return tps * (1 + (p.sine_amplitude ?? 0.3) * Math.sin((2 * Math.PI * t) / (p.sine_period_s ?? 600)));
    default:
      return tps;
  }
}

/** Curve for a preview chart plus the totals the builder shows. */
export function rateCurve(p: RateProfile, durationS: number, points = 240) {
  const out: { t: number; tps: number }[] = [];
  let events = 0;
  const dt = durationS / points;
  for (let i = 0; i <= points; i++) {
    const t = Math.min(durationS, i * dt);
    // step changes are discontinuous: sample just before/after for a crisp staircase
    out.push({ t: Math.round(t * 10) / 10, tps: rateAt(p, t, durationS) });
    if (i < points) events += rateAt(p, t + dt / 2, durationS) * dt;
  }
  const peak = Math.max(...out.map((o) => o.tps));
  return { curve: out, events, peak };
}
