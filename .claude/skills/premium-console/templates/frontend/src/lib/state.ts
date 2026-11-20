/** Judging live values against a budget, with hysteresis so one bad second does not paint a tile red. */

export type Level = "idle" | "good" | "warn" | "hot";
const SEVERITY: Record<Level, number> = { idle: 0, good: 1, warn: 2, hot: 3 };

/** A value against its budget: good below `warnAt` x budget, warn below `hotAt` x budget, hot beyond. */
export function level(value: number | null | undefined, budget: number, warnAt = 0.8, hotAt = 1): Level {
  if (value === null || value === undefined || !Number.isFinite(value)) return "idle";
  const share = value / Math.max(budget, 1e-9);
  if (share < warnAt) return "good";
  if (share < hotAt) return "warn";
  return "hot";
}

/**
 * Hysteresis: escalate to warn / hot only when the last `n` samples all agree; de-escalate at once.
 * `history` is mutated (keep one array per tile, e.g. in a useRef) and must get one sample per tick.
 */
export function steadyLevel(history: Level[], next: Level, n = 3): Level {
  history.push(next);
  while (history.length > n) history.shift();
  if (history.length < n) return SEVERITY[next] > SEVERITY.good ? "good" : next;
  return history.reduce((a, b) => (SEVERITY[b] < SEVERITY[a] ? b : a));
}

/**
 * The same hysteresis computed over a series (no mutable history): the level the last `n` samples agree on.
 * Prefer this when the series is backfilled (live.ts does), so a page opened mid-incident is right at once.
 */
export function steadyLevelOf(values: (number | null | undefined)[], budget: number | null | undefined, n = 3): Level {
  if (!budget) return "idle";
  const last = values.filter((v): v is number => typeof v === "number" && Number.isFinite(v)).slice(-n);
  if (!last.length) return "idle";
  const levels = last.map((v) => level(v, budget));
  if (levels.length < n) return levels.some((l) => SEVERITY[l] > SEVERITY.good) ? "good" : levels[levels.length - 1];
  return levels.reduce((a, b) => (SEVERITY[b] < SEVERITY[a] ? b : a));
}

export const LEVEL_TONE: Record<Level, "pass" | "warn" | "fail" | undefined> = {
  idle: undefined, good: "pass", warn: "warn", hot: "fail",
};
