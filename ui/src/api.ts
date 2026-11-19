// Thin typed client for the realy console API.

export type Stage = { name: string; count: number; p50_ms: number; p90_ms: number; p99_ms: number; p999_ms: number; max_ms: number };
export type Snapshot = {
  run_id: string; source: string; instance: string; ts_us: number; window_ms?: number;
  counters: Record<string, number>; stages: Stage[];
};
export type Current = {
  job_id: string; run_id: string; scenario: string; phase: string; abort_reason?: string | null;
  started_at?: number; rate_multiplier: number; base_tps?: number; topology?: string;
  budget?: { e2e_p99_ms: number; hot_path_p99_ms: number; min_compliance: number };
  duration_s?: number; warmup_s?: number;
} | null;
export type Job = {
  id: string; run_id?: string; scenario: string; overrides: Record<string, unknown>; verify: boolean; status: string;
  created_at: number; started_at?: number; finished_at?: number; verdict?: string; abort_reason?: string; error?: string;
};
export type RunSummary = {
  run_id: string; scenario?: string; topology?: string; started_at: number; duration_s?: number; status: string;
  verdict?: string; reasons: string[]; achieved_tps?: number; e2e_p50_ms?: number; e2e_p99_ms?: number;
  hot_path_p99_ms?: number; within_budget?: number; emitted?: number; decided?: number; missing?: number;
  duplicates?: number; abort_reason?: string | null; verify_passed?: boolean | null; has_html: boolean; phase?: string;
};
export type ScenarioInfo = { name: string; topology?: string; description: string; tps?: number; duration_s?: number; yaml: string };
export type Schedule = {
  id: string; scenario: string; overrides: Record<string, unknown>; verify: boolean; next_at: number;
  every_minutes?: number | null; remaining?: number | null; enabled: number; last_job?: string;
};

async function req<T>(method: string, path: string, body?: unknown): Promise<T> {
  const r = await fetch(path, {
    method,
    headers: body !== undefined ? { "Content-Type": "application/json" } : undefined,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const text = await r.text();
  const data = text ? JSON.parse(text) : null;
  if (!r.ok) {
    const d = data?.detail ?? text;
    throw new Error(typeof d === "string" ? d : JSON.stringify(d, null, 1));
  }
  return data as T;
}

export const api = {
  scenarios: () => req<ScenarioInfo[]>("GET", "/api/scenarios"),
  scenario: (n: string) => req<{ name: string; yaml: string }>("GET", `/api/scenarios/${n}`),
  validate: (yaml: string) => req<{ ok: boolean; fingerprint: string }>("POST", "/api/scenarios/validate", { yaml }),
  saveScenario: (n: string, yaml: string) => req<{ ok: boolean }>("PUT", `/api/scenarios/${n}`, { yaml }),
  budget: (n: string, body: Record<string, unknown>) => req<BudgetResult>("POST", `/api/scenarios/${n}/budget`, body),
  runs: () => req<{ queue: Job[]; jobs: Job[]; history: RunSummary[]; current: Current }>("GET", "/api/runs"),
  run: (id: string) => req<RunDetail>("GET", `/api/runs/${id}`),
  startRun: (scenario: string, overrides: Record<string, unknown>, verify: boolean) =>
    req<Job>("POST", "/api/runs", { scenario, overrides, verify }),
  abort: (id: string) => req<{ result: string }>("POST", `/api/runs/${id}/abort`),
  compare: (a: string, b: string) => req<CompareResult>("GET", `/api/compare?a=${encodeURIComponent(a)}&b=${encodeURIComponent(b)}`),
  rate: (tps: number) => req<{ published: boolean }>("POST", "/api/live/rate", { tps }),
  scoring: (cfg: Record<string, unknown>) => req<{ gateway_admin: string }>("POST", "/api/live/scoring", cfg),
  model: (cfg: Record<string, unknown>) => req<{ model_admin: string }>("POST", "/api/live/model", cfg),
  history: (runId?: string) => req<{ metrics: Record<string, Snapshot[]>; logs: LogMsg[]; current: Current }>(
    "GET", `/api/live/history${runId ? `?run_id=${encodeURIComponent(runId)}` : ""}`),
  schedules: () => req<Schedule[]>("GET", "/api/schedules"),
  addSchedule: (b: Record<string, unknown>) => req<Schedule>("POST", "/api/schedules", b),
  deleteSchedule: (id: string) => req<{ deleted: string }>("DELETE", `/api/schedules/${id}`),
};

export type LogMsg = { type: "log"; run_id: string; line: string; ts: number };
export type StageStats = { count: number; p50: number; p90: number; p99: number; p999: number; max: number; mean: number };
export type Level = { level: number; target_tps: number; emitted_tps: number; decided_tps: number; within_budget?: number; stages: Record<string, StageStats> };
export type Report = {
  run_id: string; scenario: string; topology: string; budget: { e2e_p99_ms: number; hot_path_p99_ms: number };
  summary: Record<string, any>; stages: Record<string, StageStats>; levels: Level[];
  tail_attribution?: { threshold_ms: number; events: number; share: Record<string, number>; mean_ms_by_stage: Record<string, number>; dominant?: string };
  detection?: Record<string, any>; sustainability?: Record<string, any>; e2e_p99_by_second?: (number | null)[];
  e2e_p50_by_second?: (number | null)[]; funnel?: Record<string, number[]>; service?: Record<string, any>; sync?: Record<string, number>;
};
export type RunDetail = { run_id: string; summary: RunSummary; manifest?: any; report?: Report; verify?: any; run?: any; scenario?: any; logs: string[] };
export type CompareResult = { a: RunSummary; b: RunSummary; stages: Record<string, Record<string, number | null>>; levels: { a: Level[]; b: Level[] } };
export type BudgetResult = {
  scenario: string; topology: string; verdict: string; e2e: Record<string, number>; hot_path: Record<string, number>;
  within_budget: number; fallback_rate: number; stages_p99: Record<string, number>; tail_share: Record<string, number>;
  advice: string[]; note: string; budget: { e2e_p99_ms: number; hot_path_p99_ms: number };
};

export const fmt = (v: unknown, d = 1): string => {
  if (v === null || v === undefined || (typeof v === "number" && !isFinite(v))) return "–";
  if (typeof v === "number") return v.toLocaleString(undefined, { maximumFractionDigits: d, minimumFractionDigits: d });
  return String(v);
};
export const fmtInt = (v: unknown) => fmt(v, 0);
export const pct = (v: unknown, d = 2) => (typeof v === "number" ? `${(v * 100).toFixed(d)}%` : "–");
export const ago = (t?: number) => {
  if (!t) return "–";
  const s = Date.now() / 1000 - t;
  if (s < 60) return `${s.toFixed(0)}s ago`;
  if (s < 3600) return `${(s / 60).toFixed(0)}m ago`;
  if (s < 86400) return `${(s / 3600).toFixed(1)}h ago`;
  return new Date(t * 1000).toLocaleString();
};
