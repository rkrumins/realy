/** Thin typed client for the console API. Every 401 flips the app into the sign-in screen. */
import type {
  AuthMe, BudgetCheck, BulkAbortResult, Campaign, ClickHouseEnv, CompareResponse, CustomerResponse, CustomersResponse, DecisionDetail, DocPage, DocsIndex,
  EntitiesResponse, EntityResponse, ExplorerViews, SearchResponse, Typology,
  EnvResponse, FlinkEnv, Job, KafkaEnv, RedisEnv, RunDetail, RunsResponse, ScenarioListItem, Schedule, SearchRow,
  Status, SummaryResponse, TopologyResponse, TopologiesResponse, PreflightResult,
} from "./types";

export class ApiError extends Error {
  status: number;
  detail: unknown;
  constructor(status: number, message: string, detail?: unknown) {
    super(message);
    this.status = status;
    this.detail = detail;
  }
}

type Listener = () => void;
const authListeners = new Set<Listener>();
export function onUnauthorized(fn: Listener): () => void {
  authListeners.add(fn);
  return () => authListeners.delete(fn);
}

function detailMessage(detail: unknown): string {
  if (typeof detail === "string") return detail;
  if (detail && typeof detail === "object") {
    const d = detail as { errors?: { loc?: unknown[]; msg?: string }[] };
    if (Array.isArray(d.errors)) {
      return d.errors.map((e) => `${(e.loc || []).join(".")}: ${e.msg}`).join("; ");
    }
    return JSON.stringify(detail);
  }
  return "request failed";
}

export async function request<T>(path: string, init?: RequestInit & { json?: unknown }): Promise<T> {
  const headers: Record<string, string> = { Accept: "application/json" };
  let body = init?.body;
  if (init?.json !== undefined) {
    headers["Content-Type"] = "application/json";
    body = JSON.stringify(init.json);
  }
  const res = await fetch(path, { credentials: "same-origin", ...init, headers: { ...headers, ...(init?.headers || {}) }, body });
  if (res.status === 401 && !path.startsWith("/api/auth/")) authListeners.forEach((f) => f());
  const text = await res.text();
  let data: unknown = undefined;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = text;
    }
  }
  if (!res.ok) {
    const detail = (data && typeof data === "object" && "detail" in (data as object)) ? (data as { detail: unknown }).detail : data;
    throw new ApiError(res.status, detailMessage(detail) || res.statusText, detail);
  }
  return data as T;
}

const enc = encodeURIComponent;
export const isNotFound = (e: unknown) => e instanceof ApiError && (e.status === 404 || e.status === 405);

export const api = {
  // auth
  me: () => request<AuthMe>("/api/auth/me"),
  login: (token: string) => request<{ ok: boolean }>("/api/auth/login", { method: "POST", json: { token } }),
  logout: () => request<{ ok: boolean }>("/api/auth/logout", { method: "POST" }),
  // scenarios
  scenarios: () => request<ScenarioListItem[]>("/api/scenarios"),
  scenario: (name: string) => request<{ name: string; yaml: string; scenario: Record<string, unknown> }>(`/api/scenarios/${enc(name)}`),
  validate: (yaml: string) => request<{ ok: boolean; scenario: Record<string, unknown>; fingerprint: string }>("/api/scenarios/validate", { method: "POST", json: { yaml } }),
  saveScenario: (name: string, yaml: string) => request<{ ok: boolean }>(`/api/scenarios/${enc(name)}`, { method: "PUT", json: { yaml } }),
  resetScenario: (name: string) => request<{ ok: boolean }>(`/api/scenarios/${enc(name)}/reset`, { method: "POST" }),
  budget: (name: string, body: { overrides?: Record<string, unknown>; yaml?: string; samples?: number }) =>
    request<BudgetCheck>(`/api/scenarios/${enc(name)}/budget`, { method: "POST", json: body }),
  // runs
  runs: (limit = 200) => request<RunsResponse>(`/api/runs?limit=${limit}`),
  run: (id: string) => request<RunDetail>(`/api/runs/${enc(id)}`),
  startRun: (scenario: string, overrides: Record<string, unknown>, verify: boolean) =>
    request<Job>("/api/runs", { method: "POST", json: { scenario, overrides, verify } }),
  abort: (ident: string) => request<{ result: string }>(`/api/runs/${enc(ident)}/abort`, { method: "POST" }),
  abortMany: (body: { ids?: string[]; all?: boolean; include_queued?: boolean; reason?: string }) =>
    request<BulkAbortResult>("/api/runs/abort", { method: "POST", json: body }),
  stopAll: (pauseSchedules = true) =>
    request<BulkAbortResult>("/api/stop-all", { method: "POST", json: { pause_schedules: pauseSchedules } }),
  clearQueue: () => request<{ cancelled: string[] }>("/api/queue/clear", { method: "POST" }),
  compare: (a: string, b: string) => request<CompareResponse>(`/api/compare?a=${enc(a)}&b=${enc(b)}`),
  summary: () => request<SummaryResponse>("/api/summary"),
  // live
  status: () => request<Status>("/api/status"),
  liveRate: (tps: number) => request<{ published: boolean }>("/api/live/rate", { method: "POST", json: { tps } }),
  liveScoring: (body: Record<string, unknown>) => request<{ published: boolean; gateway_admin?: string }>("/api/live/scoring", { method: "POST", json: body }),
  liveModel: (body: Record<string, unknown>) => request<{ published: boolean; model_admin?: string }>("/api/live/model", { method: "POST", json: body }),
  topology: (t: string, flows = false) => request<TopologyResponse>(`/api/topology?topology=${enc(t)}${flows ? "&flows=true" : ""}`),
  topologies: () => request<TopologiesResponse>("/api/topologies"),
  preflight: (q: { scenario?: string; topology?: string }) =>
    request<PreflightResult>(`/api/env/preflight?${new URLSearchParams(Object.entries(q).filter(([, v]) => v) as [string, string][]).toString()}`),
  // schedules
  schedules: () => request<Schedule[]>("/api/schedules"),
  addSchedule: (body: { scenario: string; overrides?: Record<string, unknown>; verify?: boolean; at?: string; every_minutes?: number; count?: number }) =>
    request<Schedule>("/api/schedules", { method: "POST", json: body }),
  deleteSchedule: (id: string) => request<{ deleted: string }>(`/api/schedules/${enc(id)}`, { method: "DELETE" }),
  pauseSchedule: (id: string) => request<{ paused: string[] }>(`/api/schedules/${enc(id)}/pause`, { method: "POST" }),
  resumeSchedule: (id: string) => request<{ resumed: string[] }>(`/api/schedules/${enc(id)}/resume`, { method: "POST" }),
  // environment
  env: () => request<EnvResponse>("/api/env"),
  envKafka: () => request<KafkaEnv>("/api/env/kafka"),
  envFlink: () => request<FlinkEnv>("/api/env/flink"),
  envRedis: () => request<RedisEnv>("/api/env/redis"),
  envClickhouse: () => request<ClickHouseEnv>("/api/env/clickhouse"),
  // explorer
  explorerViews: () => request<ExplorerViews>(`/api/explorer/views`),
  usecases: () => request<{ typologies: Typology[] }>(`/api/usecases`),
  search: (run: string, q: string, kind = "auto", limit = 50) =>
    request<SearchResponse>(`/api/runs/${enc(run)}/search?q=${enc(q)}&kind=${enc(kind)}&limit=${limit}`),
  decision: (run: string, eventId: string, maturityDays = 30) =>
    request<DecisionDetail>(`/api/runs/${enc(run)}/decisions/${enc(eventId)}?maturity_days=${maturityDays}`),
  entities: (run: string, entity: string, sort = "events", show = "all", limit = 100, q = "") =>
    request<EntitiesResponse>(`/api/runs/${enc(run)}/entities/${enc(entity)}?sort=${enc(sort)}&show=${enc(show)}&limit=${limit}&q=${enc(q)}`),
  entity: (run: string, entity: string, id: string, offset = 0, limit = 500) =>
    request<EntityResponse>(`/api/runs/${enc(run)}/entities/${enc(entity)}/${enc(id)}?offset=${offset}&limit=${limit}`),
  customer: (run: string, cid: string) => request<CustomerResponse>(`/api/runs/${enc(run)}/customers/${enc(cid)}`),
  customers: (run: string, sort = "events", show = "all", limit = 100) =>
    request<CustomersResponse>(`/api/runs/${enc(run)}/customers?sort=${enc(sort)}&show=${enc(show)}&limit=${limit}`),
  campaigns: (run: string) => request<{ campaigns: Campaign[] }>(`/api/runs/${enc(run)}/campaigns`),
  campaign: (run: string, cid: string) => request<{ events: SearchRow[] }>(`/api/runs/${enc(run)}/campaigns/${enc(cid)}`),
  // docs
  docs: () => request<DocsIndex>("/api/docs"),
  docPage: (path: string) => request<DocPage>(`/api/docs/page?path=${enc(path)}`),
  scenarioSchema: () => request<Record<string, unknown>>("/api/reference/scenario-schema"),
  openapi: () => request<Record<string, unknown>>("/api/openapi.json"),
};
