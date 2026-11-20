/** Thin typed client for the console API. Every 401 flips the app into the sign-in screen. */
import type { AuthMe, EnvResponse, Job, JobKind, JobsResponse, LogLine, MetricSample, Status, Summary } from "./types";

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

/** FastAPI errors: {"detail": "text"} or {"detail": {"errors": [{loc, msg}]}} (Pydantic validation). */
function detailMessage(detail: unknown): string {
  if (typeof detail === "string") return detail;
  if (detail && typeof detail === "object") {
    const d = detail as { errors?: { loc?: unknown[]; msg?: string }[] };
    if (Array.isArray(d.errors)) return d.errors.map((e) => `${(e.loc || []).join(".")}: ${e.msg}`).join("; ");
    if (Array.isArray(detail)) {
      return (detail as { loc?: unknown[]; msg?: string }[]).map((e) => `${(e.loc || []).join(".")}: ${e.msg}`).join("; ");
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
    const detail = data && typeof data === "object" && "detail" in (data as object) ? (data as { detail: unknown }).detail : data;
    throw new ApiError(res.status, detailMessage(detail) || res.statusText, detail);
  }
  return data as T;
}

const enc = encodeURIComponent;
/** An endpoint this backend version does not have: render "not available", not an error. */
export const isNotFound = (e: unknown) => e instanceof ApiError && (e.status === 404 || e.status === 405);

export const api = {
  // auth
  me: () => request<AuthMe>("/api/auth/me"),
  login: (token: string) => request<{ ok: boolean }>("/api/auth/login", { method: "POST", json: { token } }),
  logout: () => request<{ ok: boolean }>("/api/auth/logout", { method: "POST" }),
  // overview + status
  summary: () => request<Summary>("/api/summary"),
  status: () => request<Status>("/api/status"),
  // jobs
  kinds: () => request<JobKind[]>("/api/kinds"),
  jobs: (limit = 200) => request<JobsResponse>(`/api/jobs?limit=${limit}`),
  job: (id: string) => request<Job>(`/api/jobs/${enc(id)}`),
  createJob: (kind: string, params: Record<string, unknown>) => request<Job>("/api/jobs", { method: "POST", json: { kind, params } }),
  abortJob: (id: string) => request<{ result: string }>(`/api/jobs/${enc(id)}/abort`, { method: "POST" }),
  // environment
  env: () => request<EnvResponse>("/api/env"),
  // live backfill (the WebSocket carries everything after this)
  liveHistory: (seconds = 600) =>
    request<{ metrics: MetricSample[]; logs: LogLine[] } & Status>(`/api/live/history?seconds=${seconds}`),
};
