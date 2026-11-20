/** API types, mirrored by hand from backend/server (keep both sides in sync when a payload changes). */

export type JobStatus = "queued" | "running" | "done" | "failed" | "aborted" | "cancelled" | "interrupted";
export type StepStatus = "pending" | "running" | "done" | "skipped" | "warn" | "failed";

export interface StepProgress {
  done: number;
  total: number;
  unit?: string;
  rate?: number | null; // units per second
  eta_s?: number | null;
}

/** One step of a multi-step job (checklist with progress, see components/steps.tsx). */
export interface Step {
  id: string;
  index: number;
  label: string;
  status: StepStatus;
  detail?: string;
  duration_s?: number | null;
  progress?: StepProgress | null;
  /** the component this step is busy with (drives a "warming" state on the live page) */
  component?: string | null;
}

export type Severity = "critical" | "warning" | "info" | "good";

/** A ranked finding: what happened, the evidence, and the next step. */
export interface Insight {
  severity: Severity;
  title: string;
  detail?: string;
  action?: string;
  area?: string;
  /** in-page section id (#anchor) that holds the evidence */
  anchor?: string | null;
}

export interface SeriesPoint {
  t: number; // seconds since the job started
  throughput: number | null;
  p50_ms: number | null;
  p99_ms: number | null;
}

export interface JobResult {
  processed: number;
  failed: number;
  throughput: number | null; // items/s, mean over the run
  p50_ms: number | null;
  p99_ms: number | null;
  budget_ms: number | null;
  within_budget: number | null; // share of seconds whose p99 was within budget (0..1)
  insights: Insight[];
  series: SeriesPoint[];
}

export interface Job {
  id: string;
  kind: string;
  params: Record<string, unknown>;
  status: JobStatus;
  created_at: number; // epoch seconds
  started_at: number | null;
  finished_at: number | null;
  error: string | null;
  abort_reason: string | null;
  result: JobResult | null;
  steps: Step[];
  logs?: string[];
}

export interface CurrentJob {
  id: string;
  kind: string;
  phase: string;
  started_at: number | null;
  planned_s: number | null;
  budget_ms: number | null;
  abort_reason: string | null;
  steps: Step[];
}

/** Explicit per-component state from the backend (server/state.py): never "red" just because it is warming. */
export type ComponentStateName = "down" | "starting" | "warming" | "ready" | "degraded";
export interface ComponentState {
  state: ComponentStateName;
  reason: string;
  progress: number | null;
}

export interface Status {
  current: CurrentJob | null;
  queue: Job[];
  components?: Record<string, ComponentState>;
  server_time?: number;
}

export interface MetricSample {
  job_id: string;
  ts: number; // epoch seconds
  throughput: number | null;
  p50_ms: number | null;
  p99_ms: number | null;
  errors: number | null;
}

export interface LogLine {
  job_id: string;
  line: string;
  ts: number;
}

export interface JobsResponse {
  jobs: Job[];
  queue: Job[];
  current: CurrentJob | null;
}

export interface Summary {
  jobs_total: number;
  jobs_done: number;
  jobs_failed: number;
  items_total: number;
  last_jobs: Job[];
  current: CurrentJob | null;
}

export interface JobParam {
  name: string;
  label: string;
  unit?: string;
  default: number;
  min?: number;
  max?: number;
  step?: number;
  hint?: string;
}

export interface JobKind {
  kind: string;
  label: string;
  description: string;
  params: JobParam[];
}

export type HealthStatus = "up" | "degraded" | "down";

export interface EnvComponent {
  id: string;
  name: string;
  kind: string;
  status: HealthStatus;
  latency_ms: number | null;
  version: string | null;
  details: Record<string, unknown>;
  url?: string | null;
}

export interface EnvResponse {
  checked_at: string; // ISO-8601
  components: EnvComponent[];
}

export interface AuthMe {
  auth_required: boolean;
  authenticated: boolean;
}
