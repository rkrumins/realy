/** API types (python/realy/control/app.py + the new console endpoints). u64 ids are strings. */

export type Verdict = "PASS" | "FAIL" | "ABORTED" | "ERROR" | null | undefined;
export type Topology = "gateway-kafka" | "gateway-grpc" | "flink-inline" | "echo-kafka";
export const TOPOLOGIES: Topology[] = ["gateway-kafka", "gateway-grpc", "flink-inline", "echo-kafka"];

export interface ScenarioListItem {
  name: string;
  topology: Topology | null;
  description: string;
  tps: number | null;
  duration_s: number | null;
  yaml: string;
}

export interface Budget {
  e2e_p99_ms: number;
  hot_path_p99_ms: number;
  min_compliance?: number;
}

export interface CurrentRun {
  job_id: string;
  run_id: string | null;
  scenario: string;
  phase: string;
  abort_reason: string | null;
  abort_kind?: string | null;
  started_at: number | null;
  rate_multiplier: number;
  base_tps: number | null;
  topology: Topology | null;
  budget: Budget | null;
  duration_s: number | null;
  warmup_s: number | null;
}

export interface Job {
  id: string;
  run_id: string | null;
  scenario: string;
  overrides: Record<string, unknown>;
  verify: boolean;
  status: string;
  created_at: number;
  started_at: number | null;
  finished_at: number | null;
  verdict: string | null;
  verify_passed: number | null;
  abort_reason: string | null;
  error: string | null;
  schedule_id: string | null;
}

/** The pipeline lease holder: whichever process (console, CLI, bench Job) owns the pipeline now. */
export interface LeaseHolder {
  run_id: string;
  scenario?: string;
  topology?: string;
  owner?: string;
  phase?: string;
  started_at?: number;
  expires_in_s?: number | null;
}

export interface Status {
  current: CurrentRun | null;
  queue: Job[];
  blocked_by?: LeaseHolder | null;
  lease?: LeaseHolder | null;
  kafka?: boolean;
  kafka_error?: string | null;
  pipeline?: PipelineStatus | null;
}

/** Explicit per-node state from the console (pipeline_state.py): never "red" just because a run is warming. */
export type NodeStateName = "down" | "starting" | "warming" | "ready" | "degraded";
export interface NodeState {
  state: NodeStateName;
  reason: string;
  progress: number | null;
}
export interface StartupProgress {
  done: number;
  total: number;
  unit: string;
  rate: number | null;
  eta_s: number | null;
}
export interface StartupStep {
  id: string;
  label: string;
  index: number;
  status: "pending" | "running" | "done" | "skipped" | "failed" | "warn";
  detail: string;
  started_at: number | null;
  ended_at: number | null;
  duration_s?: number;
  progress: StartupProgress | null;
}
export interface StartupSnapshot {
  started_at: number;
  updated_at: number;
  active: string | null;
  steps: StartupStep[];
}
export interface PipelineStatus {
  topology: string;
  run_id: string | null;
  phase: string | null;
  nodes: Record<string, NodeState>;
  startup: StartupSnapshot | null;
}

export interface BulkAbortResult {
  results: Record<string, { result: string; channels?: Record<string, boolean> }>;
  count?: number;
  schedules_paused?: string[];
}

export interface RunSummary {
  run_id: string;
  scenario: string | null;
  topology: Topology | null;
  started_at: number;
  duration_s: number | null;
  status: string;
  phase?: string;
  verdict: Verdict;
  reasons: string[];
  achieved_tps: number | null;
  e2e_p50_ms: number | null;
  e2e_p99_ms: number | null;
  hot_path_p99_ms: number | null;
  within_budget: number | null;
  emitted: number | null;
  decided: number | null;
  missing: number | null;
  duplicates: number | null;
  abort_reason: string | null;
  abort_kind?: string | null;
  abort_phase?: string | null;
  measured_s?: number | null;
  owner?: string | null;
  verify_passed: boolean | null;
  has_html: boolean;
  job_id?: string;
  verify_requested?: boolean;
}

export interface RunsResponse {
  queue: Job[];
  jobs: Job[];
  history: RunSummary[];
  current: CurrentRun | null;
  lease?: LeaseHolder | null;
  blocked_by?: LeaseHolder | null;
}

export interface StageStats {
  count: number;
  p50: number;
  p90: number;
  p99: number;
  p999: number;
  max: number;
  mean?: number;
}

export interface Level {
  level: number;
  t_from_s: number;
  target_tps: number;
  emitted_tps: number;
  decided_tps: number;
  stages: Record<string, StageStats>;
  within_budget?: number;
}

export interface ReportSummary {
  measured_s?: number | null;
  prepare_s?: number | null;
  verdict: Verdict;
  reasons: string[];
  emitted: number;
  decided: number;
  missing: number;
  duplicates: number;
  unknown?: number;
  achieved_emit_tps: number;
  e2e_p50_ms: number | null;
  e2e_p99_ms: number | null;
  e2e_p999_ms: number | null;
  hot_path_p99_ms: number | null;
  within_budget: number;
  schedule_lag_p99_ms: number | null;
  degraded_fraction: number;
  outcomes: Record<string, number>;
  online_store_update_lag_p99_ms?: number | null;
  knee: { level: number; target_tps: number; e2e_p99_ms: number; decided_tps: number; emitted_tps: number } | null;
  sustainable: boolean | null;
  expected_verdict?: string;
  matches_expectation?: boolean;
}

export interface Detection {
  fraud_events?: number;
  legit_events?: number;
  recall_flagged?: number | null;
  recall_declined?: number | null;
  precision_flagged?: number | null;
  false_positive_rate?: number | null;
  fraud_usd_total?: number;
  fraud_usd_approved?: number;
  by_type?: Record<string, {
    events: number; campaigns: number; recall_flagged: number; recall_declined: number;
    approved_before_first_decline_mean: number; usd_approved: number;
  }>;
  by_rule?: Record<string, { hits: number; fraud: number; legit: number; precision: number; legit_hit_rate: number }>;
  legit_flagged?: number;
  legit_declined?: number;
  fraud_flagged?: number;
  fraud_declined?: number;
  model?: {
    scored: number; rules_only_fallback: number; fallback_recall_flagged: number | null;
    roc_auc?: number; pr_auc?: number; fraud_base_rate?: number;
  };
  evaluation?: {
    sample: "in-sample" | "out-of-sample" | "unknown" | "not-trained"; reason?: string; model_version?: string;
    labels?: string; trained_on?: { run_id: string; scenario?: string; seed?: number }[];
  };
  labels?: { maturity_days: number; fraud_events: number; reported: number; coverage: number; by_type: Record<string, number> } | null;
}

/** A ranked finding (python/realy/report/insights.py). */
export interface Insight {
  severity: "critical" | "warning" | "info" | "good";
  area: string;
  title: string;
  detail: string;
  evidence: Record<string, unknown>;
  link: { kind: string; query?: string; id?: string; a?: string; b?: string } | null;
  action: string;
}

export interface InsightsConfig {
  production_base_rate?: number;
  label_maturity_days?: number;
  costs?: { review_cost_usd?: number; reviews_per_analyst_hour?: number; decline_friction_usd?: number; challenge_abandon_rate?: number };
  thresholds?: Record<string, number>;
}

export interface Report {
  run_id: string;
  scenario: string;
  topology: Topology;
  manifest: Record<string, unknown>;
  budget: Budget;
  summary: ReportSummary;
  stages: Record<string, StageStats>;
  levels: Level[];
  funnel: Record<string, number[]>;
  backlog: number[];
  e2e_p99_by_second: (number | null)[];
  e2e_p50_by_second: (number | null)[];
  tail_attribution: {
    threshold_ms?: number; events?: number; mean_ms_by_stage?: Record<string, number>;
    share?: Record<string, number>; dominant?: string | null;
  };
  sustainability: {
    window_s?: number; p99_by_window_ms?: number[]; p99_slope_ms_per_window?: number;
    backlog_growing?: boolean; max_backlog?: number; sustainable?: boolean;
  };
  detection: Detection;
  insights?: Insight[];
  insights_config?: InsightsConfig;
  startup?: StartupSnapshot | null;
  service: { stages?: Record<string, { p50_median_ms: number; p99_median_ms: number; p99_max_ms: number; count: number }>; counters?: Record<string, number> };
  sync?: Record<string, number>;
}

export interface VerifyResult {
  run_id: string;
  passed?: boolean;
  error?: string;
  features?: {
    compared_events: number; sampling_mod: number; entities_checked: string[];
    events_with_defaulted_features?: number; exact_set_agreement: number | null;
    per_feature: Record<string, { agreement: number; max_abs_diff: number; entity: string | null; source: string }>;
    worst: [string, number][]; note: string;
  };
  rules?: { checked: number; parity: number; mismatches: unknown[] };
  score?: { checked: number; parity: number; model_version?: string | null };
  outcome?: { checked: number; parity: number };
  audit?: Record<string, number | boolean | string> & { error?: string };
}

export interface RunDetail {
  run_id: string;
  summary: RunSummary;
  manifest: Record<string, unknown> | null;
  report: Report | null;
  verify: VerifyResult | null;
  run: Record<string, unknown> | null;
  scenario: Record<string, unknown> | null;
  logs: string[];
}

export interface CompareResponse {
  a: RunSummary;
  b: RunSummary;
  stages: Record<string, Record<string, number | null>>;
  levels: { a: Level[]; b: Level[] };
  detection: { a: Detection; b: Detection };
}

export interface BudgetCheck {
  scenario: string;
  topology: string;
  samples: number;
  budget: Budget;
  e2e: Record<string, number>;
  hot_path: Record<string, number>;
  within_budget: number;
  fallback_rate: number;
  stages_p99: Record<string, number>;
  tail_share: Record<string, number>;
  verdict: "FEASIBLE" | "MARGINAL" | "INFEASIBLE";
  advice: string[];
  note?: string;
}

export interface Schedule {
  id: string;
  scenario: string;
  overrides: Record<string, unknown> | string;
  verify: boolean | number;
  next_at: number;
  every_minutes: number | null;
  remaining: number | null;
  enabled: number | boolean;
  created_at: number;
  last_job: string | null;
}

// ------------------------------------------------------------------ live telemetry
export interface StageStat {
  name: string;
  count: number;
  p50_ms: number;
  p90_ms: number;
  p99_ms: number;
  p999_ms: number;
  max_ms: number;
}

export interface Snapshot {
  run_id: string;
  source: "emitter" | "collector" | "gateway" | "model" | "flink" | string;
  instance: string;
  ts_us: number;
  window_ms?: number;
  counters: Record<string, number>;
  stages: StageStat[];
  phase?: number | string;
}

export interface LiveDecision {
  run_id: string;
  event_id: string;
  customer_id: string;
  outcome: string;
  score: number;
  path: string;
  degraded: boolean;
  amount_usd: number;
  payment_type: string;
  channel: string;
  rule_hits: string[];
  hot_path_ms: number | null;
  ts_us: number;
  merchant_country?: string;
}

// ------------------------------------------------------------------ new endpoints
export interface AuthMe {
  auth_required: boolean;
  authenticated: boolean;
}

export interface SummaryResponse {
  runs_total: number;
  runs_pass: number;
  runs_fail: number;
  events_total: number;
  decisions_total: number;
  last_runs: {
    run_id: string; scenario: string; topology: string; verdict: Verdict; started: number | string;
    e2e_p50_ms: number | null; e2e_p99_ms: number | null; achieved_emit_tps: number | null;
    emitted: number | null; decided: number | null; missing: number | null; duplicates: number | null;
    status?: string; within_budget?: number | null; hot_path_p99_ms?: number | null;
  }[];
  current?: CurrentRun | null;
}

export type HealthStatus = "up" | "down" | "degraded";
export interface EnvComponent {
  id: string;
  name: string;
  kind?: string;
  status: HealthStatus;
  latency_ms: number | null;
  version: string | null;
  details: Record<string, unknown>;
  url: string | null;
}
export interface EnvResponse {
  checked_at: string;
  components: EnvComponent[];
  links: { flink?: string | null; grafana?: string | null; prometheus?: string | null };
}
export interface KafkaEnv {
  brokers: { id: number | string; host: string; port: number }[];
  topics: { name: string; partitions: number; end_offset_total: number }[];
  groups: { group: string; topic: string; lag: number | null; state?: string | null }[];
}
export interface FlinkEnv {
  version: string | null;
  ui_url: string | null;
  taskmanagers: number;
  slots_total: number;
  slots_available: number;
  jobs: {
    id: string; name: string; state: string; start_time: number; duration_ms: number;
    checkpoints: { completed: number; failed: number; last_duration_ms: number | null; last_size_bytes: number | null; last_completed_at: number | null } | null;
    vertices: { name: string; parallelism: number; status: string; backpressure: string | null; read_records: number | null; write_records: number | null }[];
  }[];
}
export interface RedisEnv {
  version: string; used_memory_bytes: number; keys: number; ops_per_sec: number;
  connected_clients: number; uptime_s: number; stream_blobs?: number | null; profile_blobs?: number | null;
}
export interface ClickHouseEnv {
  version: string;
  tables: { table: string; rows: number; parts: number; bytes: number }[];
}

export interface MetricRef {
  source: string;
  counter?: string;
  stage?: string;
}
export interface TopologyNode {
  id: string;
  label: string;
  kind: "driver" | "kafka" | "flink" | "store" | "service" | "model" | "sink";
  column: number;
  row: number;
  rate: MetricRef | null;
  p99: MetricRef | null;
  subtitle?: string | null;
  /** own SLO for this node's p99 (e.g. online-store freshness) instead of a share of the e2e budget */
  slo_ms?: number;
}
export interface TopologyEdge {
  from: string;
  to: string;
  label?: string;
  rate: MetricRef | null;
  p99?: MetricRef | null;
  /** history-replay rate on this hop, shown while warming and never judged */
  bulk?: MetricRef | null;
}
export interface TopologyResponse {
  topology: string;
  nodes: TopologyNode[];
  edges: TopologyEdge[];
}

export interface SearchRow {
  event_id: string;
  customer_id: string;
  card_id: string;
  event_time_us: number;
  payment_type: string;
  channel: string;
  amount_usd: number;
  merchant_country: string;
  outcome: string | null;
  score: number | null;
  is_fraud: boolean;
  fraud_type: string;
  campaign_id: string;
  e2e_ms: number | null;
}

export interface DecisionDetail {
  event: Record<string, unknown>;
  decision: {
    outcome: string; score: number; path: string; degraded: boolean; ingress_mode: string;
    reason_codes: string[]; rule_hits: string[]; model_version: string; revision: number; revisions?: number;
  } | null;
  features: { index: number; name: string; entity: string | null; source: string; agg?: string; value: number; missing: boolean; default: number }[] | null;
  spans: { stage: number | null; stage_name: string; attr: number; entity?: string | null; domain: number | null; start_us: number | null; dur_us: number }[];
  timing: {
    t_intended_us?: number | null; t_sent_us?: number | null; t_ingest_us?: number | null;
    t_decision_us?: number | null; t_collected_us?: number | null; t_response_us?: number | null; e2e_ms?: number | null; ack_us?: number | null;
  };
  label: {
    is_fraud: boolean; fraud_type: string; campaign_id: string;
    story?: LabelStory; story_error?: string;
  };
  explanation?: Explanation | null;
}

export interface LabelStory {
  title: string; truth: "fraud" | "legitimate"; typology: string | null; campaign_id: string | null;
  role: string | null; role_typology: string | null; step: { index: number; of: number } | null;
  label_source: string | null; label_delay_days: number | null; maturity_days: number;
  observed: string; training: string;
}

export interface Explanation {
  error?: string;
  rule_set?: string;
  rules_source?: "snapshot" | "current";
  served_rule_hits?: string[];
  policy?: { outcome: string | null; branch: string; why: string; thresholds: { review: number; decline: number } };
  rules?: { id: string; action: string; when: string; hit: boolean; reason: string; values: Record<string, unknown>; error?: string }[];
  attribution?: {
    model_version: string | null; score: number; served_score: number; baseline_score: number; logit: number; baseline_logit: number;
    method: string; top: { feature: string; value: number; typical: number; contribution: number; direction: string }[];
  };
}

export interface CustomerResponse {
  customer: { customer_id: string; segment: string | number; home_country: string; tenure_days: number; avg_ticket_usd: number } | null;
  events: SearchRow[];
}

export interface Campaign {
  campaign_id: string;
  fraud_type: string;
  events: number;
  flagged: number;
  declined: number;
  first_event_us: number;
  last_event_us: number;
  approved_before_first_decline: number;
  usd_total: number;
  usd_approved: number;
}

export interface DocsIndex {
  sections: { title: string; items: { path: string; title: string }[] }[];
}
export interface DocPage {
  path: string;
  title: string;
  markdown: string;
}
