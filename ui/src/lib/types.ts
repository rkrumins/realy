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
  /** fraud whose money did not move: declined, failed / abandoned a step-up, or held for review */
  recall_stopped?: number | null;
  fraud_usd_total?: number;
  /** approved outright or after passing a step-up */
  fraud_usd_approved?: number;
  /** ... plus fraud reviewed only after it was paid (a review cannot hold a card authorisation) */
  fraud_usd_paid?: number;
  /** payment types a REVIEW holds (contracts/insights.yaml review_holds) */
  review_holds?: string[] | null;
  stepup?: StepUp | null;
  by_type?: Record<string, {
    events: number; campaigns: number; recall_flagged: number; recall_declined: number; recall_stopped?: number;
    approved_before_first_decline_mean: number; usd_approved: number; usd_paid?: number;
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

export type StepUpSplit = { pass: number; fail: number; abandon: number };

/** Step-up economics (report.detection.stepup): present when the run issued CHALLENGE. */
export interface StepUp {
  challenged: number; legit_challenged: number; fraud_challenged: number;
  legit_challenge_rate: number; fraud_challenge_rate: number;
  legit: StepUpSplit; fraud: StepUpSplit;
  fraud_stopped: number; fraud_passed: number; fraud_usd_passed: number;
  legit_abandoned: number; lost_sales_usd: number; legit_failed: number;
  by_type: Record<string, StepUpSplit & { challenged: number; usd_passed: number }>;
  note: string;
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
  costs?: { review_cost_usd?: number; reviews_per_analyst_hour?: number; decline_friction_usd?: number };
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

export interface StateLineage {
  mode: "fresh" | "warm" | "none";
  inherited_runs?: number | null;
  inherited?: string[];
  unknown?: boolean;
}

export interface VerifyResult {
  run_id: string;
  passed?: boolean;
  /** every failed check, in words (absent on results older than the field) */
  reasons?: string[];
  /** only the feature check failed, and only because the state was not this run's own */
  inconclusive?: boolean;
  state?: StateLineage | null;
  error?: string;
  features?: {
    compared_events: number; sampling_mod: number; entities_checked: string[];
    events_with_defaulted_features?: number; exact_set_agreement: number | null;
    exact_set_target?: number; exact_set_ok?: boolean;
    comparable?: boolean; not_comparable_because?: string;
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

/** One measured event of a run, as search results and trails list it. Entity ids are null when the
 *  payment has no such entity (a transfer has no card, a card-present payment no device). */
export interface SearchRow {
  event_id: string;
  event_time_us: number;
  customer_id: string | null;
  account_id: string | null;
  card_id: string | null;
  device_id: string | null;
  merchant_id: string | null;
  terminal_id: string | null;
  payee_id: string | null;
  ip: string | null;
  ip_v4: number | null;
  ip_country: string | null;
  payment_type: string;
  channel: string;
  entry_mode: string;
  card_present: boolean;
  amount_usd: number;
  merchant_country: string | null;
  outcome: string | null;
  /** decision path; a RULES_ONLY_FALLBACK / PROVISIONAL / ECHO decision has no model score (score null) */
  path: string | null;
  score: number | null;
  model_version: string | null;
  rule_hits: string[];
  /** what happened to the payment: approved, declined, held for review, abandoned at step-up, … */
  fate: string | null;
  stepup: "pass" | "fail" | "abandon" | null;
  is_fraud: boolean;
  fraud_type: string;
  campaign_id: string;
  role: string | null;
  label: { fraud: boolean; after_days: number } | null;
  e2e_ms: number | null;
}

export interface SearchResponse {
  results: SearchRow[];
  /** every match, of which `results` is the first page */
  total: number;
  /** a bare id: how many events matched it as each kind of id (ids of different kinds can collide) */
  matched: Record<string, number>;
}

export interface DecisionDetail {
  event: Record<string, unknown>;
  decision: {
    outcome: string; score: number | null; score_note?: string | null; path: string; degraded: boolean; ingress_mode: string;
    reason_codes: string[]; rule_hits: string[]; model_version: string; revision: number; revisions?: number;
  } | null;
  features: { index: number; name: string; entity: string | null; source: string; agg?: string; value: number; missing: boolean; default: number }[] | null;
  features_note?: string | null;
  spans: { stage: number | null; stage_name: string; attr: number; entity?: string | null; domain: number | null; start_us: number | null; dur_us: number }[];
  timing: {
    t_intended_us?: number | null; t_sent_us?: number | null; t_ingest_us?: number | null;
    t_decision_us?: number | null; t_collected_us?: number | null; t_response_us?: number | null; e2e_ms?: number | null; ack_us?: number | null;
  };
  label: {
    is_fraud: boolean; fraud_type: string; campaign_id: string; role?: string | null; backfill?: boolean;
    story?: LabelStory; story_error?: string;
  };
  explanation?: Explanation | null;
  warnings?: string[];
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
  policy?: { outcome: string | null; expected?: string; branch: string; why: string; challengeable?: boolean;
    thresholds: { review: number; decline: number; challenge?: number } };
  /** the instrument's latent step-up result for this payment, and its fate if it was challenged */
  stepup?: { result: "pass" | "fail" | "abandon"; challenged: boolean; final: string | null };
  rules?: { id: string; action: string; when: string; hit: boolean; reason: string; values: Record<string, unknown>; error?: string }[];
  attribution?: {
    model_version: string | null; score: number; served_score: number; baseline_score: number; logit: number; baseline_logit: number;
    method: string; top: { feature: string; value: number; typical: number; contribution: number; direction: string }[];
  };
}

/** A column of the Explorer's trail (contracts/explorer.yaml). */
export interface ViewColumn {
  key: string;
  kind?: "field" | "feature";
  label: string;
  link?: string | null;
  /** usd_minor: a money aggregate in cents */
  format?: "usd" | "usd_minor" | "flag" | "int" | "ratio" | null;
}
export interface ViewContext { name: string; title: string; columns: ViewColumn[] }
export interface EntityDef {
  name: string; key: string; title: string; format?: string | null; shared_by: string[]; shared_min: number;
  profile?: string | null; common_point: boolean; features: ViewColumn[];
}
export interface Lens { name: string; title: string; search?: string; entity?: string; show?: string; sort?: string; description?: string }
export interface ExplorerViews {
  version: number; entities: Record<string, EntityDef>; contexts: ViewContext[]; lenses: Lens[]; kinds: string[];
  problems: string[]; cache?: { runs: string[]; mb: number; budget_mb: number };
}

/** One event of an entity's decision trail: what the decision saw and did. */
export interface TrailRow extends SearchRow {
  /** the context (kind of payment) that picked `ctx`'s columns */
  context?: string | null;
  /** the context's column values; null = n/a (the payment lacks it, or the run's schema did not have it) */
  ctx?: Record<string, string | number | boolean | null>;
  /** the entity's own state features at this event (null = defaulted / not applicable) */
  state?: Record<string, number | null>;
  /** older name of `state` */
  features?: Record<string, number | null> | null;
  /** false when the decision's feature vector was not recorded (feature sampling) */
  features_recorded?: boolean;
}

export interface TrailSummary {
  events: number; cards?: number; fraud_events?: number; amount_usd?: number; first_us?: number; last_us?: number;
  approve?: number; challenge?: number; review?: number; decline?: number; max_score?: number | null; unscored?: number;
  fraud_usd_paid?: number; fraud_usd_stopped?: number; good_stopped?: number; payment_types?: Record<string, number>;
}

export interface RelatedEntity { distinct: number; top: { id: string; display: string; events: number; fraud: number }[] }

export interface CommonPoint {
  cards_seen: number; cards_later_fraud?: number; share?: number; baseline_share?: number; lift?: number | null;
  by_type?: Record<string, number>; note?: string; error?: string;
}

export interface EntityResponse {
  entity: string; id: string; display: string; title: string;
  profile: Record<string, unknown> | null;
  summary: TrailSummary;
  related: Record<string, RelatedEntity>;
  columns: { state: ViewColumn[]; contexts: ViewContext[] };
  events: TrailRow[];
  total: number; offset: number; truncated: boolean;
  common_point?: CommonPoint;
  features_note?: string | null;
  note?: string;
}
export type CustomerResponse = EntityResponse & { customer?: Record<string, unknown> | null; features?: string[] };

export interface EntityRow {
  id: string; display: string; events: number; fraud_events: number; fraud_types: string[];
  flagged: number; declined: number; challenged: number; max_score: number | null; amount_usd: number;
  first_us: number; last_us: number; distinct: Record<string, number>;
  common_point?: { cards_seen: number; cards_later_fraud: number; lift: number | null };
}
export type CustomerRow = EntityRow & { customer_id: string; cards: number };

export interface EntitiesResponse {
  entity: string;
  rows: EntityRow[];
  stats: { entities: number; events?: number; with_2_plus?: number; with_10_plus?: number; median_events?: number;
    max_events?: number; with_fraud?: number; mixed?: number; shared?: number; shared_by?: string | null; shared_min?: number; note?: string };
}
export type CustomersResponse = EntitiesResponse & { customers: CustomerRow[] };

export interface Campaign {
  campaign_id: string;
  fraud_type: string;
  events: number;
  /** genuine events of the campaign's story (a victim's own payments, an account's build-up) */
  story_events?: number;
  customers?: number;
  flagged: number;
  declined: number;
  challenged?: number;
  stopped?: number;
  first_event_us: number;
  last_event_us: number;
  approved_before_first_decline: number;
  /** fraud events paid out before the first one was stopped (declined, held, or failed / abandoned its step-up) */
  approved_before_first_stop?: number;
  usd_total: number;
  usd_approved: number;
  /** approved, plus passed step-ups and reviews that do not hold the payment */
  usd_paid?: number;
}

export interface Typology {
  id: number; name: string; title: string; description: string; pattern: string; mix_weight: number;
  roles: string[]; signals: string[]; rules: string[]; disabled: boolean; source: string;
}

export interface DocsIndex {
  sections: { title: string; items: { path: string; title: string }[] }[];
}
export interface DocPage {
  path: string;
  title: string;
  markdown: string;
}
