/** Pipeline topology graphs + live metric resolution for the topology map.
 * The backend serves /api/topology; these client-side definitions are the fallback (same shape). */
import type { MetricRef, Snapshot, TopologyEdge, TopologyNode, TopologyResponse } from "./types";

const n = (id: string, label: string, kind: TopologyNode["kind"], column: number, row: number,
  rate: MetricRef | null, p99: MetricRef | null): TopologyNode => ({ id, label, kind, column, row, rate, p99 });
const e = (from: string, to: string, rate: MetricRef | null, label?: string): TopologyEdge => ({ from, to, rate, label });

const EMIT = { source: "emitter", counter: "emitted" };
const DECIDED = { source: "collector", counter: "decided" };

export const FALLBACK_TOPOLOGIES: Record<string, TopologyResponse> = {
  "gateway-kafka": {
    topology: "gateway-kafka",
    nodes: [
      n("emitter", "Emitter", "driver", 0, 1, EMIT, { source: "emitter", stage: "schedule_lag" }),
      n("events", "events.raw", "kafka", 1, 1, { source: "emitter", counter: "acked" }, { source: "emitter", stage: "produce_ack" }),
      n("flink", "Flink features", "flink", 2, 0, { source: "flink", stage: "state_customer" }, { source: "flink", stage: "store_update_lag" }),
      n("gateway", "Gateway", "service", 2, 2, { source: "gateway", counter: "decisions" }, { source: "gateway", stage: "hot_path" }),
      n("redis", "Redis", "store", 3, 0, { source: "flink", stage: "feature_store_write" }, { source: "collector", stage: "feature_lookup" }),
      n("model", "Model", "model", 3, 2, { source: "model", counter: "requests" }, { source: "model", stage: "model_server_total" }),
      n("decisions", "decisions", "kafka", 4, 2, { source: "gateway", counter: "decisions" }, { source: "gateway", stage: "audit_ack" }),
      n("clickhouse", "ClickHouse", "sink", 5, 3, null, null),
      n("collector", "Collector", "driver", 5, 1, DECIDED, { source: "collector", stage: "e2e" }),
    ],
    edges: [
      e("emitter", "events", EMIT), e("events", "flink", { source: "flink", stage: "state_customer" }),
      e("events", "gateway", { source: "gateway", counter: "decisions" }), e("flink", "redis", { source: "flink", stage: "feature_store_write" }),
      e("redis", "gateway", { source: "gateway", counter: "decisions" }, "lookup"), e("gateway", "model", { source: "model", counter: "requests" }),
      e("gateway", "decisions", { source: "gateway", counter: "decisions" }), e("decisions", "collector", DECIDED),
      e("decisions", "clickhouse", DECIDED),
    ],
  },
  "gateway-grpc": {
    topology: "gateway-grpc",
    nodes: [
      n("emitter", "gRPC emitter", "driver", 0, 2, EMIT, { source: "emitter", stage: "e2e" }),
      n("gateway", "Gateway", "service", 1, 2, { source: "gateway", counter: "decisions" }, { source: "gateway", stage: "hot_path" }),
      n("events", "events.raw", "kafka", 2, 0, { source: "flink", stage: "state_customer" }, null),
      n("model", "Model", "model", 2, 3, { source: "model", counter: "requests" }, { source: "model", stage: "model_server_total" }),
      n("flink", "Flink features", "flink", 3, 0, { source: "flink", stage: "state_customer" }, { source: "flink", stage: "store_update_lag" }),
      n("redis", "Redis", "store", 4, 0, { source: "flink", stage: "feature_store_write" }, { source: "collector", stage: "feature_lookup" }),
      n("decisions", "decisions (audit)", "kafka", 3, 2, { source: "gateway", counter: "decisions" }, { source: "gateway", stage: "audit_ack" }),
      n("collector", "Collector", "driver", 4, 2, DECIDED, null),
      n("clickhouse", "ClickHouse", "sink", 4, 3, null, null),
    ],
    edges: [
      e("emitter", "gateway", EMIT, "Score()"), e("gateway", "events", { source: "flink", stage: "state_customer" }, "mirror"),
      e("events", "flink", { source: "flink", stage: "state_customer" }), e("flink", "redis", { source: "flink", stage: "feature_store_write" }),
      e("redis", "gateway", { source: "gateway", counter: "decisions" }, "lookup"), e("gateway", "model", { source: "model", counter: "requests" }),
      e("gateway", "decisions", { source: "gateway", counter: "decisions" }), e("decisions", "collector", DECIDED),
      e("decisions", "clickhouse", DECIDED),
    ],
  },
  "flink-inline": {
    topology: "flink-inline",
    nodes: [
      n("emitter", "Emitter", "driver", 0, 1, EMIT, { source: "emitter", stage: "schedule_lag" }),
      n("events", "events.raw", "kafka", 1, 1, { source: "emitter", counter: "acked" }, { source: "emitter", stage: "produce_ack" }),
      n("flink", "Flink: features + scoring", "flink", 2, 1, { source: "flink", counter: "decisions" }, { source: "flink", stage: "hot_path" }),
      n("redis", "Redis (profiles)", "store", 3, 0, null, { source: "collector", stage: "feature_lookup" }),
      n("model", "Model", "model", 3, 2, { source: "model", counter: "requests" }, { source: "model", stage: "model_server_total" }),
      n("decisions", "decisions", "kafka", 4, 1, DECIDED, { source: "collector", stage: "egress" }),
      n("collector", "Collector", "driver", 5, 1, DECIDED, { source: "collector", stage: "e2e" }),
      n("clickhouse", "ClickHouse", "sink", 5, 2, null, null),
    ],
    edges: [
      e("emitter", "events", EMIT), e("events", "flink", { source: "flink", counter: "decisions" }),
      e("redis", "flink", null, "lookup"), e("flink", "model", { source: "model", counter: "requests" }),
      e("flink", "decisions", { source: "flink", counter: "decisions" }), e("decisions", "collector", DECIDED),
      e("decisions", "clickhouse", DECIDED),
    ],
  },
  "echo-kafka": {
    topology: "echo-kafka",
    nodes: [
      n("emitter", "Emitter", "driver", 0, 1, EMIT, { source: "emitter", stage: "schedule_lag" }),
      n("events", "events.raw", "kafka", 1, 1, { source: "emitter", counter: "acked" }, { source: "emitter", stage: "produce_ack" }),
      n("gateway", "Gateway (echo)", "service", 2, 1, { source: "gateway", counter: "decisions" }, { source: "gateway", stage: "hot_path" }),
      n("decisions", "decisions", "kafka", 3, 1, DECIDED, { source: "collector", stage: "egress" }),
      n("collector", "Collector", "driver", 4, 1, DECIDED, { source: "collector", stage: "e2e" }),
    ],
    edges: [e("emitter", "events", EMIT), e("events", "gateway", { source: "gateway", counter: "decisions" }),
      e("gateway", "decisions", { source: "gateway", counter: "decisions" }), e("decisions", "collector", DECIDED)],
  },
};

/** events/s for a metric reference: sum of a counter (or of a stage's sample count) over instances. */
export function resolveRate(ref: MetricRef | null, snaps: Snapshot[]): number | null {
  if (!ref) return null;
  let v: number | null = null;
  for (const s of snaps) {
    if (s.source !== ref.source) continue;
    const per = Math.max(0.05, (s.window_ms || 1000) / 1000); // snapshot counters are per window
    if (ref.counter) {
      const c = s.counters[ref.counter];
      if (c !== undefined) v = (v ?? 0) + c / per;
    } else if (ref.stage) {
      const st = s.stages.find((x) => x.name === ref.stage);
      if (st) v = (v ?? 0) + st.count / per;
    }
  }
  return v;
}

/** worst p99 (ms) across instances for a stage reference. */
export function resolveP99(ref: MetricRef | null, snaps: Snapshot[]): number | null {
  if (!ref?.stage) return null;
  let v: number | null = null;
  for (const s of snaps) {
    if (s.source !== ref.source) continue;
    const st = s.stages.find((x) => x.name === ref.stage);
    if (st) v = Math.max(v ?? 0, st.p99_ms);
  }
  return v;
}

export type Heat = "idle" | "good" | "warn" | "hot";
/** End-to-end style stages are judged against the whole budget; a single component against its share. */
const WHOLE = new Set(["e2e", "hot_path"]);
export function heat(p99: number | null, budget: number, stage?: string): Heat {
  if (p99 === null || !Number.isFinite(p99)) return "idle";
  const share = p99 / Math.max(budget, 1e-9);
  const [warn, hot] = stage && WHOLE.has(stage) ? [0.8, 1] : [0.25, 0.5];
  if (share < warn) return "good";
  if (share < hot) return "warn";
  return "hot";
}
