/**
 * Live telemetry store: one WebSocket to /api/live (metrics | log | status | decision messages),
 * backfilled from /api/live/history, aggregated into per-second points for the charts. React reads it
 * through useSyncExternalStore; notifications are throttled to ~5/s so 1 s snapshots from many
 * instances never thrash rendering.
 */
import { useSyncExternalStore } from "react";
import { request } from "./api";
import type { LiveDecision, Snapshot, StageStat, Status } from "./types";

const KEEP_S = 600;

export interface LogLine {
  run_id: string;
  line: string;
  ts: number;
}

type Bucket = Map<string, Snapshot>; // key = source:instance

/** What GET /api/runs/{id}/telemetry returns (control/replay.py). */
export interface Telemetry {
  run: {
    run_id: string; scenario: string; topology: string | null; budget: { e2e_p99_ms?: number } | null; base_tps: number | null;
    warmup_s: number | null; duration_s: number | null; started_at: number | null; measure_started_at: number | null;
    ended_at: number | null; verdict: string | null; complete: boolean; first_us?: number; last_us?: number;
    timeline?: import("./types").RunTimeline;
  };
  metrics: Record<string, Snapshot[]>;
  logs: LogLine[];
  startup: { started_at: number; steps: { id: string; label: string; index: number; status: string; detail: string; started_at: number | null; ended_at: number | null }[] } | null;
  markers: { t: number; kind: string; label: string }[];
  decisions: LiveDecision[];
  note?: string;
}

/**
 * The live telemetry store. The default instance (`live`) follows the pipeline over the WebSocket; a
 * replay instance (`new LiveStore({ replay: true })`) never connects: it is loaded with a past run's
 * telemetry and read "as of" its `clock`, so the Live page renders a past run with the same components.
 */
export class LiveStore {
  readonly replay: boolean;
  /** replay cursor (epoch seconds); null = now */
  clock: number | null = null;
  telemetry: Telemetry | null = null;
  status: Status | null = null;
  connected = false;
  kafka = false;
  latest = new Map<string, Snapshot>();
  seconds = new Map<number, Bucket>();
  decisions: LiveDecision[] = [];
  logs: LogLine[] = [];
  lastRunId: string | null = null;
  version = 0;
  private listeners = new Set<() => void>();
  private ws: WebSocket | null = null;
  private retry = 1000;
  private pending = false;
  private stopped = false;
  private authListeners = new Set<() => void>();

  constructor(opts: { replay?: boolean } = {}) {
    this.replay = Boolean(opts.replay);
  }

  subscribe = (fn: () => void) => {
    this.listeners.add(fn);
    if (this.listeners.size === 1 && !this.ws && !this.replay) this.connect();
    return () => {
      this.listeners.delete(fn);
    };
  };

  getVersion = () => this.version;

  onAuthRequired(fn: () => void) {
    this.authListeners.add(fn);
    return () => this.authListeners.delete(fn);
  }

  /** Replay: take a run's whole recorded telemetry. */
  load(t: Telemetry) {
    this.telemetry = t;
    this.seconds.clear();
    this.latest.clear();
    for (const snaps of Object.values(t.metrics || {})) for (const s of snaps) this.ingest(s, false);
    this.decisions = [...(t.decisions || [])].sort((a, b) => Number(b.ts_us) - Number(a.ts_us));
    this.logs = [...(t.logs || [])].sort((a, b) => a.ts - b.ts);
    this.lastRunId = t.run.run_id;
    this.clock = t.run.measure_started_at ?? (t.run.first_us ? t.run.first_us / 1e6 : null);
    this.version++;
    this.listeners.forEach((f) => f());
  }

  setClock(t: number) {
    this.clock = t;
    this.version++;
    this.listeners.forEach((f) => f());
  }

  private notify() {
    if (this.pending) return;
    this.pending = true;
    setTimeout(() => {
      this.pending = false;
      this.version++;
      this.listeners.forEach((f) => f());
    }, 200);
  }

  connect() {
    this.stopped = false;
    const proto = location.protocol === "https:" ? "wss" : "ws";
    let ws: WebSocket;
    try {
      ws = new WebSocket(`${proto}://${location.host}/api/live`);
    } catch {
      this.scheduleReconnect();
      return;
    }
    this.ws = ws;
    ws.onopen = () => {
      this.connected = true;
      this.retry = 1000;
      this.backfill();
      this.notify();
    };
    ws.onmessage = (e) => {
      try {
        this.handle(JSON.parse(e.data));
      } catch {
        /* ignore malformed frames */
      }
    };
    ws.onclose = (e) => {
      this.connected = false;
      this.ws = null;
      this.notify();
      if (e.code === 4401) {
        this.authListeners.forEach((f) => f());
        return;
      }
      if (!this.stopped) this.scheduleReconnect();
    };
    ws.onerror = () => ws.close();
  }

  reconnect() {
    this.stopped = false;
    if (this.ws) this.ws.close();
    else this.connect();
  }

  private scheduleReconnect() {
    setTimeout(() => this.connect(), this.retry);
    this.retry = Math.min(this.retry * 2, 10000);
  }

  private async backfill() {
    try {
      const h = await request<{ metrics: Record<string, Snapshot[]>; logs: LogLine[]; decisions?: LiveDecision[]; current: Status["current"]; queue: Status["queue"] }>(
        "/api/live/history?seconds=600",
      );
      if (h.decisions?.length && !this.decisions.length) this.decisions = [...h.decisions].reverse().slice(0, 300);
      for (const snaps of Object.values(h.metrics || {})) for (const s of snaps) this.ingest(s, false);
      if (h.logs?.length) {
        const seen = new Set(this.logs.map((l) => `${l.ts}|${l.line}`));
        for (const l of h.logs) if (!seen.has(`${l.ts}|${l.line}`)) this.logs.push(l);
        this.logs.sort((a, b) => a.ts - b.ts);
        if (this.logs.length > 3000) this.logs = this.logs.slice(-3000);
      }
      this.status = { ...(this.status || { queue: [] }), current: h.current, queue: h.queue };
      this.notify();
    } catch {
      /* history is best effort */
    }
  }

  handle(msg: Record<string, unknown>) {
    switch (msg.type) {
      case "metrics":
        this.ingest(msg as unknown as Snapshot, true);
        break;
      case "log":
        this.logs.push({ run_id: String(msg.run_id || ""), line: String(msg.line || ""), ts: Number(msg.ts || Date.now() / 1000) });
        if (this.logs.length > 3000) this.logs.splice(0, this.logs.length - 3000);
        this.notify();
        break;
      case "status": {
        const { type: _t, ...rest } = msg;
        void _t;
        this.status = rest as unknown as Status;
        this.kafka = Boolean((msg as { kafka?: boolean }).kafka);
        const rid = (this.status.active ?? this.status.current)?.run_id;
        if (rid) this.lastRunId = rid;
        this.notify();
        break;
      }
      case "decision":
        this.decisions.unshift(msg as unknown as LiveDecision);
        if (this.decisions.length > 300) this.decisions.length = 300;
        this.notify();
        break;
      default:
        break;
    }
  }

  ingest(raw: Snapshot, notify: boolean) {
    const s: Snapshot = {
      ...raw,
      ts_us: Number(raw.ts_us) || Date.now() * 1000,
      counters: raw.counters || {},
      stages: (raw.stages || []) as StageStat[],
    };
    const key = `${s.source}:${s.instance}`;
    const prev = this.latest.get(key);
    if (!prev || prev.ts_us <= s.ts_us) this.latest.set(key, s);
    if ((s.source === "emitter" || s.source === "collector") && s.run_id) this.lastRunId = s.run_id;
    const sec = Math.round(s.ts_us / 1e6);
    let b = this.seconds.get(sec);
    if (!b) {
      b = new Map();
      this.seconds.set(sec, b);
      if (!this.replay) {
        const cutoff = sec - KEEP_S;
        for (const k of this.seconds.keys()) if (k < cutoff) this.seconds.delete(k);
      }
    }
    b.set(key, s);
    if (notify) this.notify();
  }

  clearDecisions() {
    this.decisions = [];
    this.notify();
  }
}

export const live = new LiveStore();

export function useLiveVersion(store: LiveStore = live): number {
  return useSyncExternalStore(store.subscribe, store.getVersion, store.getVersion);
}

// ------------------------------------------------------------------ derived series
export interface LivePoint {
  t: number; // epoch seconds
  emitted: number | null;
  acked: number | null;
  decided: number | null;
  e2e_p50: number | null;
  e2e_p99: number | null;
  e2e_p999: number | null;
  sched_p99: number | null;
  backlog: number | null;
  degraded: number | null;
  approve: number | null;
  review: number | null;
  decline: number | null;
  challenge: number | null;
  stages: Record<string, number>; // p99 per stage name (collector/gateway/model/flink)
}

const stage = (s: Snapshot, name: string) => s.stages.find((x) => x.name === name);

/** Which run the charts should follow: the running one, else the latest seen. */
export function focusRunId(): string | null {
  return (live.status?.active ?? live.status?.current)?.run_id || live.lastRunId;
}

function runMatch(s: Snapshot, runId: string | null) {
  if (s.source !== "emitter" && s.source !== "collector") return true;
  return !runId || !s.run_id || s.run_id === runId;
}

export const STAGE_SERIES: { key: string; label: string; sources: string[]; names: string[] }[] = [
  { key: "ingress", label: "ingress transit", sources: ["collector"], names: ["ingress"] },
  { key: "queue_wait", label: "queue wait", sources: ["collector"], names: ["queue_wait"] },
  { key: "flink", label: "Flink stages", sources: ["collector"], names: ["shuffle", "state_features", "async_wait"] },
  { key: "feature_lookup", label: "feature lookup", sources: ["collector"], names: ["feature_lookup"] },
  { key: "model_call", label: "model call", sources: ["collector"], names: ["model_call"] },
  { key: "rules", label: "rules + policy", sources: ["collector"], names: ["rules", "policy"] },
  { key: "audit_ack", label: "audit ack", sources: ["gateway", "collector"], names: ["audit_ack"] },
  { key: "egress", label: "egress", sources: ["collector"], names: ["egress"] },
];

export function buildSeries(windowS = 300, runId: string | null = focusRunId(), store: LiveStore = live): LivePoint[] {
  const all = [...store.seconds.keys()].sort((a, b) => a - b);
  const secs = store.clock !== null ? all.filter((x) => x <= store.clock!) : all;
  if (!secs.length) return [];
  const end = secs[secs.length - 1];
  const start = end - windowS;
  const totals = new Map<string, number>(); // running *_total per instance for backlog
  const out: LivePoint[] = [];
  for (const sec of secs) {
    if (sec < start - 5) {
      // still advance running totals so backlog is right at the window start
      const b = store.seconds.get(sec)!;
      for (const [k, s] of b) {
        if (!runMatch(s, runId)) continue;
        if (s.source === "emitter" && s.counters.emitted_total !== undefined) totals.set(k, s.counters.emitted_total);
        if (s.source === "collector" && s.counters.decided_total !== undefined) totals.set(k, s.counters.decided_total);
      }
      continue;
    }
    const b = store.seconds.get(sec)!;
    let emitted: number | null = null, acked: number | null = null, decided: number | null = null;
    let e2e50: number | null = null, e2e99: number | null = null, e2e999: number | null = null, sched: number | null = null;
    let degraded: number | null = null, approve: number | null = null, review: number | null = null, decline: number | null = null;
    let challenge: number | null = null;
    let emitterE2e99: number | null = null, emitterE2e50: number | null = null;
    const stages: Record<string, number> = {};
    const add = (v: number | undefined, cur: number | null) => (v === undefined ? cur : (cur ?? 0) + v);
    const mx = (v: number | undefined, cur: number | null) => (v === undefined ? cur : Math.max(cur ?? 0, v));
    for (const [k, s] of b) {
      if (!runMatch(s, runId)) continue;
      const c = s.counters;
      if (s.source === "emitter") {
        emitted = add(c.emitted, emitted);
        acked = add(c.acked ?? c.responses, acked);
        sched = mx(stage(s, "schedule_lag")?.p99_ms, sched);
        emitterE2e99 = mx(stage(s, "e2e")?.p99_ms, emitterE2e99);
        emitterE2e50 = mx(stage(s, "e2e")?.p50_ms, emitterE2e50);
        if (c.emitted_total !== undefined) totals.set(k, c.emitted_total);
      } else if (s.source === "collector") {
        decided = add(c.decided, decided);
        degraded = add(c.degraded, degraded);
        approve = add(c.approve, approve);
        review = add(c.review, review);
        decline = add(c.decline, decline);
        challenge = add(c.challenge, challenge);
        const e = stage(s, "e2e");
        e2e50 = mx(e?.p50_ms, e2e50);
        e2e99 = mx(e?.p99_ms, e2e99);
        e2e999 = mx(e?.p999_ms, e2e999);
        if (c.decided_total !== undefined) totals.set(k, c.decided_total);
      }
      for (const st of s.stages) {
        const name = s.source === "collector" || s.source === "emitter" ? st.name : `${s.source}.${st.name}`;
        stages[name] = Math.max(stages[name] ?? 0, st.p99_ms);
      }
    }
    if (e2e99 === null && emitterE2e99 !== null) {
      e2e99 = emitterE2e99;
      e2e50 = emitterE2e50;
    }
    let em = 0, de = 0, haveE = false, haveD = false;
    for (const [k, v] of totals) {
      if (k.startsWith("emitter:")) { em += v; haveE = true; }
      if (k.startsWith("collector:")) { de += v; haveD = true; }
    }
    const merged: Record<string, number> = {};
    for (const ser of STAGE_SERIES) {
      let v: number | undefined;
      for (const n of ser.names) {
        const x = stages[n] ?? stages[`gateway.${n}`];
        if (x !== undefined) v = (v ?? 0) + x;
      }
      if (v !== undefined) merged[ser.key] = v;
    }
    out.push({
      t: sec, emitted, acked, decided, e2e_p50: e2e50, e2e_p99: e2e99, e2e_p999: e2e999, sched_p99: sched,
      backlog: haveE && haveD ? Math.max(0, em - de) : null, degraded, approve, review, decline, challenge,
      stages: { ...stages, ...merged },
    });
  }
  return out.filter((p) => p.t >= start);
}

/** Latest snapshots (≤ maxAgeS old) of one source, all instances. */
export function latestOf(source: string, maxAgeS = 4): Snapshot[] {
  const now = Math.max(Date.now() * 1000, ...[...live.latest.values()].map((s) => s.ts_us));
  return [...live.latest.values()].filter((s) => s.source === source && now - s.ts_us <= maxAgeS * 1e6);
}

export function logsFor(runId: string | null, store: LiveStore = live): LogLine[] {
  const upTo = store.clock;
  const logs = upTo !== null ? store.logs.filter((l) => l.ts <= upTo) : store.logs;
  if (!runId) return logs.slice(-500);
  return logs.filter((l) => !l.run_id || l.run_id === runId).slice(-800);
}

/** The newest snapshot of every instance at most `maxAgeS` old at the store's clock (now when live). */
export function snapshotsAt(store: LiveStore, runId: string | null, maxAgeS = 5): Snapshot[] {
  const keep = (s: Snapshot) => (s.source !== "emitter" && s.source !== "collector") || !runId || s.run_id === runId;
  if (store.clock === null) {
    const all = [...store.latest.values()];
    const now = Math.max(Date.now() * 1000, ...all.map((s) => s.ts_us));
    return all.filter((s) => now - s.ts_us <= maxAgeS * 1e6 && keep(s));
  }
  const out = new Map<string, Snapshot>();
  const end = Math.floor(store.clock);
  for (let sec = end; sec > end - maxAgeS; sec--) {
    const b = store.seconds.get(sec);
    if (!b) continue;
    for (const [k, s] of b) if (!out.has(k) && keep(s)) out.set(k, s);
  }
  return [...out.values()];
}

/** Decisions seen up to the store's clock, newest first. */
export function decisionsAt(store: LiveStore): LiveDecision[] {
  if (store.clock === null) return store.decisions;
  const upTo = store.clock * 1e6;
  return store.decisions.filter((d) => Number(d.ts_us) <= upTo);
}
