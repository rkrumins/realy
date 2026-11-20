/**
 * Live store: one WebSocket to /api/live (status | metrics | log messages), backfilled from
 * /api/live/history on every (re)connect, reconnecting with exponential backoff. React reads it through
 * useSyncExternalStore; notifications are throttled to ~5/s so a burst of messages never thrashes
 * rendering. While the socket is down, hooks.ts falls back to polling /api/status.
 */
import { useSyncExternalStore } from "react";
import { request } from "./api";
import type { LogLine, MetricSample, Status } from "./types";

const KEEP_S = 900; // seconds of metrics kept in memory
const MAX_LOGS = 3000;

class LiveStore {
  status: Status | null = null;
  connected = false;
  /** epoch seconds of the last frame received (any type): drives "stale" indicators */
  lastFrameAt = 0;
  samples = new Map<string, MetricSample>(); // key = job:second (dedupes backfill vs live frames)
  logs: LogLine[] = [];
  lastJobId: string | null = null;
  version = 0;
  private listeners = new Set<() => void>();
  private authListeners = new Set<() => void>();
  private ws: WebSocket | null = null;
  private retry = 1000;
  private pending = false;
  private stopped = false;

  subscribe = (fn: () => void) => {
    this.listeners.add(fn);
    if (this.listeners.size === 1 && !this.ws) this.connect();
    return () => {
      this.listeners.delete(fn);
    };
  };

  getVersion = () => this.version;

  /** The backend closes the socket with 4401 when a token is required and missing. */
  onAuthRequired(fn: () => void) {
    this.authListeners.add(fn);
    return () => this.authListeners.delete(fn);
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
    if (typeof window === "undefined" || typeof WebSocket === "undefined") return;
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
      void this.backfill();
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
    this.retry = Math.min(this.retry * 2, 10_000);
  }

  private async backfill() {
    try {
      const h = await request<{ metrics: MetricSample[]; logs: LogLine[] } & Status>(`/api/live/history?seconds=${KEEP_S}`);
      for (const m of h.metrics || []) this.ingest(m);
      if (h.logs?.length) {
        const seen = new Set(this.logs.map((l) => `${l.ts}|${l.line}`));
        for (const l of h.logs) if (!seen.has(`${l.ts}|${l.line}`)) this.logs.push(l);
        this.logs.sort((a, b) => a.ts - b.ts);
        if (this.logs.length > MAX_LOGS) this.logs = this.logs.slice(-MAX_LOGS);
      }
      this.status = { ...(this.status ?? { queue: [] }), current: h.current, queue: h.queue };
      this.notify();
    } catch {
      /* history is best effort */
    }
  }

  handle(msg: Record<string, unknown>) {
    this.lastFrameAt = Date.now() / 1000;
    switch (msg.type) {
      case "status": {
        const { type: _t, ...rest } = msg;
        void _t;
        this.status = rest as unknown as Status;
        if (this.status.current?.id) this.lastJobId = this.status.current.id;
        break;
      }
      case "metrics":
        this.ingest(msg as unknown as MetricSample);
        break;
      case "log":
        this.logs.push({ job_id: String(msg.job_id || ""), line: String(msg.line || ""), ts: Number(msg.ts || Date.now() / 1000) });
        if (this.logs.length > MAX_LOGS) this.logs.splice(0, this.logs.length - MAX_LOGS);
        break;
      default:
        return;
    }
    this.notify();
  }

  ingest(m: MetricSample) {
    const ts = Number(m.ts) || Date.now() / 1000;
    this.samples.set(`${m.job_id}:${Math.round(ts)}`, { ...m, ts });
    if (m.job_id) this.lastJobId = m.job_id;
    const cutoff = ts - KEEP_S;
    if (this.samples.size > KEEP_S * 2) for (const [k, s] of this.samples) if (s.ts < cutoff) this.samples.delete(k);
  }
}

export const live = new LiveStore();

export function useLiveVersion(): number {
  return useSyncExternalStore(live.subscribe, live.getVersion, live.getVersion);
}

// ------------------------------------------------------------------ derived series
export interface LivePoint {
  t: number; // epoch seconds
  throughput: number | null;
  p50_ms: number | null;
  p99_ms: number | null;
  errors: number | null;
}

/** Which job the charts follow: the running one, else the last one seen. */
export function focusJobId(): string | null {
  return live.status?.current?.id || live.lastJobId;
}

/** Per-second points of one job for the last `windowS` seconds of data (not of wall time). */
export function buildSeries(windowS = 300, jobId: string | null = focusJobId()): LivePoint[] {
  const pts = [...live.samples.values()]
    .filter((s) => !jobId || s.job_id === jobId)
    .sort((a, b) => a.ts - b.ts);
  if (!pts.length) return [];
  const end = pts[pts.length - 1].ts;
  return pts
    .filter((s) => s.ts >= end - windowS)
    .map((s) => ({ t: Math.round(s.ts), throughput: s.throughput, p50_ms: s.p50_ms, p99_ms: s.p99_ms, errors: s.errors }));
}

export function logsFor(jobId: string | null): LogLine[] {
  if (!jobId) return live.logs.slice(-500);
  return live.logs.filter((l) => !l.job_id || l.job_id === jobId).slice(-800);
}
