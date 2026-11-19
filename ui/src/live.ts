import { useEffect, useSyncExternalStore } from "react";
import { api, Current, LogMsg, Snapshot } from "./api";

// One shared WebSocket + ring buffers for the whole app (pages subscribe to the same store).
const WINDOW_S = 300;

type State = {
  connected: boolean;
  kafka: boolean;
  current: Current;
  queueLen: number;
  rings: Map<string, Snapshot[]>;
  logs: LogMsg[];
  version: number;
};

let state: State = { connected: false, kafka: false, current: null, queueLen: 0, rings: new Map(), logs: [], version: 0 };
const listeners = new Set<() => void>();
let ws: WebSocket | null = null;
let started = false;
let bumpPending = false;

function bump() {
  if (bumpPending) return;
  bumpPending = true;
  // coalesce bursts (many sources report within the same second)
  setTimeout(() => {
    bumpPending = false;
    state = { ...state, version: state.version + 1 };
    listeners.forEach((l) => l());
  }, 250);
}

function addSnapshot(s: Snapshot) {
  const key = `${s.source}:${s.instance}`;
  let ring = state.rings.get(key);
  if (!ring) {
    ring = [];
    state.rings.set(key, ring);
  }
  ring.push(s);
  const cutoff = (Date.now() / 1000 - WINDOW_S) * 1e6;
  while (ring.length && ring[0].ts_us < cutoff) ring.shift();
}

function connect() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/api/live`);
  ws.onopen = () => {
    state.connected = true;
    bump();
  };
  ws.onclose = () => {
    state.connected = false;
    bump();
    setTimeout(connect, 2000);
  };
  ws.onmessage = (ev) => {
    const m = JSON.parse(ev.data);
    if (m.type === "metrics") addSnapshot(m as Snapshot);
    else if (m.type === "log") {
      state.logs.push(m as LogMsg);
      if (state.logs.length > 2000) state.logs.splice(0, state.logs.length - 2000);
    } else if (m.type === "status") {
      const prev = state.current?.run_id;
      state.current = m.current;
      state.queueLen = (m.queue || []).length;
      state.kafka = !!m.kafka;
      if (m.current?.run_id && m.current.run_id !== prev) {
        // new run: drop logs of older runs from the live view
        state.logs = state.logs.filter((l) => l.run_id === m.current.run_id);
      }
    }
    bump();
  };
}

async function start() {
  if (started) return;
  started = true;
  try {
    const h = await api.history();
    Object.values(h.metrics).forEach((arr) => arr.forEach(addSnapshot));
    state.logs = h.logs || [];
    state.current = h.current;
  } catch {
    /* console may be starting */
  }
  connect();
  bump();
}

export function useLive(): State {
  useEffect(() => {
    start();
  }, []);
  return useSyncExternalStore(
    (l) => {
      listeners.add(l);
      return () => listeners.delete(l);
    },
    () => state,
  );
}

// ------------------------------------------------------------------ per-second aggregation
export type Series = { xs: number[]; get: (name: string) => (number | null)[] };

const stageOf = (s: Snapshot, name: string) => s.stages.find((x) => x.name === name);

/** Align all sources on whole seconds and derive the live view's series. */
export function aggregate(rings: Map<string, Snapshot[]>, runId?: string | null): Series {
  const bySec = new Map<number, Snapshot[]>();
  let minS = Infinity;
  let maxS = -Infinity;
  rings.forEach((arr) =>
    arr.forEach((s) => {
      if (runId && s.run_id && s.run_id !== runId) return;
      const sec = Math.floor(s.ts_us / 1e6);
      minS = Math.min(minS, sec);
      maxS = Math.max(maxS, sec);
      let b = bySec.get(sec);
      if (!b) bySec.set(sec, (b = []));
      b.push(s);
    }),
  );
  const xs: number[] = [];
  if (isFinite(minS)) for (let t = Math.max(minS, maxS - WINDOW_S); t <= maxS; t++) xs.push(t);
  const cache = new Map<string, (number | null)[]>();
  const lastTotals = { emitted: new Map<string, number>(), decided: new Map<string, number>() };

  const sum = (snaps: Snapshot[], source: string, counter: string) => {
    let v = 0;
    let any = false;
    for (const s of snaps) if (s.source === source && counter in s.counters) { v += s.counters[counter]; any = true; }
    return any ? v : null;
  };
  const maxStage = (snaps: Snapshot[], sources: string[], name: string, q: "p50_ms" | "p99_ms") => {
    let v: number | null = null;
    for (const s of snaps) {
      if (!sources.includes(s.source)) continue;
      const st = stageOf(s, name);
      if (st && st.count > 0) v = v === null ? st[q] : Math.max(v, st[q]);
    }
    return v;
  };

  const compute = (name: string): (number | null)[] => {
    return xs.map((t) => {
      const snaps = bySec.get(t) || [];
      switch (name) {
        case "emitted": return sum(snaps, "emitter", "emitted");
        case "decided": {
          const c = sum(snaps, "collector", "decided");
          return c !== null ? c : sum(snaps, "emitter", "responses");
        }
        case "acked": return sum(snaps, "emitter", "acked");
        case "gateway_decisions": return sum(snaps, "gateway", "decisions");
        case "flink_decisions": return sum(snaps, "flink", "decisions");
        case "degraded": return sum(snaps, "collector", "degraded");
        case "backlog": {
          for (const s of snaps) {
            if (s.source === "emitter" && "emitted_total" in s.counters) lastTotals.emitted.set(s.instance, s.counters.emitted_total);
            if (s.source === "collector" && "decided_total" in s.counters) lastTotals.decided.set(s.instance, s.counters.decided_total);
          }
          if (!lastTotals.emitted.size) return null;
          let e = 0, d = 0;
          lastTotals.emitted.forEach((v) => (e += v));
          lastTotals.decided.forEach((v) => (d += v));
          return Math.max(0, e - d);
        }
        default: {
          // "<stage>:<q>" e.g. "e2e:p99", "feature_lookup:p99"
          const [stage, q] = name.split(":");
          const qq = (q === "p50" ? "p50_ms" : "p99_ms") as "p50_ms" | "p99_ms";
          if (stage === "e2e") {
            const c = maxStage(snaps, ["collector"], "e2e", qq);
            return c !== null ? c : maxStage(snaps, ["emitter"], "e2e", qq);
          }
          if (stage === "schedule_lag" || stage === "produce_ack") return maxStage(snaps, ["emitter"], stage, qq);
          if (stage === "audit_ack") return maxStage(snaps, ["gateway"], stage, qq);
          if (stage.startsWith("model_server")) return maxStage(snaps, ["model"], stage, qq);
          return maxStage(snaps, ["collector", "gateway", "flink"], stage, qq);
        }
      }
    });
  };
  return {
    xs,
    get: (name: string) => {
      let v = cache.get(name);
      if (!v) cache.set(name, (v = compute(name)));
      return v;
    },
  };
}

export function lastValue(arr: (number | null)[], skip = 1): number | null {
  for (let i = arr.length - 1 - skip; i >= 0; i--) if (arr[i] !== null) return arr[i];
  for (let i = arr.length - 1; i >= 0; i--) if (arr[i] !== null) return arr[i];
  return null;
}
