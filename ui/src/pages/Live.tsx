import { useEffect, useMemo, useRef, useState } from "react";
import { api, fmt, fmtInt } from "../api";
import Chart from "../components/Chart";
import { aggregate, lastValue, useLive } from "../live";

const STAGES: [string, string][] = [
  ["schedule_lag:p99", "on-time publish"],
  ["ingress:p99", "ingress transit"],
  ["feature_lookup:p99", "feature lookup"],
  ["model_call:p99", "model call"],
  ["audit_ack:p99", "audit ack"],
  ["hot_path:p99", "hot path"],
  ["egress:p99", "egress"],
  ["model_server_queue:p99", "model queue"],
];

function Kpi({ label, value, sub, gauge }: { label: string; value: string; sub?: string; gauge?: { frac: number; bad: boolean } }) {
  return (
    <div className="kpi">
      <div className="l">{label}</div>
      <div className="v">{value}</div>
      {sub && <div className="s">{sub}</div>}
      {gauge && (
        <div className="gauge"><i style={{ width: `${Math.min(100, gauge.frac * 100)}%`, background: gauge.bad ? "var(--bad)" : "var(--good)" }} /></div>
      )}
    </div>
  );
}

export default function Live() {
  const live = useLive();
  const cur = live.current;
  const series = useMemo(() => aggregate(live.rings, cur?.run_id), [live.version, cur?.run_id]);
  const xs = series.xs;
  const budget = cur?.budget?.e2e_p99_ms ?? 150;

  const emitted = series.get("emitted");
  const decided = series.get("decided");
  const p99 = series.get("e2e:p99");
  const lag = series.get("schedule_lag:p99");
  const backlog = series.get("backlog");

  // ---------------------------------------------------------------- controls
  const base = cur?.base_tps ?? 500;
  const [tps, setTps] = useState<number>(base);
  const touched = useRef(false);
  useEffect(() => {
    if (!touched.current && cur) setTps(Math.round((cur.base_tps ?? 500) * (cur.rate_multiplier ?? 1)));
  }, [cur?.run_id, cur?.rate_multiplier]);
  const [msg, setMsg] = useState("");
  const [model, setModel] = useState({ median: "8", p99: "25", workers: "64", max_batch: "1" });
  const [feat, setFeat] = useState({ median: "2", p99: "8" });

  const act = async (f: () => Promise<unknown>, label: string) => {
    try {
      const r = await f();
      setMsg(`${label}: ${JSON.stringify(r)}`);
    } catch (e) {
      setMsg(`${label} failed: ${e}`);
    }
  };
  const logRef = useRef<HTMLDivElement>(null);
  const logs = live.logs.filter((l) => !cur || l.run_id === cur.run_id || l.run_id === "");
  useEffect(() => {
    const el = logRef.current;
    if (el && el.scrollHeight - el.scrollTop - el.clientHeight < 80) el.scrollTop = el.scrollHeight;
  }, [logs.length]);

  const elapsed = cur?.started_at ? Date.now() / 1000 - cur.started_at : 0;
  const lagNow = lastValue(lag);

  return (
    <div className="grid">
      <section className="panel">
        <div className="head">
          <h2>{cur ? <>Live · <span className="mono">{cur.run_id}</span></> : "Live telemetry"}</h2>
          {cur && <span className={`badge ${cur.phase}`}>{cur.phase}</span>}
          <span className="sp" />
          {cur && <span className="muted">{cur.topology} · budget e2e p99 ≤ {fmtInt(budget)} ms · {fmtInt(elapsed)} s / {fmtInt((cur.duration_s ?? 0) + 0)} s</span>}
          {cur && <button className="danger" onClick={() => act(() => api.abort(cur.run_id), "abort")}>Abort run</button>}
        </div>
        {!cur && <div className="muted">No run in progress — charts show whatever the services are still reporting on bench.metrics. Start one from Runs.</div>}
        <div className="kpis">
          <Kpi label="emitted / s" value={fmtInt(lastValue(emitted))} sub={cur ? `target ${fmtInt(base * (cur.rate_multiplier ?? 1))}` : undefined} />
          <Kpi label="decided / s" value={fmtInt(lastValue(decided))} />
          <Kpi label="e2e p99 (1 s)" value={`${fmt(lastValue(p99))} ms`} sub={`p50 ${fmt(lastValue(series.get("e2e:p50")))} ms`}
            gauge={{ frac: (lastValue(p99) ?? 0) / budget, bad: (lastValue(p99) ?? 0) > budget }} />
          <Kpi label="on-time publish p99" value={`${fmt(lagNow, 2)} ms`} sub="sent − intended"
            gauge={{ frac: (lagNow ?? 0) / 50, bad: (lagNow ?? 0) > 50 }} />
          <Kpi label="backlog" value={fmtInt(lastValue(backlog))} sub="emitted − decided" />
          <Kpi label="degraded / s" value={fmtInt(lastValue(series.get("degraded")))} sub="rules-only fallbacks" />
        </div>
      </section>

      <div className="grid g2">
        <section className="panel">
          <Chart title="Throughput funnel" unit="events/s" data={[xs, emitted, series.get("acked"), decided]}
            series={[{ label: "emitted" }, { label: "acked" }, { label: "decided" }]} />
        </section>
        <section className="panel">
          <Chart title="End-to-end latency" unit="ms" data={[xs, series.get("e2e:p50"), p99]}
            series={[{ label: "p50" }, { label: "p99" }]} hline={{ value: budget, label: `budget ${budget} ms` }} />
        </section>
        <section className="panel">
          <Chart title="Per-stage p99" unit="ms" data={[xs, ...STAGES.map(([k]) => series.get(k))]}
            series={STAGES.map(([, l]) => ({ label: l }))} />
        </section>
        <section className="panel">
          <Chart title="Backlog and on-time publishing" unit="events · ms" data={[xs, backlog, lag]}
            series={[{ label: "backlog (events)" }, { label: "publish lag p99 (ms)" }]} />
        </section>
      </div>

      <div className="grid g3">
        <section className="panel">
          <div className="head"><h2>Rate</h2><span className="sp" /><span className="muted">RateCommand → emitters</span></div>
          <div className="row">
            <input type="range" min={Math.max(1, Math.round(base * 0.1))} max={Math.round(base * 3)} value={tps} style={{ flex: 1 }}
              onChange={(e) => { touched.current = true; setTps(+e.target.value); }} />
            <input type="number" value={tps} onChange={(e) => { touched.current = true; setTps(+e.target.value); }} />
            <button className="primary" disabled={!cur} onClick={() => act(() => api.rate(tps), `rate → ${tps} TPS`)}>Apply</button>
          </div>
          <div className="muted" style={{ marginTop: 6 }}>base {fmtInt(base)} TPS · multiplier now ×{fmt(cur?.rate_multiplier ?? 1, 2)}</div>
        </section>
        <section className="panel">
          <div className="head"><h2>Model server</h2><span className="sp" /><span className="muted">service time + capacity</span></div>
          <div className="row">
            <label className="f">median ms<input type="number" value={model.median} onChange={(e) => setModel({ ...model, median: e.target.value })} /></label>
            <label className="f">p99 ms<input type="number" value={model.p99} onChange={(e) => setModel({ ...model, p99: e.target.value })} /></label>
            <label className="f">workers<input type="number" value={model.workers} onChange={(e) => setModel({ ...model, workers: e.target.value })} /></label>
            <label className="f">max batch<input type="number" value={model.max_batch} onChange={(e) => setModel({ ...model, max_batch: e.target.value })} /></label>
            <button onClick={() => act(() => api.model({
              service_time: { lognormal: { median_ms: +model.median, p99_ms: +model.p99 } },
              workers: +model.workers, max_batch: +model.max_batch, max_queue_delay_us: 1000,
            }), "model config")}>Apply</button>
          </div>
        </section>
        <section className="panel">
          <div className="head"><h2>Feature lookup delay</h2><span className="sp" /><span className="muted">injected on top of Redis</span></div>
          <div className="row">
            <label className="f">median ms<input type="number" value={feat.median} onChange={(e) => setFeat({ ...feat, median: e.target.value })} /></label>
            <label className="f">p99 ms<input type="number" value={feat.p99} onChange={(e) => setFeat({ ...feat, p99: e.target.value })} /></label>
            <button onClick={() => act(() => api.scoring({ feature_lookup_delay: { lognormal: { median_ms: +feat.median, p99_ms: +feat.p99 } } }),
              "feature delay")}>Apply</button>
          </div>
        </section>
      </div>
      {msg && <div className="muted mono">{msg}</div>}

      <section className="panel">
        <div className="head"><h2>Run log</h2></div>
        <div className="log" ref={logRef}>
          {logs.length ? logs.map((l, i) => <div key={i}>{l.line}</div>) : <span className="muted">no log lines yet</span>}
        </div>
      </section>
    </div>
  );
}
