import { useEffect, useMemo, useState } from "react";
import { api, CompareResult, fmt, fmtInt, pct, RunDetail, RunSummary } from "../api";
import { go, Route } from "../App";
import Chart from "../components/Chart";

const STEP_LABEL: Record<string, string> = {
  schedule_lag: "1 on-time publish (sent − intended)", produce_ack: "2 produce ack (broker write)",
  ingress: "3 ingress transit + pickup", queue_wait: "service queue wait", deserialize: "deserialize",
  shuffle: "4 Flink keyBy shuffles", state_features: "4 Flink keyed state", async_wait: "4 Flink async I/O wait",
  feature_lookup: "5 feature lookup", feature_assemble: "5 feature assemble", model_call: "5 model call",
  model_batch_wait: "6 model micro-batch wait", model_server_queue: "6 model server queue",
  model_server_compute: "6 model server compute", rules: "5 rules (CEL)", policy: "5 policy",
  audit_ack: "5 audit ack (Kafka)", hot_path: "= hot path", egress: "7 egress / response", e2e: "8 END-TO-END",
};
const ORDER = Object.keys(STEP_LABEL);

function Summary({ d }: { d: RunDetail }) {
  const s = d.report?.summary || {};
  return (
    <section className="panel">
      <div className="head">
        <h2 className="mono">{d.run_id}</h2>
        {s.verdict && <span className={`badge ${s.verdict}`}>{s.verdict}</span>}
        <span className="sp" />
        <span className="muted">{d.report?.topology} · budget e2e p99 ≤ {d.report?.budget?.e2e_p99_ms} ms, hot ≤ {d.report?.budget?.hot_path_p99_ms} ms</span>
        {d.summary.has_html && <a href={`/api/runs/${d.run_id}/report.html`} target="_blank">report.html ↗</a>}
      </div>
      {s.reasons?.length > 0 && <ul className="err" style={{ margin: "0 0 8px" }}>{s.reasons.map((r: string, i: number) => <li key={i}>{r}</li>)}</ul>}
      <div className="kpis">
        <div className="kpi"><div className="l">emitted / decided</div><div className="v">{fmtInt(s.emitted)}</div><div className="s">{fmtInt(s.decided)} decided · {fmtInt(s.missing)} missing · {fmtInt(s.duplicates)} dups</div></div>
        <div className="kpi"><div className="l">achieved TPS</div><div className="v">{fmtInt(s.achieved_emit_tps)}</div></div>
        <div className="kpi"><div className="l">e2e p50 / p99</div><div className="v">{fmt(s.e2e_p99_ms)} ms</div><div className="s">p50 {fmt(s.e2e_p50_ms)} · p99.9 {fmt(s.e2e_p999_ms)}</div></div>
        <div className="kpi"><div className="l">hot path p99</div><div className="v">{fmt(s.hot_path_p99_ms)} ms</div></div>
        <div className="kpi"><div className="l">within budget</div><div className="v">{pct(s.within_budget)}</div></div>
        <div className="kpi"><div className="l">on-time publish p99</div><div className="v">{fmt(s.schedule_lag_p99_ms, 2)} ms</div></div>
        <div className="kpi"><div className="l">degraded</div><div className="v">{pct(s.degraded_fraction)}</div></div>
        <div className="kpi"><div className="l">online-store lag p99</div><div className="v">{fmt(s.online_store_update_lag_p99_ms)} ms</div></div>
      </div>
    </section>
  );
}

function Detail({ d }: { d: RunDetail }) {
  const r = d.report;
  const p99 = r?.e2e_p99_by_second || [];
  const xs = useMemo(() => p99.map((_, i) => i), [p99.length]);
  if (!r) return <section className="panel empty">Report not available yet.</section>;
  const tail = r.tail_attribution;
  const det = r.detection || {};
  const v = d.verify;
  return (
    <>
      <Summary d={d} />
      <div className="grid g2">
        <section className="panel">
          <div className="head"><h2>Latency by step</h2><span className="sp" /><span className="muted">measured window, ms</span></div>
          <div className="tw"><table>
            <thead><tr><th>step</th><th className="num">p50</th><th className="num">p90</th><th className="num">p99</th><th className="num">p99.9</th><th className="num">max</th></tr></thead>
            <tbody>{ORDER.filter((k) => r.stages[k]?.count).map((k) => {
              const st = r.stages[k];
              return <tr key={k} style={k === "e2e" ? { fontWeight: 600 } : undefined}><td>{STEP_LABEL[k]}</td>
                <td className="num">{fmt(st.p50, 2)}</td><td className="num">{fmt(st.p90, 2)}</td><td className="num">{fmt(st.p99, 2)}</td>
                <td className="num">{fmt(st.p999, 2)}</td><td className="num">{fmt(st.max, 2)}</td></tr>;
            })}</tbody>
          </table></div>
        </section>
        <section className="panel">
          <Chart title="e2e latency per second" unit="ms (by intended time)" xTime={false}
            data={[xs, r.e2e_p50_by_second || [], p99]} series={[{ label: "p50" }, { label: "p99" }]}
            hline={{ value: r.budget.e2e_p99_ms, label: `budget ${r.budget.e2e_p99_ms} ms` }} />
          {r.funnel && (
            <Chart title="Throughput funnel" unit="events/s" xTime={false} height={150}
              data={[(r.funnel.emitted || []).map((_, i) => i), r.funnel.emitted || [], r.funnel.decided || [], r.funnel.collected || r.funnel.responded || []]}
              series={[{ label: "emitted" }, { label: "decided" }, { label: r.funnel.collected ? "collected" : "responded" }]} />
          )}
        </section>
      </div>

      {r.levels?.length > 0 && (
        <section className="panel">
          <div className="head"><h2>Latency vs throughput</h2>{r.summary.knee && <span className="badge FAIL">knee at {fmtInt(r.summary.knee.target_tps)} TPS</span>}</div>
          <div className="grid g2">
            <div className="tw"><table>
              <thead><tr><th className="num">target</th><th className="num">emitted</th><th className="num">decided</th><th className="num">e2e p50</th><th className="num">e2e p99</th>
                <th className="num">hot p99</th><th className="num">feature p99</th><th className="num">model p99</th><th className="num">in budget</th></tr></thead>
              <tbody>{r.levels.map((l) => (
                <tr key={l.level}><td className="num">{fmtInt(l.target_tps)}</td><td className="num">{fmtInt(l.emitted_tps)}</td><td className="num">{fmtInt(l.decided_tps)}</td>
                  <td className="num">{fmt(l.stages.e2e?.p50)}</td><td className="num">{fmt(l.stages.e2e?.p99)}</td><td className="num">{fmt(l.stages.hot_path?.p99)}</td>
                  <td className="num">{fmt(l.stages.feature_lookup?.p99)}</td><td className="num">{fmt(l.stages.model_call?.p99)}</td><td className="num">{pct(l.within_budget)}</td></tr>
              ))}</tbody>
            </table></div>
            <Chart title="p99 by TPS level" unit="ms" xTime={false} height={200}
              data={[r.levels.map((l) => l.target_tps), r.levels.map((l) => l.stages.e2e?.p99 ?? null), r.levels.map((l) => l.stages.hot_path?.p99 ?? null),
                r.levels.map((l) => l.stages.model_call?.p99 ?? null)]}
              series={[{ label: "e2e" }, { label: "hot path" }, { label: "model" }]} hline={{ value: r.budget.e2e_p99_ms, label: "budget" }} />
          </div>
        </section>
      )}

      <div className="grid g2">
        {tail && (
          <section className="panel">
            <div className="head"><h2>Tail attribution</h2><span className="sp" /><span className="muted">events ≥ p99 ({fmt(tail.threshold_ms)} ms), dominated by {tail.dominant}</span></div>
            <table><tbody>{Object.entries(tail.share).sort((a, b) => b[1] - a[1]).map(([k, v]) => (
              <tr key={k}><td>{k}</td><td className="num">{fmt(tail.mean_ms_by_stage[k])} ms</td>
                <td style={{ width: "50%" }}><div className="bar"><i style={{ width: `${v * 100}%` }} /></div></td><td className="num">{pct(v, 0)}</td></tr>
            ))}</tbody></table>
          </section>
        )}
        <section className="panel">
          <div className="head"><h2>Verification</h2>{v && <span className={`badge ${v.passed ? "PASS" : "FAIL"}`}>{v.passed ? "PASS" : "FAIL"}</span>}</div>
          {v ? (
            <table><tbody>
              <tr><td>features vs reference engine</td><td className="num">{pct(v.features?.exact_set_agreement, 4)}</td><td className="muted wrap">{v.features?.note}</td></tr>
              <tr><td>CEL rule parity (Java vs Python)</td><td className="num">{pct(v.rules?.parity, 4)}</td><td className="muted">{fmtInt(v.rules?.checked)} decisions</td></tr>
              <tr><td>score parity (proxy model)</td><td className="num">{pct(v.score?.parity, 4)}</td><td className="muted">{fmtInt(v.score?.checked)}</td></tr>
              <tr><td>outcome parity</td><td className="num">{pct(v.outcome?.parity, 4)}</td><td /></tr>
              {v.audit && !v.audit.error && <>
                <tr><td>ClickHouse audit completeness</td><td className="num">{fmtInt(v.audit.audited_unique)} / {fmtInt(v.audit.emitted)}</td><td className="muted">gaps {fmtInt(v.audit.audit_gaps)}</td></tr>
                <tr><td>audit visibility (Kafka → ClickHouse)</td><td className="num">{fmt(v.audit.visibility_p99_ms, 0)} ms p99</td><td className="muted">p50 {fmt(v.audit.visibility_p50_ms, 0)} ms</td></tr>
                <tr><td>replay from stored inputs</td><td className="num">{pct(v.audit.replay_score_parity)}</td><td className="muted">{fmtInt(v.audit.replay_checked)} sampled</td></tr>
              </>}
              {v.audit?.error && <tr><td colSpan={3} className="muted">{v.audit.error}</td></tr>}
            </tbody></table>
          ) : <div className="muted">Not verified. Queue the run with “verify” or run <span className="mono">realy verify runs/{d.run_id}</span>.</div>}
        </section>
      </div>

      {det.fraud_events > 0 && (
        <div className="grid g2">
          <section className="panel">
            <div className="head"><h2>Detection by typology</h2><span className="sp" />
              <span className="muted">recall {fmt(det.recall_flagged, 2)} · precision {fmt(det.precision_flagged, 2)} · FPR {pct(det.false_positive_rate)}</span></div>
            <div className="tw"><table>
              <thead><tr><th>typology</th><th className="num">events</th><th className="num">campaigns</th><th className="num">flagged</th><th className="num">declined</th><th className="num">approved before 1st decline</th><th className="num">$ approved</th></tr></thead>
              <tbody>{Object.entries(det.by_type || {}).map(([k, x]: [string, any]) => (
                <tr key={k}><td>{k}</td><td className="num">{fmtInt(x.events)}</td><td className="num">{fmtInt(x.campaigns)}</td><td className="num">{pct(x.recall_flagged, 0)}</td>
                  <td className="num">{pct(x.recall_declined, 0)}</td><td className="num">{fmt(x.approved_before_first_decline_mean)}</td><td className="num">{fmtInt(x.usd_approved)}</td></tr>
              ))}</tbody>
            </table></div>
          </section>
          <section className="panel">
            <div className="head"><h2>Rules and thresholds</h2><span className="sp" /><span className="muted">sorted by legit hits (false-positive drivers first)</span></div>
            <div className="tw"><table>
              <thead><tr><th>rule / reason</th><th className="num">hits</th><th className="num">fraud</th><th className="num">legit</th><th className="num">precision</th><th className="num">legit hit rate</th></tr></thead>
              <tbody>{Object.entries(det.by_rule || {}).map(([k, x]: [string, any]) => (
                <tr key={k}><td className="mono">{k}</td><td className="num">{fmtInt(x.hits)}</td><td className="num">{fmtInt(x.fraud)}</td><td className="num">{fmtInt(x.legit)}</td>
                  <td className="num">{fmt(x.precision, 2)}</td><td className="num">{pct(x.legit_hit_rate)}</td></tr>
              ))}</tbody>
            </table></div>
          </section>
        </div>
      )}
      {d.logs.length > 0 && (
        <section className="panel"><div className="head"><h2>Run log</h2></div><div className="log">{d.logs.join("\n")}</div></section>
      )}
    </>
  );
}

function Compare({ a, b }: { a: string; b: string }) {
  const [c, setC] = useState<CompareResult | null>(null);
  const [err, setErr] = useState("");
  useEffect(() => {
    setC(null);
    api.compare(a, b).then(setC).catch((e) => setErr(String(e)));
  }, [a, b]);
  if (err) return <div className="err">{err}</div>;
  if (!c) return <div className="muted">loading…</div>;
  const row = (label: string, k: keyof RunSummary, d = 1) => {
    const va = c.a[k] as number | undefined, vb = c.b[k] as number | undefined;
    const delta = typeof va === "number" && typeof vb === "number" ? vb - va : null;
    return <tr key={label}><td>{label}</td><td className="num">{fmt(va, d)}</td><td className="num">{fmt(vb, d)}</td>
      <td className="num" style={{ color: delta === null ? undefined : delta > 0 ? "var(--bad)" : "var(--good)" }}>{delta === null ? "–" : `${delta > 0 ? "+" : ""}${fmt(delta, d)}`}</td></tr>;
  };
  return (
    <div className="grid g2">
      <section className="panel">
        <div className="head"><h2>Summary</h2></div>
        <table>
          <thead><tr><th /><th className="num mono">A {c.a.run_id}</th><th className="num mono">B {c.b.run_id}</th><th className="num">B − A</th></tr></thead>
          <tbody>
            <tr><td>verdict</td><td className="num"><span className={`badge ${c.a.verdict}`}>{c.a.verdict}</span></td><td className="num"><span className={`badge ${c.b.verdict}`}>{c.b.verdict}</span></td><td /></tr>
            <tr><td>topology</td><td className="num">{c.a.topology}</td><td className="num">{c.b.topology}</td><td /></tr>
            {row("achieved TPS", "achieved_tps", 0)}{row("e2e p50 ms", "e2e_p50_ms")}{row("e2e p99 ms", "e2e_p99_ms")}
            {row("hot path p99 ms", "hot_path_p99_ms")}{row("missing", "missing", 0)}{row("duplicates", "duplicates", 0)}
          </tbody>
        </table>
      </section>
      <section className="panel">
        <div className="head"><h2>Per-stage p50 / p99 (ms)</h2></div>
        <div className="tw"><table>
          <thead><tr><th>stage</th><th className="num">A p50</th><th className="num">B p50</th><th className="num">A p99</th><th className="num">B p99</th><th className="num">Δ p99</th></tr></thead>
          <tbody>{ORDER.filter((k) => c.stages[k]).map((k) => {
            const s = c.stages[k];
            const d = s.d_p99 as number | null;
            return <tr key={k}><td>{STEP_LABEL[k]}</td><td className="num">{fmt(s.a_p50, 2)}</td><td className="num">{fmt(s.b_p50, 2)}</td>
              <td className="num">{fmt(s.a_p99, 2)}</td><td className="num">{fmt(s.b_p99, 2)}</td>
              <td className="num" style={{ color: d === null ? undefined : d > 0 ? "var(--bad)" : "var(--good)" }}>{d === null ? "–" : `${d > 0 ? "+" : ""}${fmt(d, 2)}`}</td></tr>;
          })}</tbody>
        </table></div>
      </section>
    </div>
  );
}

export default function Reports({ route }: { route: Route }) {
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [detail, setDetail] = useState<RunDetail | null>(null);
  const [err, setErr] = useState("");
  const isCompare = route.page === "compare";
  const a = route.query.get("a") || "";
  const b = route.query.get("b") || "";
  const selected = isCompare ? undefined : route.arg || runs[0]?.run_id;

  useEffect(() => {
    api.runs().then((r) => setRuns(r.history)).catch((e) => setErr(String(e)));
  }, []);
  useEffect(() => {
    if (!selected) return;
    setDetail(null);
    const load = () => api.run(selected).then(setDetail).catch((e) => setErr(String(e)));
    load();
    const t = setInterval(() => { if (!detail?.report) load(); }, 5000);
    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selected]);

  return (
    <div className="grid">
      <section className="panel">
        <div className="row">
          <label className="f">Run
            <select value={selected || ""} onChange={(e) => go(`reports/${e.target.value}`)}>
              {runs.map((r) => <option key={r.run_id} value={r.run_id}>{r.run_id} · {r.verdict || r.status}</option>)}
            </select>
          </label>
          <span style={{ flex: 1 }} />
          <label className="f">Compare A
            <select value={a || selected || ""} onChange={(e) => go(`compare?a=${e.target.value}&b=${b || runs[1]?.run_id || ""}`)}>
              {runs.map((r) => <option key={r.run_id} value={r.run_id}>{r.run_id}</option>)}
            </select>
          </label>
          <label className="f">with B
            <select value={b} onChange={(e) => go(`compare?a=${a || selected || runs[0]?.run_id}&b=${e.target.value}`)}>
              <option value="">—</option>
              {runs.map((r) => <option key={r.run_id} value={r.run_id}>{r.run_id}</option>)}
            </select>
          </label>
          {isCompare && <button onClick={() => go(`reports/${a}`)}>Back to report</button>}
        </div>
      </section>
      {err && <div className="err">{err}</div>}
      {isCompare && a && b ? <Compare a={a} b={b} /> : detail ? <Detail d={detail} /> : selected ? <div className="muted">loading…</div> : <div className="empty">No runs yet.</div>}
    </div>
  );
}
