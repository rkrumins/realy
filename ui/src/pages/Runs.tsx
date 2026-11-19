import { useEffect, useState } from "react";
import { ago, api, fmt, fmtInt, Job, pct, RunSummary, ScenarioInfo, Schedule } from "../api";
import { go } from "../App";

const TOPOLOGIES = ["", "gateway-kafka", "gateway-grpc", "flink-inline", "echo-kafka"];

function setPath(o: Record<string, any>, path: string, v: unknown) {
  const parts = path.split(".");
  let cur = o;
  for (const p of parts.slice(0, -1)) cur = cur[p] ??= {};
  cur[parts[parts.length - 1]] = v;
}

export default function Runs() {
  const [scenarios, setScenarios] = useState<ScenarioInfo[]>([]);
  const [data, setData] = useState<{ queue: Job[]; jobs: Job[]; history: RunSummary[]; current: any } | null>(null);
  const [schedules, setSchedules] = useState<Schedule[]>([]);
  const [err, setErr] = useState("");
  const [form, setForm] = useState({ scenario: "smoke", tps: "", duration: "", warmup: "", topology: "", backfill: "", verify: false });
  const [sched, setSched] = useState({ every: "60", count: "", at: "" });

  const load = () => {
    api.runs().then(setData).catch((e) => setErr(String(e)));
    api.schedules().then(setSchedules).catch(() => {});
  };
  useEffect(() => {
    api.scenarios().then(setScenarios).catch((e) => setErr(String(e)));
    load();
    const t = setInterval(load, 3000);
    return () => clearInterval(t);
  }, []);

  const overrides = () => {
    const o: Record<string, any> = {};
    if (form.tps) setPath(o, "traffic.profile.tps", +form.tps);
    if (form.duration) setPath(o, "traffic.duration_s", +form.duration);
    if (form.warmup) setPath(o, "traffic.warmup_s", +form.warmup);
    if (form.backfill) setPath(o, "traffic.backfill_days", +form.backfill);
    if (form.topology) o.topology = form.topology;
    return o;
  };
  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setErr("");
    try {
      await api.startRun(form.scenario, overrides(), form.verify);
      load();
    } catch (x) {
      setErr(String(x));
    }
  };
  const addSchedule = async () => {
    setErr("");
    try {
      await api.addSchedule({
        scenario: form.scenario, overrides: overrides(), verify: form.verify,
        every_minutes: sched.every ? +sched.every : null, count: sched.count ? +sched.count : null,
        at: sched.at ? new Date(sched.at).toISOString() : null,
      });
      load();
    } catch (x) {
      setErr(String(x));
    }
  };
  const sel = scenarios.find((s) => s.name === form.scenario);
  const cur = data?.current;

  return (
    <div className="grid">
      <div className="grid side">
        <section className="panel">
          <div className="head"><h2>Start a run</h2><span className="sp" />
            {sel && <span className="muted">{sel.topology} · {fmtInt(sel.tps)} TPS · {fmtInt(sel.duration_s)} s</span>}</div>
          <form className="inline" onSubmit={submit}>
            <label className="f">Scenario
              <select value={form.scenario} onChange={(e) => setForm({ ...form, scenario: e.target.value })}>
                {scenarios.map((s) => <option key={s.name} value={s.name}>{s.name}</option>)}
              </select>
            </label>
            <label className="f">TPS<input type="number" min="1" placeholder={String(sel?.tps ?? "")} value={form.tps}
              onChange={(e) => setForm({ ...form, tps: e.target.value })} /></label>
            <label className="f">Duration s<input type="number" min="5" placeholder={String(sel?.duration_s ?? "")} value={form.duration}
              onChange={(e) => setForm({ ...form, duration: e.target.value })} /></label>
            <label className="f">Warmup s<input type="number" min="0" value={form.warmup}
              onChange={(e) => setForm({ ...form, warmup: e.target.value })} /></label>
            <label className="f">Backfill days<input type="number" min="0" step="0.5" value={form.backfill}
              onChange={(e) => setForm({ ...form, backfill: e.target.value })} /></label>
            <label className="f">Topology
              <select value={form.topology} onChange={(e) => setForm({ ...form, topology: e.target.value })}>
                {TOPOLOGIES.map((t) => <option key={t} value={t}>{t || "(scenario)"}</option>)}
              </select>
            </label>
            <label className="f" style={{ flexDirection: "row", alignItems: "center", gap: 6, paddingBottom: 6 }}>
              <input type="checkbox" checked={form.verify} onChange={(e) => setForm({ ...form, verify: e.target.checked })} /> verify
            </label>
            <button className="primary" type="submit">Queue run</button>
          </form>
          {sel && <p className="muted" style={{ margin: "8px 0 0" }}>{sel.description}</p>}
          {err && <div className="err" style={{ marginTop: 8 }}>{err}</div>}
        </section>
        <section className="panel">
          <div className="head"><h2>Now running</h2></div>
          {cur ? (
            <div className="grid">
              <div><span className={`badge ${cur.phase}`}>{cur.phase}</span> <span className="mono">{cur.run_id}</span></div>
              <div className="muted">{cur.scenario} · {cur.topology} · base {fmtInt(cur.base_tps)} TPS × {fmt(cur.rate_multiplier, 2)} · started {ago(cur.started_at)}</div>
              <div className="row">
                <button onClick={() => go("live")}>Open live view</button>
                <button className="danger" onClick={() => api.abort(cur.run_id).then(load)}>Abort</button>
              </div>
            </div>
          ) : <div className="muted">Nothing running. Queued runs start automatically, one at a time.</div>}
          {data && data.queue.length > 0 && (
            <>
              <h3 style={{ marginTop: 12 }}>Queue</h3>
              <table><tbody>
                {data.queue.map((j) => (
                  <tr key={j.id}><td>{j.scenario}</td><td className="muted mono">{JSON.stringify(j.overrides)}</td>
                    <td><button onClick={() => api.abort(j.id).then(load)}>cancel</button></td></tr>
                ))}
              </tbody></table>
            </>
          )}
        </section>
      </div>

      <section className="panel">
        <div className="head"><h2>Run history</h2><span className="sp" /><span className="muted">click a run for its report</span></div>
        <div className="tw">
          <table>
            <thead><tr>
              <th>Run</th><th>Topology</th><th>Status</th><th>Verdict</th><th className="num">TPS</th>
              <th className="num">e2e p50</th><th className="num">e2e p99</th><th className="num">hot p99</th>
              <th className="num">in budget</th><th className="num">missing</th><th className="num">dups</th><th>Verify</th><th>Notes</th>
            </tr></thead>
            <tbody>
              {(data?.history || []).map((r) => (
                <tr key={r.run_id} className="click" onClick={() => go(`reports/${r.run_id}`)}>
                  <td className="mono">{r.run_id}</td><td>{r.topology}</td>
                  <td><span className={`badge ${r.status}`}>{r.phase || r.status}</span></td>
                  <td>{r.verdict && <span className={`badge ${r.verdict}`}>{r.verdict}</span>}</td>
                  <td className="num">{fmtInt(r.achieved_tps)}</td>
                  <td className="num">{fmt(r.e2e_p50_ms)}</td><td className="num">{fmt(r.e2e_p99_ms)}</td>
                  <td className="num">{fmt(r.hot_path_p99_ms)}</td><td className="num">{pct(r.within_budget)}</td>
                  <td className="num">{fmtInt(r.missing)}</td><td className="num">{fmtInt(r.duplicates)}</td>
                  <td>{r.verify_passed === true ? <span className="badge ok">PASS</span> : r.verify_passed === false ? <span className="badge FAIL">FAIL</span> : ""}</td>
                  <td className="wrap muted">{r.abort_reason || (r.reasons || []).join("; ")}</td>
                </tr>
              ))}
              {data && data.history.length === 0 && <tr><td colSpan={13} className="empty">No runs yet.</td></tr>}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <div className="head"><h2>Schedules (sustained / soak testing)</h2><span className="sp" />
          <span className="muted">uses the start-run form above for scenario + overrides</span></div>
        <div className="row">
          <label className="f">Every (minutes)<input type="number" min="1" value={sched.every} onChange={(e) => setSched({ ...sched, every: e.target.value })} /></label>
          <label className="f">Count (blank = forever)<input type="number" min="1" value={sched.count} onChange={(e) => setSched({ ...sched, count: e.target.value })} /></label>
          <label className="f">First at (blank = now)<input type="datetime-local" value={sched.at} onChange={(e) => setSched({ ...sched, at: e.target.value })} /></label>
          <button onClick={addSchedule}>Add schedule for “{form.scenario}”</button>
        </div>
        <div className="tw" style={{ marginTop: 8 }}>
          <table>
            <thead><tr><th>Scenario</th><th>Overrides</th><th>Next</th><th>Every</th><th>Remaining</th><th>Enabled</th><th /></tr></thead>
            <tbody>
              {schedules.map((s) => (
                <tr key={s.id}>
                  <td>{s.scenario}</td><td className="mono muted">{JSON.stringify(s.overrides)}</td>
                  <td>{new Date(s.next_at * 1000).toLocaleString()}</td><td>{s.every_minutes ? `${s.every_minutes} min` : "once"}</td>
                  <td>{s.remaining ?? "∞"}</td><td>{s.enabled ? "yes" : "no"}</td>
                  <td><button onClick={() => api.deleteSchedule(s.id).then(load)}>delete</button></td>
                </tr>
              ))}
              {schedules.length === 0 && <tr><td colSpan={7} className="muted">No schedules.</td></tr>}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
