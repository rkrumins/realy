import { useEffect, useState } from "react";
import { api, BudgetResult, fmt, pct, ScenarioInfo } from "../api";
import { go } from "../App";

export default function Scenarios({ name }: { name?: string }) {
  const [list, setList] = useState<ScenarioInfo[]>([]);
  const [text, setText] = useState("");
  const [status, setStatus] = useState<{ ok?: string; err?: string }>({});
  const [budget, setBudget] = useState<BudgetResult | null>(null);
  const [busy, setBusy] = useState(false);
  const selected = name || list[0]?.name;

  useEffect(() => {
    api.scenarios().then(setList).catch((e) => setStatus({ err: String(e) }));
  }, []);
  useEffect(() => {
    const s = list.find((x) => x.name === selected);
    if (s) setText(s.yaml);
    setBudget(null);
    setStatus({});
  }, [selected, list]);

  const run = async (label: string, f: () => Promise<void>) => {
    setBusy(true);
    setStatus({});
    try {
      await f();
    } catch (e) {
      setStatus({ err: `${label}: ${e}` });
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="grid side" style={{ gridTemplateColumns: "minmax(200px, 1fr) minmax(0, 3fr)" }}>
      <section className="panel">
        <div className="head"><h2>Scenarios</h2></div>
        <div className="list">
          {list.map((s) => (
            <a key={s.name} href={`#/scenarios/${s.name}`} className={s.name === selected ? "on" : ""}>
              <span>{s.name}</span><span className="muted">{s.topology}</span>
            </a>
          ))}
        </div>
      </section>
      <div className="grid">
        <section className="panel">
          <div className="head">
            <h2 className="mono">{selected}.yaml</h2><span className="sp" />
            <button disabled={busy} onClick={() => run("validate", async () => {
              const r = await api.validate(text);
              setStatus({ ok: `valid · fingerprint ${r.fingerprint}` });
            })}>Validate</button>
            <button disabled={busy} onClick={() => run("budget", async () => setBudget(await api.budget(selected!, { yaml: text })))}>Budget check</button>
            <button className="primary" disabled={busy || !selected} onClick={() => run("save", async () => {
              await api.saveScenario(selected!, text);
              setStatus({ ok: "saved" });
              setList(await api.scenarios());
            })}>Save</button>
            <button disabled={busy} onClick={() => run("queue", async () => {
              await api.saveScenario(selected!, text);
              await api.startRun(selected!, {}, false);
              go("runs");
            })}>Save &amp; run</button>
          </div>
          <textarea rows={28} value={text} spellCheck={false} onChange={(e) => setText(e.target.value)} />
          {status.ok && <div className="muted" style={{ marginTop: 6 }}>✓ {status.ok}</div>}
          {status.err && <div className="err" style={{ marginTop: 6 }}>{status.err}</div>}
        </section>
        {budget && (
          <section className="panel">
            <div className="head">
              <h2>Budget feasibility (Monte Carlo)</h2><span className={`badge ${budget.verdict}`}>{budget.verdict}</span><span className="sp" />
              <span className="muted">budget e2e p99 ≤ {budget.budget.e2e_p99_ms} ms · hot path ≤ {budget.budget.hot_path_p99_ms} ms</span>
            </div>
            <div className="kpis">
              <div className="kpi"><div className="l">predicted e2e p50</div><div className="v">{fmt(budget.e2e.p50)} ms</div></div>
              <div className="kpi"><div className="l">predicted e2e p99</div><div className="v">{fmt(budget.e2e.p99)} ms</div></div>
              <div className="kpi"><div className="l">p99.9</div><div className="v">{fmt(budget.e2e["p99.9"])} ms</div></div>
              <div className="kpi"><div className="l">hot path p99</div><div className="v">{fmt(budget.hot_path.p99)} ms</div></div>
              <div className="kpi"><div className="l">within budget</div><div className="v">{pct(budget.within_budget)}</div></div>
              <div className="kpi"><div className="l">fallback rate</div><div className="v">{pct(budget.fallback_rate)}</div></div>
            </div>
            <div className="grid g2" style={{ marginTop: 10 }}>
              <div className="tw"><table>
                <thead><tr><th>stage</th><th className="num">p99 ms</th><th>share of tail</th></tr></thead>
                <tbody>{Object.entries(budget.stages_p99).map(([k, v]) => (
                  <tr key={k}><td>{k}</td><td className="num">{fmt(v)}</td>
                    <td style={{ width: "45%" }}><div className="bar"><i style={{ width: `${(budget.tail_share[k] ?? 0) * 100}%` }} /></div></td></tr>
                ))}</tbody>
              </table></div>
              <div>
                <h3>Advice</h3>
                <ul>{budget.advice.map((a, i) => <li key={i}>{a}</li>)}</ul>
                <p className="muted">{budget.note}</p>
              </div>
            </div>
          </section>
        )}
      </div>
    </div>
  );
}
