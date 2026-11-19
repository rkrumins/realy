import { useQuery } from "@tanstack/react-query";
import { ArrowLeftRight, GitCompareArrows } from "lucide-react";
import { useNavigate, useSearchParams } from "react-router";
import { Bar, BarChart, CartesianGrid, Legend as RLegend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { Legend, SERIES } from "@/components/charts";
import { EmptyState, ErrorState, PageHeader, SectionTitle, TopologyBadge, VerdictBadge } from "@/components/common";
import { STAGE_ORDER } from "@/components/waterfall";
import { Button } from "@/components/ui/button";
import { Card, CardBody, CardHeader } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/controls";
import { Select } from "@/components/ui/select";
import { api } from "@/lib/api";
import { cn } from "@/lib/cn";
import { fmtCompact, fmtMs, fmtNum, fmtPct, isNum } from "@/lib/format";
import type { Detection, RunSummary } from "@/lib/types";

const ROWS: { label: string; get: (r: RunSummary) => number | null; fmt: (v: number | null) => string; better: "lower" | "higher" }[] = [
  { label: "Achieved TPS", get: (r) => r.achieved_tps, fmt: (v) => fmtNum(v), better: "higher" },
  { label: "e2e p50", get: (r) => r.e2e_p50_ms, fmt: (v) => fmtMs(v), better: "lower" },
  { label: "e2e p99", get: (r) => r.e2e_p99_ms, fmt: (v) => fmtMs(v), better: "lower" },
  { label: "Hot path p99", get: (r) => r.hot_path_p99_ms, fmt: (v) => fmtMs(v), better: "lower" },
  { label: "Within budget", get: (r) => r.within_budget, fmt: (v) => fmtPct(v, 2), better: "higher" },
  { label: "Decided", get: (r) => r.decided, fmt: (v) => fmtCompact(v), better: "higher" },
  { label: "Missing", get: (r) => r.missing, fmt: (v) => fmtNum(v), better: "lower" },
  { label: "Duplicates", get: (r) => r.duplicates, fmt: (v) => fmtNum(v), better: "lower" },
];

function Delta({ a, b, better, pct = true }: { a: number | null; b: number | null; better: "lower" | "higher"; pct?: boolean }) {
  if (!isNum(a) || !isNum(b)) return <span className="text-subtle">—</span>;
  const d = b - a;
  if (Math.abs(d) < 1e-9) return <span className="text-subtle">=</span>;
  const good = better === "lower" ? d < 0 : d > 0;
  const rel = a !== 0 ? d / Math.abs(a) : null;
  return (
    <span className={cn("num font-medium", good ? "text-pass" : "text-fail")}>
      {d > 0 ? "+" : "−"}{pct && rel !== null ? fmtPct(Math.abs(rel), 0) : fmtCompact(Math.abs(d))}
    </span>
  );
}

const DET: { k: keyof Detection; label: string; better: "lower" | "higher" }[] = [
  { k: "recall_flagged", label: "Recall (flagged)", better: "higher" }, { k: "recall_declined", label: "Recall (declined)", better: "higher" },
  { k: "precision_flagged", label: "Precision", better: "higher" }, { k: "false_positive_rate", label: "False-positive rate", better: "lower" },
];

export default function Compare() {
  const [params, setParams] = useSearchParams();
  const nav = useNavigate();
  const a = params.get("a") || "";
  const b = params.get("b") || "";
  const runs = useQuery({ queryKey: ["runs"], queryFn: () => api.runs(500) });
  const cmp = useQuery({ queryKey: ["compare", a, b], queryFn: () => api.compare(a, b), enabled: Boolean(a && b) });
  const opts = (runs.data?.history ?? []).filter((r) => r.verdict).map((r) => ({ value: r.run_id, label: <span className="font-mono text-[12.5px]">{r.run_id}</span>, hint: `${r.verdict} · ${fmtMs(r.e2e_p99_ms)} p99` }));
  const set = (k: "a" | "b", v: string) => setParams((p) => { p.set(k, v); return p; }, { replace: true });
  const c = cmp.data;
  type SR = { key: string; label: string; sub?: boolean } & Record<string, unknown>;
  const stageRows: SR[] = c ? STAGE_ORDER.filter((s) => c.stages[s.key]).map((s) => ({ ...s, ...c.stages[s.key] }))
    .concat(["hot_path", "e2e"].filter((k) => c.stages[k]).map((k) => ({ key: k, label: k === "e2e" ? "end to end" : "hot path", ...c.stages[k] }))) : [];
  const chart = stageRows.filter((r) => r.key !== "e2e").map((r) => ({ name: r.label, A: (r.a_p99 as number | null) ?? 0, B: (r.b_p99 as number | null) ?? 0 }));
  const la = c?.levels.a ?? [], lb = c?.levels.b ?? [];

  return (
    <>
      <PageHeader eyebrow="Compare" title="Compare two runs" subtitle="Same inputs, different topology or configuration: see exactly which stage moved." />
      <Card className="mb-6">
        <CardBody className="grid items-end gap-3 pt-5 md:grid-cols-[1fr_auto_1fr]">
          <label className="text-[12.5px] font-medium text-muted"><span className="mb-1.5 flex items-center gap-2"><span className="size-2 rounded-full" style={{ background: SERIES[0] }} />Run A (baseline)</span>
            <Select value={a} onChange={(v) => set("a", v)} options={opts} placeholder="Pick a run…" ariaLabel="Run A" />
          </label>
          <Button variant="ghost" size="icon" aria-label="Swap" onClick={() => setParams({ a: b, b: a }, { replace: true })}><ArrowLeftRight /></Button>
          <label className="text-[12.5px] font-medium text-muted"><span className="mb-1.5 flex items-center gap-2"><span className="size-2 rounded-full" style={{ background: SERIES[1] }} />Run B (candidate)</span>
            <Select value={b} onChange={(v) => set("b", v)} options={opts} placeholder="Pick a run…" ariaLabel="Run B" />
          </label>
        </CardBody>
      </Card>
      {!(a && b) ? (
        <EmptyState icon={<GitCompareArrows />} title="Pick two runs" action={<Button variant="secondary" onClick={() => nav("/runs")}>Select on the Runs page</Button>}>
          Deltas are B relative to A; green means B is better.
        </EmptyState>
      ) : cmp.isLoading ? <Skeleton className="h-96 rounded-2xl" /> : cmp.error ? <ErrorState error={cmp.error} /> : c && (
        <>
          <div className="grid gap-5 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.4fr)]">
            <Card className="overflow-hidden">
              <table className="w-full text-[13px]">
                <thead className="border-b border-border bg-surface-2/40 text-left text-[11px] uppercase tracking-wider text-subtle">
                  <tr><th className="px-5 py-2.5 font-medium">Metric</th><th className="px-3 py-2.5 text-right font-medium">A</th><th className="px-3 py-2.5 text-right font-medium">B</th><th className="px-5 py-2.5 text-right font-medium">Δ</th></tr>
                </thead>
                <tbody>
                  <tr className="border-b border-border"><td className="px-5 py-2.5 text-muted">Verdict</td><td className="px-3 py-2.5 text-right"><VerdictBadge verdict={c.a.verdict} /></td><td className="px-3 py-2.5 text-right"><VerdictBadge verdict={c.b.verdict} /></td><td /></tr>
                  <tr className="border-b border-border"><td className="px-5 py-2.5 text-muted">Topology</td><td className="px-3 py-2.5 text-right"><TopologyBadge topology={c.a.topology} /></td><td className="px-3 py-2.5 text-right"><TopologyBadge topology={c.b.topology} /></td><td /></tr>
                  {ROWS.map((r) => (
                    <tr key={r.label} className="border-b border-border last:border-0">
                      <td className="px-5 py-2.5 text-muted">{r.label}</td>
                      <td className="px-3 py-2.5 text-right num">{r.fmt(r.get(c.a))}</td>
                      <td className="px-3 py-2.5 text-right num">{r.fmt(r.get(c.b))}</td>
                      <td className="px-5 py-2.5 text-right"><Delta a={r.get(c.a)} b={r.get(c.b)} better={r.better} /></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </Card>
            <Card>
              <CardHeader title="p99 by stage" subtitle="where the two runs spend their tail" />
              <CardBody>
                <Legend className="mb-2" series={[{ label: "A", color: SERIES[0] }, { label: "B", color: SERIES[1] }]} />
                <ResponsiveContainer width="100%" height={Math.max(260, chart.length * 30)}>
                  <BarChart data={chart} layout="vertical" margin={{ top: 0, right: 16, bottom: 0, left: 0 }} barGap={2} barCategoryGap={8}>
                    <CartesianGrid stroke="var(--grid)" horizontal={false} />
                    <XAxis type="number" tickLine={false} axisLine={false} tick={{ fill: "var(--fg-subtle)", fontSize: 11 }} tickFormatter={(v) => fmtMs(v)} />
                    <YAxis type="category" dataKey="name" width={120} tickLine={false} axisLine={false} tick={{ fill: "var(--fg-muted)", fontSize: 11.5 }} />
                    <Tooltip cursor={{ fill: "var(--card-2)" }} isAnimationActive={false} formatter={(v) => fmtMs(v as number)}
                      contentStyle={{ background: "var(--card)", border: "1px solid var(--border)", borderRadius: 12, fontSize: 12 }} labelStyle={{ color: "var(--fg)" }} itemStyle={{ color: "var(--fg-muted)" }} />
                    <Bar dataKey="A" fill={SERIES[0]} radius={[0, 4, 4, 0]} isAnimationActive={false} />
                    <Bar dataKey="B" fill={SERIES[1]} radius={[0, 4, 4, 0]} isAnimationActive={false} />
                  </BarChart>
                </ResponsiveContainer>
              </CardBody>
            </Card>
          </div>

          <SectionTitle>Stage deltas</SectionTitle>
          <Card className="overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full min-w-[760px] text-[12.5px]">
                <thead className="border-b border-border bg-surface-2/40 text-left text-[11px] uppercase tracking-wider text-subtle">
                  <tr><th className="px-5 py-2 font-medium">Stage</th>
                    {["p50", "p99", "p99.9"].map((q) => <th key={q} colSpan={3} className="px-3 py-2 text-center font-medium">{q} · A / B / Δ</th>)}</tr>
                </thead>
                <tbody>
                  {stageRows.map((r) => (
                    <tr key={r.key} className={cn("border-b border-border last:border-0", (r.key === "e2e" || r.key === "hot_path") && "bg-surface-2/40 font-medium")}>
                      <td className={cn("px-5 py-2", "sub" in r && r.sub && "pl-9 text-subtle")}>{r.label}</td>
                      {(["p50", "p99", "p999"] as const).map((q) => {
                        const va = r[`a_${q}`] as number | null, vb = r[`b_${q}`] as number | null;
                        return [
                          <td key={`${q}a`} className="px-2 py-2 text-right num text-muted">{fmtMs(va)}</td>,
                          <td key={`${q}b`} className="px-2 py-2 text-right num">{fmtMs(vb)}</td>,
                          <td key={`${q}d`} className="px-3 py-2 text-right"><Delta a={va} b={vb} better="lower" /></td>,
                        ];
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>

          {la.length > 1 && lb.length > 1 && (
            <>
              <SectionTitle>Load curves</SectionTitle>
              <Card>
                <CardBody className="pt-5">
                  <ResponsiveContainer width="100%" height={260}>
                    <LineChart margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
                      <CartesianGrid stroke="var(--grid)" vertical={false} />
                      <XAxis dataKey="tps" type="number" domain={["dataMin", "dataMax"]} tickLine={false} axisLine={false} tick={{ fill: "var(--fg-subtle)", fontSize: 11 }} tickFormatter={(v) => fmtCompact(v)} allowDuplicatedCategory={false} />
                      <YAxis scale="log" domain={["auto", "auto"]} allowDataOverflow tickLine={false} axisLine={false} width={52} tick={{ fill: "var(--fg-subtle)", fontSize: 11 }} tickFormatter={(v) => fmtMs(v)} />
                      <Tooltip formatter={(v) => fmtMs(v as number)} contentStyle={{ background: "var(--card)", border: "1px solid var(--border)", borderRadius: 12, fontSize: 12 }} />
                      <RLegend wrapperStyle={{ fontSize: 12 }} />
                      <Line data={la.map((l) => ({ tps: l.target_tps, p99: l.stages?.e2e?.p99 }))} dataKey="p99" name="A p99" stroke={SERIES[0]} strokeWidth={2} dot={{ r: 3 }} isAnimationActive={false} />
                      <Line data={lb.map((l) => ({ tps: l.target_tps, p99: l.stages?.e2e?.p99 }))} dataKey="p99" name="B p99" stroke={SERIES[1]} strokeWidth={2} dot={{ r: 3 }} isAnimationActive={false} />
                    </LineChart>
                  </ResponsiveContainer>
                </CardBody>
              </Card>
            </>
          )}

          {(isNum(c.detection.a?.fraud_events) || isNum(c.detection.b?.fraud_events)) && (
            <>
              <SectionTitle>Detection</SectionTitle>
              <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
                {DET.map((m) => {
                  const va = c.detection.a?.[m.k] as number | null | undefined, vb = c.detection.b?.[m.k] as number | null | undefined;
                  return (
                    <Card key={m.k} className="p-4">
                      <div className="text-[11.5px] font-medium uppercase tracking-wider text-subtle">{m.label}</div>
                      <div className="mt-2 flex items-baseline gap-3 num">
                        <span className="text-[18px] font-semibold" style={{ color: SERIES[0] }}>{fmtPct(va, 1)}</span>
                        <span className="text-[18px] font-semibold" style={{ color: SERIES[1] }}>{fmtPct(vb, 1)}</span>
                        <span className="ml-auto text-[12.5px]"><Delta a={va ?? null} b={vb ?? null} better={m.better} pct={false} /></span>
                      </div>
                    </Card>
                  );
                })}
              </div>
            </>
          )}
        </>
      )}
    </>
  );
}
