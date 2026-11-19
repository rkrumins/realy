import { useQuery } from "@tanstack/react-query";
import { Activity, ArrowRight, CheckCircle2, Database, Gauge, Layers, Radio, Rocket, Timer, Zap } from "lucide-react";
import { Link, useNavigate } from "react-router";
import { Bar, BarChart, CartesianGrid, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { Sparkline } from "@/components/charts";
import { EmptyState, ErrorState, Kpi, PageHeader, StatusBadge, TopologyBadge, VerdictBadge } from "@/components/common";
import { Dot } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardBody, CardHeader } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/controls";
import { api, isNotFound } from "@/lib/api";
import { fmtAgo, fmtCompact, fmtMs, fmtNum, fmtPct, isNum } from "@/lib/format";
import { useEnv, useLiveSeries, useStatus } from "@/lib/hooks";
import type { RunSummary, SummaryResponse } from "@/lib/types";

function fromRuns(history: RunSummary[]): SummaryResponse {
  const done = history.filter((r) => r.verdict);
  return {
    runs_total: history.length,
    runs_pass: done.filter((r) => r.verdict === "PASS").length,
    runs_fail: done.filter((r) => r.verdict === "FAIL").length,
    events_total: done.reduce((a, r) => a + (r.emitted ?? 0), 0),
    decisions_total: done.reduce((a, r) => a + (r.decided ?? 0), 0),
    last_runs: history.slice(0, 12).map((r) => ({
      ...r, scenario: r.scenario ?? "", topology: r.topology ?? "", started: r.started_at, achieved_emit_tps: r.achieved_tps,
    })),
  };
}

function NowRunning() {
  const status = useStatus();
  const cur = status?.current;
  const series = useLiveSeries(120, cur?.run_id ?? null);
  if (!cur) return null;
  const last = series.filter((p) => p.emitted !== null).slice(-5);
  const tps = last.length ? last.reduce((a, p) => a + (p.emitted ?? 0), 0) / last.length : null;
  const p99 = [...series].reverse().find((p) => p.e2e_p99 !== null)?.e2e_p99 ?? null;
  return (
    <Link to="/live" className="group mb-6 block overflow-hidden rounded-2xl border border-accent/30 bg-[linear-gradient(110deg,color-mix(in_oklab,var(--accent)_14%,var(--card))_0%,var(--card)_60%)] shadow-card">
      <div className="flex flex-wrap items-center gap-x-8 gap-y-3 px-5 py-4">
        <div className="flex items-center gap-3">
          <div className="flex size-10 items-center justify-center rounded-xl bg-accent/15 text-accent"><Radio className="size-5" /></div>
          <div>
            <div className="flex items-center gap-2 text-[11.5px] font-semibold uppercase tracking-[0.12em] text-accent"><Dot tone="accent" pulse />Running now · {cur.phase?.toLowerCase()}</div>
            <div className="mt-0.5 font-mono text-[14px] font-medium text-fg">{cur.scenario}</div>
          </div>
        </div>
        <div className="text-[13px] text-muted"><div className="text-[11px] uppercase tracking-wider text-subtle">throughput</div><span className="text-[18px] font-semibold text-fg num">{fmtNum(tps)}</span> TPS</div>
        <div className="text-[13px] text-muted"><div className="text-[11px] uppercase tracking-wider text-subtle">e2e p99</div><span className="text-[18px] font-semibold text-fg num">{fmtMs(p99)}</span>{cur.budget && <> / {fmtMs(cur.budget.e2e_p99_ms)}</>}</div>
        <div className="hidden h-12 min-w-40 flex-1 md:block"><Sparkline data={series.map((p) => p.emitted)} /></div>
        <span className="ml-auto inline-flex items-center gap-1 text-[13px] font-medium text-accent">Open live view<ArrowRight className="size-4 transition group-hover:translate-x-0.5" /></span>
      </div>
    </Link>
  );
}

export default function Overview() {
  const nav = useNavigate();
  const summary = useQuery({
    queryKey: ["summary"],
    queryFn: async () => {
      try {
        return await api.summary();
      } catch (e) {
        if (!isNotFound(e)) throw e;
        return fromRuns((await api.runs(200)).history);
      }
    },
    refetchInterval: 10_000,
  });
  const env = useEnv();
  const s = summary.data;
  const done = (s?.runs_pass ?? 0) + (s?.runs_fail ?? 0);
  const lastDone = s?.last_runs.find((r) => r.verdict);
  const trend = (s?.last_runs ?? []).filter((r) => isNum(r.e2e_p99_ms)).slice(0, 12).reverse()
    .map((r) => ({ name: r.scenario, run: r.run_id, p99: r.e2e_p99_ms as number, verdict: r.verdict }));

  return (
    <>
      <PageHeader eyebrow="Overview" title="Fraud pipeline benchmark"
        subtitle="Drive production-shaped scoring pipelines with deterministic payments, time every stage, and prove every decision was complete and correct."
        actions={<>
          <Button variant="secondary" onClick={() => nav("/live")}><Activity />Live view</Button>
          <Button variant="gradient" onClick={() => nav("/launch")}><Rocket />Launch scenario</Button>
        </>} />
      <NowRunning />
      {summary.error && <ErrorState error={summary.error} />}
      <div className="grid grid-cols-2 gap-3 md:gap-4 lg:grid-cols-5">
        {!s ? Array.from({ length: 5 }, (_, i) => <Skeleton key={i} className="h-[96px] rounded-2xl" />) : <>
          <Kpi icon={<Layers />} label="Runs" value={fmtNum(s.runs_total)} sub={`${s.runs_pass} pass · ${s.runs_fail} fail`} />
          <Kpi icon={<CheckCircle2 />} label="Pass rate" value={done ? fmtPct(s.runs_pass / done, 0) : "—"} tone={done && s.runs_pass / done >= 0.5 ? "pass" : done ? "fail" : undefined}
            sub={`${done} with a verdict`} />
          <Kpi icon={<Zap />} label="Events emitted" value={fmtCompact(s.events_total)} sub="all completed runs" />
          <Kpi icon={<Database />} label="Decisions" value={fmtCompact(s.decisions_total)} sub={s.events_total ? `${fmtPct(s.decisions_total / s.events_total, 2)} of emitted` : undefined} />
          <Kpi icon={<Timer />} label="Last e2e p99" value={fmtMs(lastDone?.e2e_p99_ms)} className="col-span-2 lg:col-span-1"
            sub={lastDone ? `${lastDone.scenario} · ${fmtNum(lastDone.achieved_emit_tps)} TPS` : "no completed run"} />
        </>}
      </div>

      <div className="mt-6 grid grid-cols-1 gap-5 xl:grid-cols-[minmax(0,1.65fr)_minmax(0,1fr)]">
        <Card>
          <CardHeader title="Recent runs" subtitle="Newest first. Click a run for its full report."
            actions={<Button variant="ghost" size="sm" onClick={() => nav("/runs")}>All runs<ArrowRight /></Button>} />
          <div className="overflow-x-auto">
            {!s ? <div className="space-y-2 px-5 pb-5">{Array.from({ length: 6 }, (_, i) => <Skeleton key={i} className="h-9" />)}</div>
              : s.last_runs.length === 0 ? (
                <div className="px-5 pb-5">
                  <EmptyState icon={<Rocket />} title="No runs yet" action={<Button variant="gradient" onClick={() => nav("/launch")}><Rocket />Launch the smoke test</Button>}>
                    Start with the 60-second smoke scenario to check the whole pipeline end to end.
                  </EmptyState>
                </div>
              ) : (
                <table className="w-full min-w-[640px] text-[13px]">
                  <thead>
                    <tr className="border-y border-border bg-surface-2/40 text-left text-[11px] uppercase tracking-wider text-subtle">
                      <th className="px-5 py-2 font-medium">Verdict</th><th className="px-3 py-2 font-medium">Scenario</th>
                      <th className="px-3 py-2 font-medium">Topology</th><th className="px-3 py-2 text-right font-medium">TPS</th>
                      <th className="px-3 py-2 text-right font-medium">e2e p99</th><th className="px-5 py-2 text-right font-medium">Started</th>
                    </tr>
                  </thead>
                  <tbody>
                    {s.last_runs.map((r) => (
                      <tr key={r.run_id} onClick={() => nav(`/runs/${encodeURIComponent(r.run_id)}`)} className="cursor-pointer border-b border-border last:border-0 hover:bg-surface-2/60">
                        <td className="px-5 py-2.5">{r.verdict ? <VerdictBadge verdict={r.verdict} /> : <StatusBadge status={r.status ?? "incomplete"} />}</td>
                        <td className="px-3 py-2.5 font-mono text-[12.5px] text-fg">{r.scenario}</td>
                        <td className="px-3 py-2.5"><TopologyBadge topology={r.topology} /></td>
                        <td className="px-3 py-2.5 text-right num">{fmtNum(r.achieved_emit_tps)}</td>
                        <td className="px-3 py-2.5 text-right num">{fmtMs(r.e2e_p99_ms)}</td>
                        <td className="px-5 py-2.5 text-right text-muted">{fmtAgo(Number(r.started))}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
          </div>
        </Card>

        <div className="flex flex-col gap-5">
          <Card>
            <CardHeader title="Environment" subtitle={env.data ? `checked ${new Date(env.data.checked_at).toLocaleTimeString("en-GB")}` : "component health"}
              actions={<Button variant="ghost" size="sm" onClick={() => nav("/environment")}>Details<ArrowRight /></Button>} />
            <CardBody className="pt-0">
              {env.isLoading ? <div className="space-y-2">{Array.from({ length: 5 }, (_, i) => <Skeleton key={i} className="h-7" />)}</div>
                : !env.data ? <div className="text-[13px] text-subtle">Environment endpoint not available.</div> : (
                  <ul className="divide-y divide-border">
                    {env.data.components.map((c) => (
                      <li key={c.id} className="flex items-center gap-3 py-2 text-[13px]">
                        <Dot tone={c.status === "up" ? "pass" : c.status === "degraded" ? "warn" : "fail"} pulse={c.status !== "up"} />
                        <span className="font-medium text-fg">{c.name}</span>
                        <span className="truncate text-[12px] text-subtle">{c.version ?? ""}</span>
                        <span className="ml-auto text-[12px] text-muted num">{isNum(c.latency_ms) ? fmtMs(c.latency_ms) : c.status}</span>
                      </li>
                    ))}
                  </ul>
                )}
            </CardBody>
          </Card>
          <Card>
            <CardHeader title="End-to-end p99 by run" subtitle="Last 12 runs with a latency result" icon={<Gauge />} />
            <CardBody className="pt-0">
              {trend.length < 2 ? <div className="py-8 text-center text-[13px] text-subtle">Not enough runs yet.</div> : (
                <ResponsiveContainer width="100%" height={170}>
                  <BarChart data={trend} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
                    <CartesianGrid stroke="var(--grid)" vertical={false} />
                    <XAxis dataKey="run" hide />
                    <YAxis width={48} tickLine={false} axisLine={false} tick={{ fill: "var(--fg-subtle)", fontSize: 11 }} tickFormatter={(v) => fmtMs(v)} scale="log" domain={[(lo: number) => Math.max(1, lo / 2), (hi: number) => hi * 1.3]} allowDataOverflow />
                    <Tooltip cursor={{ fill: "var(--card-2)" }} isAnimationActive={false}
                      content={({ active, payload }) => active && payload?.[0] ? (
                        <div className="rounded-xl border border-border bg-surface px-3 py-2 text-[12px] shadow-xl">
                          <div className="font-mono text-fg">{(payload[0].payload as { name: string }).name}</div>
                          <div className="text-muted">p99 <b className="text-fg num">{fmtMs(payload[0].value)}</b> · {(payload[0].payload as { verdict: string }).verdict ?? "no verdict"}</div>
                        </div>
                      ) : null} />
                    <Bar dataKey="p99" radius={[4, 4, 0, 0]} isAnimationActive={false} onClick={(d) => nav(`/runs/${encodeURIComponent((d as unknown as { run: string }).run)}`)} className="cursor-pointer">
                      {trend.map((t) => <Cell key={t.run} fill={t.verdict === "PASS" ? "var(--pass)" : t.verdict ? "var(--fail)" : "var(--fg-subtle)"} fillOpacity={0.8} />)}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              )}
              <div className="mt-1 flex gap-4 text-[11px] text-subtle">
                <span className="inline-flex items-center gap-1.5"><CheckCircle2 className="size-3 text-pass" />PASS</span>
                <span className="inline-flex items-center gap-1.5"><span className="size-2 rounded-sm bg-fail" />FAIL</span>
              </div>
            </CardBody>
          </Card>
        </div>
      </div>
    </>
  );
}
