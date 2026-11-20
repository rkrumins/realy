import { useQuery } from "@tanstack/react-query";
import { Activity, ArrowRight, CheckCircle2, Gauge, Layers, Plus, Radio, Timer, Zap } from "lucide-react";
import { Link, useNavigate } from "react-router";
import { Bar, BarChart, CartesianGrid, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { Sparkline } from "@/components/charts";
import { EmptyState, ErrorState, Kpi, LegendDots, PageHeader, StatusBadge } from "@/components/common";
import { ActiveStep } from "@/components/steps";
import { Dot } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardBody, CardHeader } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/controls";
import { api } from "@/lib/api";
import { fmtAgo, fmtCompact, fmtMs, fmtNum, fmtPct, isNum } from "@/lib/format";
import { useEnv, useLiveSeries, useStatus } from "@/lib/hooks";

/** Hero card while a job runs: what it is, where it is, two live numbers, a sparkline, one click to Live. */
function NowRunning() {
  const status = useStatus();
  const cur = status?.current;
  const series = useLiveSeries(120, cur?.id ?? null);
  if (!cur) return null;
  const last = series.filter((p) => p.throughput !== null).slice(-5);
  const tput = last.length ? last.reduce((a, p) => a + (p.throughput ?? 0), 0) / last.length : null;
  const p99 = [...series].reverse().find((p) => p.p99_ms !== null)?.p99_ms ?? null;
  return (
    <Link to="/live" className="group mb-6 block overflow-hidden rounded-2xl border border-accent/30 bg-[linear-gradient(110deg,color-mix(in_oklab,var(--accent)_14%,var(--card))_0%,var(--card)_60%)] shadow-card">
      <div className="flex flex-wrap items-center gap-x-8 gap-y-3 px-5 py-4">
        <div className="flex items-center gap-3">
          <div className="flex size-10 items-center justify-center rounded-xl bg-accent/15 text-accent"><Radio className="size-5" /></div>
          <div>
            <div className="flex items-center gap-2 text-[11.5px] font-semibold uppercase tracking-[0.12em] text-accent"><Dot tone="accent" pulse />Running now · {cur.phase?.toLowerCase()}</div>
            <div className="mt-0.5 font-mono text-[14px] font-medium text-fg">{cur.kind} <span className="text-subtle">{cur.id}</span></div>
            <ActiveStep steps={cur.steps} />
          </div>
        </div>
        <div className="text-[13px] text-muted"><div className="text-[11px] uppercase tracking-wider text-subtle">throughput</div><span className="text-[18px] font-semibold text-fg num">{fmtNum(tput)}</span> items/s</div>
        <div className="text-[13px] text-muted"><div className="text-[11px] uppercase tracking-wider text-subtle">p99 latency</div><span className="text-[18px] font-semibold text-fg num">{fmtMs(p99)}</span>{isNum(cur.budget_ms) && <> / {fmtMs(cur.budget_ms)}</>}</div>
        <div className="hidden h-12 min-w-40 flex-1 md:block"><Sparkline data={series.map((p) => p.throughput)} /></div>
        <span className="ml-auto inline-flex items-center gap-1 text-[13px] font-medium text-accent">Open live view<ArrowRight className="size-4 transition group-hover:translate-x-0.5" /></span>
      </div>
    </Link>
  );
}

export default function Overview() {
  const nav = useNavigate();
  const summary = useQuery({ queryKey: ["summary"], queryFn: api.summary, refetchInterval: 10_000 });
  const env = useEnv();
  const s = summary.data;
  const finished = (s?.jobs_done ?? 0) + (s?.jobs_failed ?? 0);
  const lastDone = s?.last_jobs.find((j) => j.status === "done" && j.result);
  const trend = (s?.last_jobs ?? []).filter((j) => isNum(j.result?.p99_ms)).slice(0, 12).reverse()
    .map((j) => ({ id: j.id, kind: j.kind, p99: j.result!.p99_ms as number, ok: (j.result!.within_budget ?? 0) >= 0.99 }));

  return (
    <>
      <PageHeader eyebrow="Overview" title="Operations overview"
        subtitle="What is running, how recent jobs went, and whether every dependency is healthy."
        actions={<>
          <Button variant="secondary" onClick={() => nav("/live")}><Activity />Live view</Button>
          <Button variant="gradient" onClick={() => nav("/jobs?new=1")}><Plus />New job</Button>
        </>} />
      <NowRunning />
      {summary.error && <div className="mb-4"><ErrorState error={summary.error} /></div>}
      <div className="grid grid-cols-2 gap-3 md:gap-4 lg:grid-cols-5">
        {!s ? Array.from({ length: 5 }, (_, i) => <Skeleton key={i} className="h-[96px] rounded-2xl" />) : <>
          <Kpi icon={<Layers />} label="Jobs" value={fmtNum(s.jobs_total)} sub={`${s.jobs_done} done · ${s.jobs_failed} failed`} />
          <Kpi icon={<CheckCircle2 />} label="Success rate" value={finished ? fmtPct(s.jobs_done / finished, 0) : "—"}
            tone={finished ? (s.jobs_done / finished >= 0.9 ? "pass" : "warn") : undefined} sub={`${finished} finished`} />
          <Kpi icon={<Zap />} label="Items processed" value={fmtCompact(s.items_total)} sub="all finished jobs" />
          <Kpi icon={<Gauge />} label="Throughput" value={fmtNum(lastDone?.result?.throughput)} sub={lastDone ? `items/s · last job (${lastDone.kind})` : "no finished job"} />
          <Kpi icon={<Timer />} label="Last p99" value={fmtMs(lastDone?.result?.p99_ms)} className="col-span-2 lg:col-span-1"
            tone={lastDone?.result && isNum(lastDone.result.p99_ms) && isNum(lastDone.result.budget_ms) ? (lastDone.result.p99_ms > lastDone.result.budget_ms ? "fail" : "pass") : undefined}
            sub={lastDone?.result?.budget_ms ? `budget ${fmtMs(lastDone.result.budget_ms)}` : "per-item latency"} />
        </>}
      </div>

      <div className="mt-6 grid grid-cols-1 gap-5 xl:grid-cols-[minmax(0,1.65fr)_minmax(0,1fr)]">
        <Card>
          <CardHeader title="Recent jobs" subtitle="Newest first. Click a job for its full report."
            actions={<Button variant="ghost" size="sm" onClick={() => nav("/jobs")}>All jobs<ArrowRight /></Button>} />
          <div className="overflow-x-auto">
            {!s ? <div className="space-y-2 px-5 pb-5">{Array.from({ length: 6 }, (_, i) => <Skeleton key={i} className="h-9" />)}</div>
              : s.last_jobs.length === 0 ? (
                <div className="px-5 pb-5">
                  <EmptyState icon={<Plus />} title="No jobs yet" action={<Button variant="gradient" onClick={() => nav("/jobs?new=1")}><Plus />Start a demo job</Button>}>
                    Start a short demo job to see live progress, charts and a report end to end.
                  </EmptyState>
                </div>
              ) : (
                <table className="w-full min-w-[640px] text-[13px]">
                  <thead>
                    <tr className="border-y border-border bg-surface-2/40 text-left text-[11px] uppercase tracking-wider text-subtle">
                      <th className="px-5 py-2 font-medium">Status</th><th className="px-3 py-2 font-medium">Job</th>
                      <th className="px-3 py-2 text-right font-medium">Items</th><th className="px-3 py-2 text-right font-medium">Items/s</th>
                      <th className="px-3 py-2 text-right font-medium">p99</th><th className="px-5 py-2 text-right font-medium">Started</th>
                    </tr>
                  </thead>
                  <tbody>
                    {s.last_jobs.map((j) => (
                      <tr key={j.id} onClick={() => nav(`/jobs/${encodeURIComponent(j.id)}`)} className="cursor-pointer border-b border-border last:border-0 hover:bg-surface-2/60">
                        <td className="px-5 py-2.5"><StatusBadge status={j.status} /></td>
                        <td className="px-3 py-2.5">
                          <Link to={`/jobs/${encodeURIComponent(j.id)}`} className="font-mono text-[12.5px] text-fg hover:underline" onClick={(e) => e.stopPropagation()}>{j.kind}</Link>
                          <span className="ml-2 font-mono text-[11px] text-subtle">{j.id}</span>
                        </td>
                        <td className="px-3 py-2.5 text-right num">{fmtCompact(j.result?.processed)}</td>
                        <td className="px-3 py-2.5 text-right num">{fmtNum(j.result?.throughput)}</td>
                        <td className="px-3 py-2.5 text-right num">{fmtMs(j.result?.p99_ms)}</td>
                        <td className="px-5 py-2.5 text-right text-muted">{fmtAgo(j.started_at ?? j.created_at)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
          </div>
        </Card>

        <div className="flex flex-col gap-5">
          <Card>
            <CardHeader title="Environment" subtitle={env.data ? `checked ${new Date(env.data.checked_at).toLocaleTimeString("en-GB")}` : "dependency health"}
              actions={<Button variant="ghost" size="sm" onClick={() => nav("/environment")}>Details<ArrowRight /></Button>} />
            <CardBody className="pt-0">
              {env.isLoading ? <div className="space-y-2">{Array.from({ length: 4 }, (_, i) => <Skeleton key={i} className="h-7" />)}</div>
                : env.error ? <ErrorState error={env.error} />
                  : !env.data ? <div className="text-[13px] text-subtle">Environment endpoint not available.</div> : (
                    <ul className="divide-y divide-border">
                      {env.data.components.map((c) => (
                        <li key={c.id} className="flex items-center gap-3 py-2 text-[13px]">
                          <Dot tone={c.status === "up" ? "pass" : c.status === "degraded" ? "warn" : "fail"} pulse={c.status !== "up"} />
                          <span className="font-medium text-fg">{c.name}</span>
                          <span className="truncate text-[12px] text-subtle">{c.version ?? ""}</span>
                          <span className="ml-auto text-[12px] text-muted num">{c.status === "up" && isNum(c.latency_ms) ? fmtMs(c.latency_ms) : c.status}</span>
                        </li>
                      ))}
                    </ul>
                  )}
            </CardBody>
          </Card>
          <Card>
            <CardHeader title="p99 latency by job" subtitle="Last 12 jobs with a latency result (log scale)" icon={<Gauge />} />
            <CardBody className="pt-0">
              {trend.length < 2 ? <div className="py-8 text-center text-[13px] text-subtle">Not enough finished jobs yet.</div> : (
                <ResponsiveContainer width="100%" height={170}>
                  <BarChart data={trend} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
                    <CartesianGrid stroke="var(--grid)" vertical={false} />
                    <XAxis dataKey="id" hide />
                    <YAxis width={64} tickLine={false} axisLine={false} tick={{ fill: "var(--fg-subtle)", fontSize: 11 }} tickFormatter={(v) => fmtMs(v)}
                      scale="log" domain={[(lo: number) => Math.max(1, lo / 2), (hi: number) => hi * 1.3]} allowDataOverflow />
                    <Tooltip cursor={{ fill: "var(--card-2)" }} isAnimationActive={false}
                      content={({ active, payload }) => active && payload?.[0] ? (
                        <div className="rounded-xl border border-border bg-surface px-3 py-2 text-[12px] shadow-xl">
                          <div className="font-mono text-fg">{(payload[0].payload as { kind: string }).kind} · {(payload[0].payload as { id: string }).id}</div>
                          <div className="text-muted">p99 <b className="text-fg num">{fmtMs(payload[0].value)}</b></div>
                        </div>
                      ) : null} />
                    <Bar dataKey="p99" radius={[4, 4, 0, 0]} isAnimationActive={false} className="cursor-pointer"
                      onClick={(d) => nav(`/jobs/${encodeURIComponent((d as unknown as { id: string }).id)}`)}>
                      {trend.map((t) => <Cell key={t.id} fill={t.ok ? "var(--pass)" : "var(--fail)"} fillOpacity={0.8} />)}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              )}
              <LegendDots className="mt-1" items={[{ label: "p99 within budget in ≥ 99% of seconds", className: "bg-pass" }, { label: "less often", className: "bg-fail" }]} />
            </CardBody>
          </Card>
        </div>
      </div>
    </>
  );
}
