import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { CheckCircle2, Copy, Download, FileText, MoreHorizontal, OctagonX, Radio, RotateCcw, XCircle } from "lucide-react";
import { useNavigate, useParams } from "react-router";
import { toast } from "sonner";
import { secs, TimeChart } from "@/components/charts";
import { BackLink, EmptyState, ErrorState, Kpi, SectionTitle, Stat, StatusBadge } from "@/components/common";
import { Insights } from "@/components/insights";
import { StepChecklist } from "@/components/steps";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardBody, CardHeader } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/controls";
import { Dropdown, DropdownContent, DropdownItem, DropdownTrigger } from "@/components/ui/dropdown";
import { api } from "@/lib/api";
import { cn } from "@/lib/cn";
import { fmtCompact, fmtDuration, fmtMs, fmtNum, fmtPct, fmtTime, isNum, titleCase } from "@/lib/format";
import type { Job } from "@/lib/types";

const SECTIONS = [
  { id: "summary", label: "Summary" }, { id: "timeline", label: "Over time" }, { id: "steps", label: "Steps" },
  { id: "parameters", label: "Parameters" }, { id: "log", label: "Log" },
];

function download(name: string, data: unknown) {
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = name;
  a.click();
  setTimeout(() => URL.revokeObjectURL(a.href), 1000);
}

/** The verdict first: status, what ran, when, how long, and the actions you take from here. */
function Hero({ j }: { j: Job }) {
  const nav = useNavigate();
  const qc = useQueryClient();
  const r = j.result;
  const good = j.status === "done" && (r?.within_budget ?? 1) >= 0.99;
  const bad = j.status === "failed" || (j.status === "done" && (r?.within_budget ?? 1) < 0.99);
  const active = j.status === "running" || j.status === "queued";
  const abort = useMutation({
    mutationFn: () => api.abortJob(j.id),
    onSuccess: (x) => { toast.success(`Abort: ${x.result}`); qc.invalidateQueries({ queryKey: ["job", j.id] }); },
    onError: (e) => toast.error("Abort failed", { description: (e as Error).message }),
  });
  const rerun = useMutation({
    mutationFn: () => api.createJob(j.kind, j.params),
    onSuccess: (x) => { toast.success("Queued again", { description: x.id }); nav("/live"); },
    onError: (e) => toast.error("Could not queue", { description: (e as Error).message }),
  });
  return (
    <div className={cn(
      "relative mb-6 overflow-hidden rounded-2xl border p-5 shadow-card md:p-6",
      good ? "border-pass/25 bg-[linear-gradient(115deg,color-mix(in_oklab,var(--pass)_13%,var(--card))_0%,var(--card)_55%)]"
        : bad ? "border-fail/25 bg-[linear-gradient(115deg,color-mix(in_oklab,var(--fail)_13%,var(--card))_0%,var(--card)_55%)]"
          : active ? "border-accent/30 bg-[linear-gradient(115deg,color-mix(in_oklab,var(--accent)_12%,var(--card))_0%,var(--card)_55%)]"
            : "border-border bg-surface",
    )}>
      <div className="flex flex-wrap items-start gap-5">
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <StatusBadge status={j.status} />
            {/* status says whether it finished; the verdict says whether it was good */}
            {r && isNum(r.within_budget) && (r.within_budget >= 0.99
              ? <Badge tone="pass" className="font-semibold"><CheckCircle2 />within budget</Badge>
              : <Badge tone="fail" className="font-semibold"><XCircle />over budget · {fmtPct(1 - r.within_budget, 0)} of seconds</Badge>)}
            {r?.failed ? <Badge tone="warn">{fmtNum(r.failed)} failed</Badge> : null}
          </div>
          <h1 className="mt-3 font-mono text-[22px] font-semibold tracking-tight text-fg md:text-[26px]">{titleCase(j.kind)}</h1>
          <div className="mt-1 flex flex-wrap gap-x-4 gap-y-1 text-[12.5px] text-muted">
            <span className="font-mono">{j.id}</span>
            <span>queued {fmtTime(j.created_at)}</span>
            {j.started_at && j.finished_at && <span>took {fmtDuration(j.finished_at - j.started_at)}</span>}
          </div>
          {(j.error || j.abort_reason) && (
            <div className={cn("mt-3 rounded-xl border px-3.5 py-2.5 text-[13px]", j.error ? "border-fail/30 bg-fail/[0.06] text-fail" : "border-warn/30 bg-warn/[0.07] text-warn")}>
              {j.error ?? j.abort_reason}
            </div>
          )}
        </div>
        <div className="flex flex-wrap gap-2">
          {active ? <>
            <Button variant="secondary" onClick={() => nav("/live")}><Radio />Watch live</Button>
            <Button variant="danger" onClick={() => abort.mutate()} disabled={abort.isPending}><OctagonX />{j.status === "running" ? "Abort" : "Cancel"}</Button>
          </> : <Button variant="secondary" onClick={() => rerun.mutate()} disabled={rerun.isPending}><RotateCcw />Run again</Button>}
          <Dropdown>
            <DropdownTrigger asChild><Button variant="outline" size="icon" aria-label="More actions"><MoreHorizontal /></Button></DropdownTrigger>
            <DropdownContent align="end">
              <DropdownItem onSelect={() => download(`${j.id}.json`, j)}><Download />Download job.json</DropdownItem>
              <DropdownItem onSelect={() => { void navigator.clipboard?.writeText(j.id); toast.success("Job id copied"); }}><Copy />Copy job id</DropdownItem>
            </DropdownContent>
          </Dropdown>
        </div>
      </div>
    </div>
  );
}

/** Recommended addition: a jump bar for long report pages (sections carry ids + scroll-mt-20). */
function JumpBar() {
  return (
    <nav aria-label="On this page" className="-mt-2 mb-6 flex flex-wrap gap-1.5">
      {SECTIONS.map((s) => (
        <a key={s.id} href={`#${s.id}`} className="rounded-full border border-border bg-surface-2/60 px-2.5 py-1 text-[12px] text-muted hover:border-border-strong hover:text-fg">{s.label}</a>
      ))}
    </nav>
  );
}

export default function JobDetail() {
  const { id = "" } = useParams();
  // keep polling until the job is final; then the page is static
  const q = useQuery({
    queryKey: ["job", id], queryFn: () => api.job(id),
    refetchInterval: (qq) => (qq.state.data && !["queued", "running"].includes(qq.state.data.status) ? false : 2000),
  });
  const kinds = useQuery({ queryKey: ["kinds"], queryFn: api.kinds, staleTime: 60_000 });
  if (q.isLoading) return <div className="space-y-4"><Skeleton className="h-40 rounded-2xl" /><div className="grid grid-cols-2 gap-3 md:grid-cols-4">{Array.from({ length: 4 }, (_, i) => <Skeleton key={i} className="h-24 rounded-2xl" />)}</div><Skeleton className="h-80 rounded-2xl" /></div>;
  if (q.error) return <><BackLink to="/jobs">All jobs</BackLink><ErrorState error={q.error} title="Job not found" /></>;
  const j = q.data!;
  const r = j.result;
  const spec = kinds.data?.find((k) => k.kind === j.kind)?.params;
  const budget = r?.budget_ms ?? null;

  return (
    <>
      <BackLink to="/jobs">All jobs</BackLink>
      <Hero j={j} />
      <JumpBar />
      <SectionTitle id="summary">Summary</SectionTitle>
      {!r ? (
        <EmptyState title="No result yet" icon={<FileText />}>
          {j.status === "queued" || j.status === "running" ? "The job is still in progress; this page refreshes automatically." : "This job finished without a result."}
        </EmptyState>
      ) : (
        <>
          <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
            <Kpi label="Processed" value={fmtCompact(r.processed)} sub={r.failed ? `${fmtNum(r.failed)} failed` : "no failures"} tone={r.failed ? "warn" : undefined} />
            <Kpi label="Throughput" value={fmtNum(r.throughput)} sub="items/s, mean over the job" />
            <Kpi label="p99 latency" value={fmtMs(r.p99_ms)} sub={budget ? `budget ${fmtMs(budget)} · p50 ${fmtMs(r.p50_ms)}` : `p50 ${fmtMs(r.p50_ms)}`}
              tone={isNum(r.p99_ms) && isNum(budget) ? (r.p99_ms > budget ? "fail" : "pass") : undefined} />
            <Kpi label="Within budget" value={fmtPct(r.within_budget, 1)} sub="seconds whose p99 met the budget"
              tone={isNum(r.within_budget) ? (r.within_budget >= 0.99 ? "pass" : r.within_budget >= 0.9 ? "warn" : "fail") : undefined} />
          </div>
          <Insights items={r.insights} className="mt-5" />
        </>
      )}

      <SectionTitle id="timeline">Over time</SectionTitle>
      <div className="grid grid-cols-1 gap-5 lg:grid-cols-2">
        <Card>
          <CardHeader title="Throughput" subtitle="items/s per second since the job started" />
          <CardBody><TimeChart data={r?.series ?? []} xFormat={secs} syncId="job" legend={false}
            series={[{ key: "throughput", label: "items/s", kind: "area" }]} empty="No per-second samples for this job." /></CardBody>
        </Card>
        <Card>
          <CardHeader title="Latency" subtitle="per-second percentiles; green band = within budget" />
          <CardBody><TimeChart data={r?.series ?? []} xFormat={secs} syncId="job" budget={budget} band={budget ? { from: 0, to: budget } : null}
            yFormat={(v) => fmtMs(v)} series={[{ key: "p50_ms", label: "p50" }, { key: "p99_ms", label: "p99" }]} empty="No per-second samples for this job." /></CardBody>
        </Card>
      </div>

      <SectionTitle id="steps">Steps</SectionTitle>
      {j.steps?.length ? <StepChecklist steps={j.steps} title="Step timings" defaultOpen /> : <div className="text-[13px] text-subtle">This job reported no steps.</div>}

      <SectionTitle id="parameters">Parameters</SectionTitle>
      <Card><CardBody className="pt-4">
        {Object.keys(j.params ?? {}).length === 0 ? <div className="text-[13px] text-subtle">Defaults only.</div>
          : Object.entries(j.params).map(([k, v]) => {
            const p = spec?.find((x) => x.name === k);   // label + unit from the backend's registry
            return <Stat key={k} label={p?.label ?? titleCase(k)} value={`${isNum(v) ? fmtNum(v, v % 1 ? 2 : 0) : String(v)}${p?.unit ? ` ${p.unit}` : ""}`} mono />;
          })}
      </CardBody></Card>

      <SectionTitle id="log">Log</SectionTitle>
      {j.logs?.length ? (
        <pre className="max-h-80 overflow-auto rounded-2xl border border-border bg-surface-2 p-4 font-mono text-[11.5px] leading-5 text-muted">{j.logs.join("\n")}</pre>
      ) : <div className="text-[13px] text-subtle">No log lines kept for this job (logs live in memory and reset when the service restarts).</div>}
    </>
  );
}
