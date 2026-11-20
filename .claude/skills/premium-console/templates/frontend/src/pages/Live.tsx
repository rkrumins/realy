import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Activity, AlertTriangle, ArrowRight, FileBarChart2, Gauge, ListOrdered, OctagonX, Plus, Timer, TriangleAlert, Wifi, WifiOff } from "lucide-react";
import * as React from "react";
import { useNavigate } from "react-router";
import { toast } from "sonner";
import { TimeChart } from "@/components/charts";
import { EmptyState, Kpi, PageHeader } from "@/components/common";
import { LogConsole } from "@/components/feeds";
import { ComponentTiles, StateLegend } from "@/components/state";
import { StepChecklist } from "@/components/steps";
import { Badge, Dot } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardBody, CardHeader } from "@/components/ui/card";
import { Segmented } from "@/components/ui/controls";
import { api } from "@/lib/api";
import { fmtDuration, fmtMs, fmtNum, fmtPct, isNum } from "@/lib/format";
import { useElapsed, useLiveSeries, useNow, useStatus } from "@/lib/hooks";
import { focusJobId, live, logsFor, type LivePoint } from "@/lib/live";
import { LEVEL_TONE, steadyLevelOf } from "@/lib/state";

const WINDOWS = [{ value: "60", label: "1m" }, { value: "300", label: "5m" }, { value: "600", label: "10m" }] as const;
const COMPONENT_NAMES: Record<string, string> = { store: "Job store", worker: "Worker", disk: "Disk" };

function mean(xs: (number | null)[]) {
  const v = xs.filter(isNum);
  return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null;
}
function lastOf(series: LivePoint[], k: keyof LivePoint) {
  for (let i = series.length - 1; i >= 0; i--) {
    const v = series[i][k];
    if (isNum(v)) return { v, t: series[i].t };
  }
  return null;
}

/** Honest freshness: is this live, polled, or old data? */
function Freshness({ lastAt }: { lastAt: number | null }) {
  const now = useNow(1000);
  const age = lastAt ? now - lastAt : null;
  if (!live.connected) {
    return <Badge tone="warn"><WifiOff />reconnecting · polling every 3 s</Badge>;
  }
  if (age !== null && age > 5) return <Badge tone="neutral"><Wifi />live · last sample {fmtDuration(age)} ago</Badge>;
  return <Badge tone="pass"><Wifi />live</Badge>;
}

export default function Live() {
  const nav = useNavigate();
  const qc = useQueryClient();
  const status = useStatus();
  const cur = status?.current ?? null;
  const [win, setWin] = React.useState<"60" | "300" | "600">("300");
  const jobId = cur?.id ?? focusJobId();
  const series = useLiveSeries(Number(win), jobId);
  const elapsed = useElapsed(cur?.started_at);
  const prevJob = React.useRef<string | null>(null);

  React.useEffect(() => {
    // when a job finishes: refresh lists and offer its report
    if (prevJob.current && !cur) {
      const id = prevJob.current;
      qc.invalidateQueries({ queryKey: ["jobs"] });
      qc.invalidateQueries({ queryKey: ["summary"] });
      toast.success("Job finished", { description: id, action: { label: "Open report", onClick: () => nav(`/jobs/${encodeURIComponent(id)}`) } });
    }
    prevJob.current = cur?.id ?? null;
  }, [cur?.id]); // eslint-disable-line react-hooks/exhaustive-deps

  const abort = useMutation({
    mutationFn: () => api.abortJob(cur!.id),
    onSuccess: () => toast.success("Abort requested", { description: "The job stops at its next checkpoint and keeps a partial result." }),
    onError: (e) => toast.error("Abort failed", { description: (e as Error).message }),
  });

  const budget = cur?.budget_ms ?? null;
  const recent = series.slice(-5);
  const tput = mean(recent.map((p) => p.throughput));
  const errors = mean(recent.map((p) => p.errors));
  const p99 = lastOf(series, "p99_ms");
  const p50 = lastOf(series, "p50_ms");
  // hysteresis: the p99 tile only turns amber/red when the last 3 samples agree (one slow second is not an incident)
  const p99Level = steadyLevelOf(series.map((p) => p.p99_ms), budget);
  const progress = elapsed !== null && cur?.planned_s ? Math.min(1, elapsed / cur.planned_s) : null;
  const active = Boolean(cur) || series.length > 0;
  const lastAt = series.length ? series[series.length - 1].t : null;
  const logs = logsFor(jobId);
  const queued = status?.queue?.length ?? 0;

  return (
    <>
      <PageHeader
        eyebrow={<span className="inline-flex items-center gap-2">{cur ? <><Dot tone="accent" pulse />Live · {cur.phase?.toLowerCase()}</> : "Live"}</span>}
        title={cur ? <span className="font-mono">{cur.kind}</span> : "Live monitoring"}
        subtitle={cur ? <span className="inline-flex flex-wrap items-center gap-2"><span className="font-mono text-[12.5px]">{cur.id}</span><Freshness lastAt={lastAt} /></span>
          : jobId ? <span className="inline-flex flex-wrap items-center gap-2">Nothing is running. Showing the last telemetry seen ({jobId}).<Freshness lastAt={lastAt} /></span>
            : "Nothing is running. Start a job to watch its steps, throughput and latency here."}
        actions={<>
          <Segmented value={win} onChange={setWin} options={WINDOWS.map((w) => ({ ...w }))} />
          {cur && <Button variant="danger" onClick={() => abort.mutate()} disabled={abort.isPending} data-testid="abort-job"><OctagonX />Abort</Button>}
          {!cur && jobId && <Button variant="outline" onClick={() => nav(`/jobs/${encodeURIComponent(jobId)}`)}><FileBarChart2 />Report</Button>}
          {!cur && <Button variant="gradient" onClick={() => nav("/jobs?new=1")}><Plus />New job</Button>}
        </>} />

      {cur && progress !== null && (
        <div className="-mt-3 mb-5">
          <div className="mb-1.5 flex justify-between text-[11.5px] text-subtle num">
            <span>{fmtDuration(elapsed)} elapsed</span>
            <span>~{fmtDuration(cur.planned_s)} planned</span>
          </div>
          <div className="relative h-1.5 overflow-hidden rounded-full bg-surface-3" role="progressbar" aria-label="Job progress" aria-valuemin={0} aria-valuemax={100} aria-valuenow={Math.round(progress * 100)}>
            <div className="absolute inset-y-0 left-0 rounded-full bg-[image:var(--gradient-progress)] transition-[width] duration-1000" style={{ width: `${progress * 100}%` }} />
          </div>
        </div>
      )}
      {cur?.abort_reason && (
        <div className="mb-4 flex items-center gap-2 rounded-xl border border-warn/30 bg-warn/10 px-4 py-2.5 text-[13px] text-warn" role="status">
          <AlertTriangle className="size-4" />Abort requested: {cur.abort_reason} · finishing the current item and keeping a partial result
        </div>
      )}

      <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-5">
        <Kpi icon={<Activity />} label="Throughput" value={fmtNum(tput)} sub="items/s · last 5 s" />
        <Kpi icon={<Timer />} label="p99 latency" value={fmtMs(p99?.v)} sub={budget ? `budget ${fmtMs(budget)}` : "per item"} tone={LEVEL_TONE[p99Level]} />
        <Kpi icon={<Gauge />} label="p50 latency" value={fmtMs(p50?.v)} sub={isNum(p50?.v) && budget ? `${fmtPct(p50!.v / budget, 0)} of budget` : "median"} />
        <Kpi icon={<TriangleAlert />} label="Errors" value={fmtNum(errors, errors !== null && errors < 10 ? 1 : 0)} sub="per second · last 5 s" tone={isNum(errors) && errors > 0 ? "warn" : undefined} />
        <Kpi icon={<ListOrdered />} label="Queued" value={fmtNum(queued)} sub={queued ? "waiting for this job" : "queue empty"} className="col-span-2 md:col-span-1" />
      </div>

      {cur && <StepChecklist steps={cur.steps} title="Steps" className="mt-5" />}

      <Card className="mt-5">
        <CardHeader title="Components" subtitle="Explicit state per component: a busy component shows as warming (with the reason), not as slow."
          actions={<span className="text-[12px] text-subtle num">{Object.keys(status?.components ?? {}).length} tracked</span>} />
        <CardBody>
          <ComponentTiles states={status?.components} names={COMPONENT_NAMES} />
          <StateLegend className="mt-3 justify-end" />
        </CardBody>
      </Card>

      {!active ? (
        <EmptyState className="mt-5" icon={<Activity />} title="No live telemetry"
          action={<Button variant="gradient" onClick={() => nav("/jobs?new=1")}>Start a job<ArrowRight /></Button>}>
          Charts and the job log stream here, every second, once a job starts.
        </EmptyState>
      ) : (
        <>
          <div className="mt-5 grid grid-cols-1 gap-5 lg:grid-cols-2">
            <Card>
              <CardHeader title="Throughput" subtitle="items processed per second" />
              <CardBody>
                <TimeChart data={series} syncId="live" legend={false} series={[{ key: "throughput", label: "items/s", kind: "area" }]} />
              </CardBody>
            </Card>
            <Card>
              <CardHeader title="Latency" subtitle="per-second p50 / p99 per item; green band = within budget" />
              <CardBody>
                <TimeChart data={series} syncId="live" budget={budget} band={budget ? { from: 0, to: budget } : null} yFormat={(v) => fmtMs(v)}
                  series={[{ key: "p50_ms", label: "p50" }, { key: "p99_ms", label: "p99" }]} />
              </CardBody>
            </Card>
          </div>
          <Card className="mt-5">
            <CardHeader title="Job log" subtitle="worker and job events; follows the tail until you scroll up" />
            <CardBody><LogConsole lines={logs} height={300} /></CardBody>
          </Card>
        </>
      )}
    </>
  );
}
