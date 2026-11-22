import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Activity, AlertTriangle, ArrowRight, Clock, FileBarChart2, Gauge, Layers, OctagonX, Rocket, SlidersHorizontal, Timer, Zap,
} from "lucide-react";
import * as React from "react";
import { useNavigate, useSearchParams } from "react-router";
import { toast } from "sonner";
import { TimeChart } from "@/components/charts";
import { EmptyState, Kpi, PageHeader, TopologyBadge } from "@/components/common";
import { PipelineBanner, StopAllButton } from "@/components/run-controls";
import { DistributionEditor } from "@/components/editors";
import { DecisionFeed, LogConsole } from "@/components/feeds";
import { TopologyMap, useTopology } from "@/components/Topology";
import { StartupPanel } from "@/components/startup";
import { ghostSeries, ReplayBar, replayRun, startupAt, useReplay } from "@/components/replay";
import { Badge, Dot } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardBody, CardHeader } from "@/components/ui/card";
import { Segmented, Slider } from "@/components/ui/controls";
import { Dialog, DialogContent } from "@/components/ui/dialog";
import { NumberField } from "@/components/ui/input";
import { Select } from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { api } from "@/lib/api";
import type { LatencyDist } from "@/lib/dist";
import { fmtCompact, fmtDuration, fmtMs, fmtNum, fmtPct, isNum } from "@/lib/format";
import { activeRun, useElapsed, useLiveSeries, useStatus } from "@/lib/hooks";
import { decisionsAt, focusRunId, live, logsFor, snapshotsAt, STAGE_SERIES, type LivePoint } from "@/lib/live";
import type { CurrentRun } from "@/lib/types";

const WINDOWS = [{ value: "60", label: "1m" }, { value: "300", label: "5m" }, { value: "600", label: "10m" }] as const;

function mean(xs: (number | null)[]) {
  const v = xs.filter(isNum);
  return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null;
}
function lastOf(series: LivePoint[], k: keyof LivePoint) {
  for (let i = series.length - 1; i >= 0; i--) {
    const v = series[i][k];
    if (isNum(v)) return v;
  }
  return null;
}


// ------------------------------------------------------------------ control drawer
function ControlDrawer({ open, onOpenChange, cur }: { open: boolean; onOpenChange: (v: boolean) => void; cur: CurrentRun | null }) {
  const base = cur?.base_tps ?? 1000;
  const [tps, setTps] = React.useState(() => Math.round(base * (cur?.rate_multiplier ?? 1)));
  React.useEffect(() => setTps(Math.round(base * (cur?.rate_multiplier ?? 1))), [cur?.run_id]); // eslint-disable-line react-hooks/exhaustive-deps
  const [svc, setSvc] = React.useState<LatencyDist | null>({ lognormal: { median_ms: 6, p99_ms: 20 } });
  const [workers, setWorkers] = React.useState<number | undefined>();
  const [maxBatch, setMaxBatch] = React.useState<number | undefined>();
  const [batchWait, setBatchWait] = React.useState<number | undefined>();
  const [maxQueue, setMaxQueue] = React.useState<number | undefined>();
  const [featDelay, setFeatDelay] = React.useState<LatencyDist | null>(null);
  const [budgetMs, setBudgetMs] = React.useState<number | undefined>();
  const [featDl, setFeatDl] = React.useState<number | undefined>();
  const [modelDl, setModelDl] = React.useState<number | undefined>();
  const [onDl, setOnDl] = React.useState("");

  const res = (r: { gateway_admin?: string; model_admin?: string }) => r.gateway_admin ?? r.model_admin;
  const rate = useMutation({
    mutationFn: (v: number) => api.liveRate(v),
    onSuccess: (_r, v) => toast.success(`Target rate → ${fmtNum(v)} TPS`, { description: "Published on bench.control" }),
    onError: (e) => toast.error("Rate change failed", { description: (e as Error).message }),
  });
  const model = useMutation({
    mutationFn: () => {
      const b: Record<string, unknown> = {};
      if (svc) b.service_time = svc;
      if (workers !== undefined) b.workers = workers;
      if (maxBatch !== undefined) b.max_batch = maxBatch;
      if (batchWait !== undefined) b.max_queue_delay_us = batchWait;
      if (maxQueue !== undefined) b.max_queue = maxQueue;
      return api.liveModel(b);
    },
    onSuccess: (r) => toast.success("Model server reconfigured", { description: `admin: ${res(r) ?? "published"}` }),
    onError: (e) => toast.error("Model change failed", { description: (e as Error).message }),
  });
  const scoring = useMutation({
    mutationFn: (b: Record<string, unknown>) => api.liveScoring(b),
    onSuccess: (r) => toast.success("Scoring config updated", { description: `gateway admin: ${res(r) ?? "published"}` }),
    onError: (e) => toast.error("Scoring change failed", { description: (e as Error).message }),
  });
  const abort = useMutation({
    mutationFn: () => api.abort(cur!.job_id ?? cur!.run_id ?? ""),
    onSuccess: () => { toast.success("Abort requested"); onOpenChange(false); },
    onError: (e) => toast.error("Abort failed", { description: (e as Error).message }),
  });
  const maxTps = Math.max(base * 3, 2000);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent side="right" title="Live control" aria-describedby={undefined}
        description={cur ? <>Steering <span className="font-mono">{cur.scenario}</span> while it runs. Changes publish to <span className="font-mono">bench.control</span>.</> : "No run is active: changes apply to the services' standing config."}>
        <Tabs defaultValue="rate" className="flex min-h-0 flex-1 flex-col">
          <TabsList className="mx-5 mt-4">
            <TabsTrigger value="rate">Load</TabsTrigger>
            <TabsTrigger value="model">Model</TabsTrigger>
            <TabsTrigger value="scoring">Features & deadlines</TabsTrigger>
          </TabsList>
          <div className="min-h-0 flex-1 overflow-y-auto px-5 py-5">
            <TabsContent value="rate" className="space-y-5">
              <div>
                <div className="flex items-baseline justify-between">
                  <span className="text-[13px] font-medium text-muted">Target rate</span>
                  <span className="text-[24px] font-semibold tracking-tight num">{fmtNum(tps)} <span className="text-[13px] font-normal text-subtle">TPS</span></span>
                </div>
                <Slider className="mt-3" value={tps} min={10} max={maxTps} step={10} onValueChange={setTps} ariaLabel="Target TPS"
                  onValueCommit={(v) => rate.mutate(v)} />
                <div className="mt-1.5 flex justify-between text-[11px] text-subtle num"><span>10</span><span>scenario {fmtNum(base)}</span><span>{fmtNum(maxTps)}</span></div>
              </div>
              <div className="flex flex-wrap gap-2">
                {[0.5, 1, 1.5, 2, 3].map((m) => (
                  <Button key={m} size="sm" variant="outline" onClick={() => { const v = Math.round(base * m); setTps(v); rate.mutate(v); }}>{m}×</Button>
                ))}
              </div>
              <div className="flex gap-2">
                <NumberField label="Exact TPS" value={tps} onChange={(v) => v !== undefined && setTps(v)} unit="TPS" className="flex-1" />
                <Button className="self-end" onClick={() => rate.mutate(tps)} disabled={rate.isPending} data-testid="apply-rate">Apply</Button>
              </div>
              <p className="text-[12px] text-subtle">The emitter is open-loop: it keeps the schedule regardless of how the pipeline copes, so backlog and schedule lag show saturation honestly.</p>
              {cur && (
                <div className="rounded-xl border border-fail/25 bg-fail/5 p-4">
                  <div className="flex items-center gap-2 text-[13px] font-medium text-fail"><OctagonX className="size-4" />Abort this run</div>
                  <p className="mt-1 text-[12px] text-muted">The runner drains, writes a partial report and marks the run aborted.</p>
                  <Button variant="danger" size="sm" className="mt-3" onClick={() => abort.mutate()} disabled={abort.isPending}>Abort {cur.scenario}</Button>
                </div>
              )}
            </TabsContent>
            <TabsContent value="model" className="space-y-5">
              <DistributionEditor title="Service time per batch" value={svc} onChange={setSvc} allowNone={false} compact />
              <div className="grid grid-cols-2 gap-3">
                <NumberField label="Workers" value={workers} onChange={setWorkers} hint="concurrent batches; 0 = ∞" min={0} />
                <NumberField label="Max batch" value={maxBatch} onChange={setMaxBatch} hint="1 = batching off" min={1} />
                <NumberField label="Batch wait" unit="µs" value={batchWait} onChange={setBatchWait} min={0} />
                <NumberField label="Queue limit" value={maxQueue} onChange={setMaxQueue} hint="0 = unbounded" min={0} />
              </div>
              <Button onClick={() => model.mutate()} disabled={model.isPending} className="w-full">Apply to model server</Button>
            </TabsContent>
            <TabsContent value="scoring" className="space-y-5">
              <DistributionEditor title="Injected feature-lookup delay" value={featDelay} onChange={setFeatDelay} compact />
              <Button variant="secondary" className="w-full" disabled={scoring.isPending}
                onClick={() => scoring.mutate({ feature_lookup_delay: featDelay ?? { fixed: { ms: 0 } } })}>Apply feature delay</Button>
              <div className="h-px bg-border" />
              <div className="grid grid-cols-2 gap-3">
                <NumberField label="Hot-path budget" unit="ms" value={budgetMs} onChange={setBudgetMs} min={1} />
                <NumberField label="Feature deadline" unit="ms" value={featDl} onChange={setFeatDl} min={1} />
                <NumberField label="Model deadline" unit="ms" value={modelDl} onChange={setModelDl} min={1} />
                <label className="flex flex-col gap-1.5 text-[12.5px] font-medium text-muted">On deadline
                  <Select value={onDl} onChange={setOnDl} options={[
                    { value: "", label: "unchanged" }, { value: "FALLBACK_RULES", label: "rules-only fallback" },
                    { value: "TIMEOUT", label: "time out" }, { value: "PROVISIONAL_THEN_ASYNC", label: "provisional, then final" },
                  ]} />
                </label>
              </div>
              <Button className="w-full" disabled={scoring.isPending} onClick={() => {
                const b: Record<string, unknown> = {};
                if (budgetMs) b.budget_ms = budgetMs;
                if (featDl) b.feature_deadline_ms = featDl;
                if (modelDl) b.model_deadline_ms = modelDl;
                if (onDl) b.on_deadline = onDl;
                if (!Object.keys(b).length) return toast.info("Nothing to change");
                scoring.mutate(b);
              }}>Apply deadlines</Button>
            </TabsContent>
          </div>
        </Tabs>
      </DialogContent>
    </Dialog>
  );
}

// ------------------------------------------------------------------ page
export default function Live() {
  const nav = useNavigate();
  const qc = useQueryClient();
  const liveStatus = useStatus();
  // replay: ?run=<id> shows a past (or another) run with the same components, read at a scrubber clock
  const [params, setParams] = useSearchParams();
  const replayId = params.get("run");
  const replay = useReplay(replayId, params.get("t") ? Number(params.get("t")) : null);
  const store = replay.store ?? live;
  const clock = replay.store?.clock ?? null;
  const cur = replay.data ? replayRun(replay.data, clock) : activeRun(liveStatus);
  const status = replay.data
    ? { ...(liveStatus ?? { current: null, queue: [] }), lease: null, blocked_by: null,
        pipeline: liveStatus?.pipeline ? { ...liveStatus.pipeline, nodes: {}, startup: startupAt(replay.data.startup, clock) }
          : { topology: replay.data.run.topology ?? "", run_id: replayId, phase: cur?.phase ?? null, nodes: {}, startup: startupAt(replay.data.startup, clock) } }
    : liveStatus;
  const [win, setWin] = React.useState<"60" | "300" | "600">("300");
  const [drawer, setDrawer] = React.useState(false);
  const runId = cur?.run_id ?? focusRunId();
  const series = useLiveSeries(Number(win), runId, store);
  // warm-up + measurement are timed from the start of live traffic, not from the launch: a large world
  // spends minutes preparing (world, history replay, catch-up) and that shows as startup steps instead
  const liveElapsed = useElapsed(replay.data ? null : cur?.measure_started_at ?? null);
  const livePrep = useElapsed(replay.data ? null : cur?.started_at ?? null);
  const elapsed = replay.data ? (cur?.measure_started_at && clock !== null && clock >= cur.measure_started_at ? clock - cur.measure_started_at : null) : liveElapsed;
  const prepElapsed = replay.data ? (cur?.started_at && clock !== null ? clock - cur.started_at : null) : livePrep;
  const activeStep = status?.pipeline?.startup?.steps?.find((s) => s.status === "running");
  // overlay: the previous run of the same scenario, aligned on the start of traffic
  const runsList = useQuery({ queryKey: ["runs-picker"], queryFn: () => api.runs(200), staleTime: 15_000 });
  const finished = (runsList.data?.history ?? []).filter((r) => ["done", "aborted", "failed"].includes(r.status) && r.run_id);
  const [ghostOn, setGhostOn] = React.useState(false);
  const ghostId = finished.find((r) => r.scenario === cur?.scenario && r.run_id !== cur?.run_id)?.run_id ?? null;
  const ghost = useReplay(ghostOn ? ghostId : null);
  const topo = useTopology(cur?.topology ?? (series.length ? undefined : undefined), (cur?.flows?.length ?? 0) > 0);
  const prevRun = React.useRef<string | null>(null);

  React.useEffect(() => {
    // when a run finishes, refresh lists and offer the report
    if (replay.data) return;
    if (prevRun.current && !cur) {
      const id = prevRun.current;
      qc.invalidateQueries({ queryKey: ["runs"] });
      qc.invalidateQueries({ queryKey: ["summary"] });
      toast.success("Run finished", { description: id, action: { label: "Open report", onClick: () => nav(`/runs/${encodeURIComponent(id)}`) } });
    }
    prevRun.current = cur?.run_id ?? null;
  }, [cur?.run_id]); // eslint-disable-line react-hooks/exhaustive-deps

  const budget = cur?.budget?.e2e_p99_ms ?? 150;
  // idle: summarise the last seconds that carried data (the run that just finished)
  const recent = (cur ? series : series.filter((p) => p.emitted !== null || p.decided !== null)).slice(-5);
  const emitted = mean(recent.map((p) => p.emitted));
  const decided = mean(recent.map((p) => p.decided));
  const p99 = lastOf(series, "e2e_p99");
  const p50 = lastOf(series, "e2e_p50");
  const backlog = lastOf(series, "backlog");
  const degraded = mean(recent.map((p) => (isNum(p.degraded) && isNum(p.decided) && p.decided > 0 ? p.degraded / p.decided : null)));
  const target = cur?.base_tps ? cur.base_tps * (cur.rate_multiplier || 1) : null;
  const total = cur ? (cur.warmup_s ?? 0) + (cur.duration_s ?? 0) : null;
  const progress = elapsed !== null && total ? Math.min(1, elapsed / total) : null;
  const active = Boolean(cur) || series.some((p) => p.emitted);
  const snaps = snapshotsAt(store, runId);
  const graph = topo.data;
  // charts: from the run start while running; when idle, the span that carried emitter/collector data
  const ghostPts = React.useMemo(() => ghostSeries(ghost.store, ghost.data?.run.measure_started_at ?? null, cur?.measure_started_at ?? null),
    [ghost.store, ghost.data, cur?.measure_started_at]); // eslint-disable-line react-hooks/exhaustive-deps
  const shown = React.useMemo(() => {
    let pts: LivePoint[];
    if (cur?.started_at) pts = series.filter((p) => p.t >= (cur.started_at as number) - 5);
    else {
      const idx = series.flatMap((p, i) => (p.emitted !== null || p.decided !== null ? [i] : []));
      pts = idx.length ? series.slice(Math.max(0, idx[0] - 5), idx[idx.length - 1] + 6) : series;
    }
    if (!ghostPts.size) return pts;
    return pts.map((p) => {
      const g = ghostPts.get(p.t);
      return { ...p, g_decided: g?.decided ?? null, g_e2e_p99: g?.e2e_p99 ?? null };
    });
  }, [series, cur?.started_at, ghostPts]);
  const stageSeries = STAGE_SERIES.map((s) => ({ key: s.key, label: s.label, stack: "s" }))
    .filter((s) => shown.some((p) => isNum(p.stages[s.key])));
  const flat = shown.map((p) => ({ ...p, ...Object.fromEntries(STAGE_SERIES.map((s) => [s.key, p.stages[s.key] ?? null])) }));
  const logs = logsFor(runId, store);
  const t0 = cur?.measure_started_at ?? null;
  const warmupMark = t0 && cur?.warmup_s ? [{ x: Math.round(t0 + cur.warmup_s), label: "measure" }] : undefined;

  return (
    <>
      <PageHeader eyebrow={<span className="inline-flex items-center gap-2">{replay.data ? <><Dot tone="info" />Replay · {cur?.phase?.toLowerCase()}</>
          : cur ? <><Dot tone="accent" pulse />Live · {cur.phase?.toLowerCase()}</> : "Live"}</span>}
        title={cur ? <span className="font-mono">{cur.scenario}</span> : "Live pipeline"}
        subtitle={cur ? <span className="inline-flex flex-wrap items-center gap-2"><TopologyBadge topology={cur.topology} /><span className="font-mono text-[12.5px]">{cur.run_id ?? "preparing…"}</span>
            {replay.data?.run.verdict && <Badge tone={replay.data.run.verdict === "PASS" ? "pass" : "fail"}>{replay.data.run.verdict}</Badge>}</span>
          : runId ? <>No run is active. Showing the last telemetry seen ({runId}).</> : "Nothing is running. Launch a scenario to watch it flow through the pipeline."}
        actions={<>
          <Select size="sm" className="w-[230px]" value={replayId ?? ""} ariaLabel="Run to show"
            onChange={(v) => setParams((p) => { if (v) p.set("run", v); else p.delete("run"); p.delete("t"); return p; }, { replace: true })}
            options={[{ value: "", label: "Live now" }, ...finished.slice(0, 60).map((r) => ({ value: r.run_id!, label: <span className="font-mono text-[12px]">{r.run_id}</span>, hint: r.verdict ?? r.status }))]} />
          <Segmented value={win} onChange={setWin} options={WINDOWS.map((w) => ({ ...w }))} />
          {ghostId && <Button variant={ghostOn ? "secondary" : "outline"} onClick={() => setGhostOn((g) => !g)} title={`overlay ${ghostId}, aligned on the start of traffic`} data-testid="ghost-toggle">{ghostOn ? "Hide previous" : "Overlay previous"}</Button>}
          {!replay.data && <Button variant="secondary" onClick={() => setDrawer(true)} data-testid="open-controls"><SlidersHorizontal />Controls</Button>}
          {(replay.data || !cur) && runId && <Button variant="outline" onClick={() => nav(`/runs/${encodeURIComponent(runId)}`)}><FileBarChart2 />Report</Button>}
          {!replay.data && (cur || status?.lease) && <StopAllButton />}
          {!replay.data && !cur && <Button variant="gradient" onClick={() => nav("/launch")}><Rocket />Launch</Button>}
        </>} />
      {replay.store && replay.data && <ReplayBar store={replay.store} data={replay.data} />}
      {replay.data?.note && <div className="mb-4 rounded-xl border border-warn/30 bg-warn/8 px-4 py-2.5 text-[13px]">{replay.data.note}</div>}
      {replayId && replay.error && <div className="mb-4 rounded-xl border border-fail/30 bg-fail/8 px-4 py-2.5 text-[13px]">Could not load the run's telemetry: {(replay.error as Error).message}</div>}

      {cur && progress === null && (
        <div className="-mt-3 mb-5" data-testid="preparing">
          <div className="mb-1.5 flex flex-wrap justify-between gap-2 text-[11.5px] text-subtle num">
            <span>preparing for {fmtDuration(prepElapsed)}{activeStep ? ` · step ${activeStep.index}/${status!.pipeline!.startup!.steps.length}: ${activeStep.label.toLowerCase()}` : ""}
              {activeStep?.progress?.eta_s ? ` · ETA ${fmtDuration(activeStep.progress.eta_s)}` : ""}</span>
            <span>then {fmtDuration(total)} of traffic{cur.warmup_s ? ` (warm-up ${fmtDuration(cur.warmup_s)})` : ""}</span>
          </div>
          <div className="relative h-1.5 overflow-hidden rounded-full bg-surface-3">
            <div className="absolute inset-y-0 left-0 rounded-full bg-info/60 transition-[width] duration-1000"
              style={{ width: `${activeStep?.progress?.total ? Math.min(100, (100 * (activeStep.progress.done ?? 0)) / activeStep.progress.total) : 0}%` }} />
          </div>
        </div>
      )}
      {cur && progress !== null && (
        <div className="-mt-3 mb-5">
          <div className="mb-1.5 flex justify-between text-[11.5px] text-subtle num">
            <span>{fmtDuration(elapsed)} elapsed{cur.warmup_s ? ` · warm-up ${fmtDuration(cur.warmup_s)}` : ""}</span>
            <span>{fmtDuration(total)} planned</span>
          </div>
          <div className="relative h-1.5 overflow-hidden rounded-full bg-surface-3">
            {cur.warmup_s && total ? <div className="absolute inset-y-0 left-0 bg-warn/25" style={{ width: `${(cur.warmup_s / total) * 100}%` }} /> : null}
            <div className="absolute inset-y-0 left-0 rounded-full bg-[linear-gradient(90deg,#6366f1,#22d3ee)] transition-[width] duration-1000" style={{ width: `${progress * 100}%` }} />
          </div>
        </div>
      )}
      {!replay.data && <PipelineBanner status={status} shown />}
      {cur?.abort_reason && (
        <div className="mb-4 flex items-center gap-2 rounded-xl border border-warn/30 bg-warn/10 px-4 py-2.5 text-[13px] text-warn"><AlertTriangle className="size-4" />Abort requested{cur.abort_kind ? ` (${cur.abort_kind})` : ""}: {cur.abort_reason} · draining in-flight decisions and writing a partial report</div>
      )}

      <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6">
        <Kpi icon={<Zap />} label="Emitted" value={fmtNum(emitted)} sub={target ? `target ${fmtNum(target)} TPS` : "events/s"} />
        <Kpi icon={<Activity />} label="Decided" value={fmtNum(decided)} sub="decisions/s"
          tone={isNum(emitted) && isNum(decided) && emitted > 0 && decided / emitted < 0.9 ? "warn" : undefined} />
        <Kpi icon={<Timer />} label="e2e p99" value={fmtMs(p99)} sub={`budget ${fmtMs(budget)}`} tone={isNum(p99) ? (p99 > budget ? "fail" : "pass") : undefined} />
        <Kpi icon={<Gauge />} label="e2e p50" value={fmtMs(p50)} sub={isNum(p50) ? `${fmtPct(p50 / budget, 0)} of budget` : "median"} />
        <Kpi icon={<Layers />} label="Backlog" value={fmtCompact(backlog)} sub="emitted − decided" tone={isNum(backlog) && isNum(emitted) && backlog > Math.max(1000, emitted * 2) ? "warn" : undefined} />
        <Kpi icon={<Clock />} label="Degraded" value={fmtPct(degraded, 2)} sub="rules-only fallback" tone={isNum(degraded) && degraded > 0.01 ? "warn" : undefined} />
      </div>

      {cur && <StartupPanel startup={status?.pipeline?.startup} phase={cur.phase} />}

      <Card className="mt-5">
        <CardHeader title="Pipeline topology"
          subtitle="Live events/s and p99 per hop. Colour = state (starting, warming, down) or, when ready, the share of the latency budget (freshness: its own SLO)"
          actions={graph && <Badge tone="neutral" className="font-mono">{graph.topology}</Badge>} />
        <CardBody>
          {!active && !cur ? (
            <div className="relative">
              <TopologyMap graph={graph} snapshots={[]} budgetMs={budget} active={false} className="opacity-60" states={status?.pipeline?.nodes} />
            </div>
          ) : <TopologyMap graph={graph} snapshots={snaps} budgetMs={budget} active states={status?.pipeline?.nodes}
            judge={!cur || ["WARMUP", "MEASURE", "DRAIN", "COMPLETE"].includes(cur.phase ?? "")} />}
        </CardBody>
      </Card>

      {!active && !cur ? (
        <EmptyState className="mt-5" icon={<Rocket />} title="No live telemetry"
          action={<Button variant="gradient" onClick={() => nav("/launch")}>Launch a scenario<ArrowRight /></Button>}>
          Charts, the decision feed and the run log stream here once a run starts.
        </EmptyState>
      ) : (
        <>
          <div className="mt-5 grid grid-cols-1 gap-5 lg:grid-cols-2">
            <Card>
              <CardHeader title="Throughput funnel" subtitle="events/s per second" />
              <CardBody>
                <TimeChart data={shown} syncId="live" markers={warmupMark} series={[
                  { key: "emitted", label: "emitted" }, { key: "acked", label: "acked" }, { key: "decided", label: "decided" },
                  ...(ghostPts.size ? [{ key: "g_decided", label: "decided (previous run)", dashed: true, color: "var(--fg-subtle)" }] : []),
                ]} yFormat={(v) => fmtCompact(v)} />
              </CardBody>
            </Card>
            <Card>
              <CardHeader title="End-to-end latency" subtitle="per-second percentiles, intended send → decision collected" />
              <CardBody>
                <TimeChart data={shown} syncId="live" budget={budget} band={{ from: 0, to: budget }} markers={warmupMark} series={[
                  { key: "e2e_p50", label: "p50" }, { key: "e2e_p99", label: "p99" }, { key: "e2e_p999", label: "p99.9", dashed: true },
                  ...(ghostPts.size ? [{ key: "g_e2e_p99", label: "p99 (previous run)", dashed: true, color: "var(--fg-subtle)" }] : []),
                ]} yFormat={(v) => fmtMs(v)} />
              </CardBody>
            </Card>
            <Card>
              <CardHeader title="Per-stage p99" subtitle="stacked stage p99s: where the latency budget goes" />
              <CardBody>
                <TimeChart data={flat} syncId="live" series={stageSeries} yFormat={(v) => fmtMs(v)} empty="Stage timings appear once decisions are collected." />
              </CardBody>
            </Card>
            <div className="grid gap-5">
              <Card>
                <CardHeader title="Schedule lag" subtitle="p99 of actual vs intended send time (emitter health)" />
                <CardBody><TimeChart data={shown} syncId="live" height={110} series={[{ key: "sched_p99", label: "schedule lag p99" }]} yFormat={(v) => fmtMs(v)} legend={false} /></CardBody>
              </Card>
              <Card>
                <CardHeader title="Backlog" subtitle="events emitted but not yet decided" />
                <CardBody><TimeChart data={shown} syncId="live" height={110} series={[{ key: "backlog", label: "backlog", kind: "area", color: "var(--chart-4)" }]} legend={false} /></CardBody>
              </Card>
            </div>
          </div>

          <div className="mt-5 grid grid-cols-1 gap-5 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.2fr)]">
            <Card className="flex flex-col">
              <CardHeader title="Decision feed" subtitle="sampled live decisions · click one to inspect it" />
              <CardBody className="flex min-h-0 flex-1 flex-col">
                <DecisionFeed decisions={decisionsAt(store)} runId={cur?.run_id ?? null} className="h-[380px]" />
              </CardBody>
            </Card>
            <Card>
              <CardHeader title="Run log" subtitle="runner, emitter and console events" />
              <CardBody><LogConsole lines={logs} height={340} /></CardBody>
            </Card>
          </div>
        </>
      )}
      <ControlDrawer open={drawer} onOpenChange={setDrawer} cur={cur} />
    </>
  );
}
