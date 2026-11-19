import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { CalendarClock, GitCompareArrows, History, ListOrdered, Rocket, Search, ShieldCheck, ShieldX, Trash2, X } from "lucide-react";
import * as React from "react";
import { useNavigate, useSearchParams } from "react-router";
import { toast } from "sonner";
import { EmptyState, ErrorState, PageHeader, StatusBadge, TopologyBadge, topologyLabel, VerdictBadge } from "@/components/common";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Checkbox, Skeleton } from "@/components/ui/controls";
import { Input } from "@/components/ui/input";
import { Select } from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { api } from "@/lib/api";
import { cn } from "@/lib/cn";
import { fmtAgo, fmtCompact, fmtDuration, fmtMs, fmtNum, fmtPct, fmtTime } from "@/lib/format";
import { TOPOLOGIES, type Job, type RunSummary, type Schedule } from "@/lib/types";

const th = "px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wider text-subtle whitespace-nowrap";

function HistoryTable({ rows, selected, toggle }: { rows: RunSummary[]; selected: string[]; toggle: (id: string) => void }) {
  const nav = useNavigate();
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[980px] text-[13px]">
        <thead className="border-b border-border bg-surface-2/40">
          <tr>
            <th className="w-10 px-4 py-2" />
            <th className={th}>Verdict</th><th className={th}>Run</th><th className={th}>Topology</th>
            <th className={cn(th, "text-right")}>TPS</th><th className={cn(th, "text-right")}>p50</th><th className={cn(th, "text-right")}>p99</th>
            <th className={cn(th, "text-right")}>In budget</th><th className={cn(th, "text-right")}>Decided</th><th className={th}>Verify</th>
            <th className={cn(th, "pr-5 text-right")}>Started</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => {
            const sel = selected.includes(r.run_id);
            return (
              <tr key={r.run_id} onClick={() => nav(`/runs/${encodeURIComponent(r.run_id)}`)}
                className={cn("cursor-pointer border-b border-border last:border-0 transition-colors hover:bg-surface-2/60", sel && "bg-accent/6")}>
                <td className="px-4 py-2.5" onClick={(e) => e.stopPropagation()}>
                  <Checkbox checked={sel} onCheckedChange={() => toggle(r.run_id)} />
                </td>
                <td className="px-3 py-2.5">{r.verdict ? <VerdictBadge verdict={r.verdict} /> : <StatusBadge status={r.status} phase={r.phase} />}</td>
                <td className="max-w-[300px] px-3 py-2.5">
                  <div className="truncate font-mono text-[12.5px] text-fg">{r.scenario}</div>
                  <div className="truncate font-mono text-[11px] text-subtle">{r.run_id}</div>
                </td>
                <td className="px-3 py-2.5"><TopologyBadge topology={r.topology} /></td>
                <td className="px-3 py-2.5 text-right num">{fmtNum(r.achieved_tps)}</td>
                <td className="px-3 py-2.5 text-right num text-muted">{fmtMs(r.e2e_p50_ms)}</td>
                <td className="px-3 py-2.5 text-right num">{fmtMs(r.e2e_p99_ms)}</td>
                <td className="px-3 py-2.5 text-right num">{fmtPct(r.within_budget, 2)}</td>
                <td className="px-3 py-2.5 text-right num text-muted">{fmtCompact(r.decided)}{r.missing ? <span className="text-fail"> −{fmtCompact(r.missing)}</span> : null}</td>
                <td className="px-3 py-2.5">
                  {r.verify_passed === true ? <Badge tone="pass"><ShieldCheck />verified</Badge>
                    : r.verify_passed === false ? <Badge tone="fail"><ShieldX />mismatch</Badge> : <span className="text-subtle">—</span>}
                </td>
                <td className="px-3 py-2.5 pr-5 text-right text-muted" title={fmtTime(r.started_at)}>{fmtAgo(r.started_at)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function JobRow({ j, onAbort }: { j: Job; onAbort?: () => void }) {
  return (
    <li className="flex flex-wrap items-center gap-x-3 gap-y-1 px-5 py-3 text-[13px]">
      <StatusBadge status={j.status} />
      <span className="font-mono text-fg">{j.scenario}</span>
      {j.run_id && <span className="font-mono text-[11.5px] text-subtle">{j.run_id}</span>}
      {Object.keys(j.overrides || {}).length > 0 && <span className="truncate font-mono text-[11px] text-subtle">{JSON.stringify(j.overrides)}</span>}
      {j.error && <span className="text-[12px] text-fail">{j.error}</span>}
      <span className="ml-auto text-[12px] text-subtle">{j.finished_at ? `finished ${fmtAgo(j.finished_at)}` : j.started_at ? `started ${fmtAgo(j.started_at)}` : `queued ${fmtAgo(j.created_at)}`}</span>
      {onAbort && <Button size="sm" variant="ghost" onClick={onAbort}><X />{j.status === "running" ? "Abort" : "Cancel"}</Button>}
    </li>
  );
}

export default function Runs() {
  const [params, setParams] = useSearchParams();
  const nav = useNavigate();
  const qc = useQueryClient();
  const tab = params.get("tab") || "history";
  const runs = useQuery({ queryKey: ["runs"], queryFn: () => api.runs(500), refetchInterval: 5000 });
  const schedules = useQuery({ queryKey: ["schedules"], queryFn: api.schedules, refetchInterval: 15_000 });
  const [q, setQ] = React.useState("");
  const [verdict, setVerdict] = React.useState("");
  const [topology, setTopology] = React.useState("");
  const [selected, setSelected] = React.useState<string[]>([]);
  const toggle = (id: string) => setSelected((s) => (s.includes(id) ? s.filter((x) => x !== id) : [...s, id].slice(-2)));

  const abort = useMutation({
    mutationFn: (id: string) => api.abort(id),
    onSuccess: (r) => { toast.success(`Abort: ${r.result}`); qc.invalidateQueries({ queryKey: ["runs"] }); },
    onError: (e) => toast.error("Abort failed", { description: (e as Error).message }),
  });
  const del = useMutation({
    mutationFn: (id: string) => api.deleteSchedule(id),
    onSuccess: () => { toast.success("Schedule deleted"); qc.invalidateQueries({ queryKey: ["schedules"] }); },
  });

  const history = (runs.data?.history ?? []).filter((r) =>
    (!q || `${r.run_id} ${r.scenario}`.toLowerCase().includes(q.toLowerCase()))
    && (!verdict || (verdict === "none" ? !r.verdict : r.verdict === verdict))
    && (!topology || r.topology === topology));
  const queue = runs.data?.queue ?? [];
  const jobs = runs.data?.jobs ?? [];
  const running = jobs.filter((j) => j.status === "running");

  return (
    <>
      <PageHeader eyebrow="Runs" title="Runs, queue and schedules"
        subtitle="Every run keeps its manifest, Parquet results, report and (optionally) verification. Select two runs to compare them."
        actions={<>
          <Button variant="secondary" disabled={selected.length !== 2} onClick={() => nav(`/compare?a=${encodeURIComponent(selected[0])}&b=${encodeURIComponent(selected[1])}`)}>
            <GitCompareArrows />Compare{selected.length ? ` (${selected.length}/2)` : ""}
          </Button>
          <Button variant="gradient" onClick={() => nav("/launch")}><Rocket />New run</Button>
        </>} />
      <Tabs value={tab} onValueChange={(t) => setParams((p) => { p.set("tab", t); return p; }, { replace: true })}>
        <TabsList>
          <TabsTrigger value="history"><History className="size-3.5" />History <span className="text-subtle">{runs.data?.history.length ?? ""}</span></TabsTrigger>
          <TabsTrigger value="queue"><ListOrdered className="size-3.5" />Queue <span className="text-subtle">{queue.length + running.length || ""}</span></TabsTrigger>
          <TabsTrigger value="schedules"><CalendarClock className="size-3.5" />Schedules <span className="text-subtle">{schedules.data?.length || ""}</span></TabsTrigger>
        </TabsList>

        <TabsContent value="history" className="mt-4">
          <div className="mb-3 flex flex-wrap items-center gap-2">
            <div className="relative w-full max-w-xs">
              <Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-subtle" />
              <Input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Search run id or scenario…" className="pl-9" />
            </div>
            <Select className="w-36" value={verdict} onChange={setVerdict} ariaLabel="Verdict filter" options={[
              { value: "", label: "any verdict" }, { value: "PASS", label: "PASS" }, { value: "FAIL", label: "FAIL" }, { value: "none", label: "no verdict" },
            ]} />
            <Select className="w-44" value={topology} onChange={setTopology} ariaLabel="Topology filter"
              options={[{ value: "", label: "any topology" }, ...TOPOLOGIES.map((t) => ({ value: t, label: topologyLabel(t) }))]} />
            {selected.length > 0 && <Button variant="ghost" size="sm" onClick={() => setSelected([])}><X />Clear selection</Button>}
            <span className="ml-auto text-[12.5px] text-subtle">{history.length} runs</span>
          </div>
          {runs.error && <ErrorState error={runs.error} />}
          <Card className="overflow-hidden">
            {runs.isLoading ? <div className="space-y-2 p-4">{Array.from({ length: 8 }, (_, i) => <Skeleton key={i} className="h-10" />)}</div>
              : history.length === 0 ? <div className="p-5"><EmptyState title="No runs match" icon={<History />}>Clear the filters or launch a new run.</EmptyState></div>
                : <HistoryTable rows={history} selected={selected} toggle={toggle} />}
          </Card>
        </TabsContent>

        <TabsContent value="queue" className="mt-4 space-y-4">
          <Card>
            <div className="border-b border-border px-5 py-3 text-[13px] font-semibold">Running and queued</div>
            {running.length + queue.length === 0 ? <div className="px-5 py-6 text-[13px] text-subtle">Nothing running or queued.</div> : (
              <ul className="divide-y divide-border">
                {running.map((j) => <JobRow key={j.id} j={j} onAbort={() => abort.mutate(j.id)} />)}
                {queue.map((j) => <JobRow key={j.id} j={j} onAbort={() => abort.mutate(j.id)} />)}
              </ul>
            )}
          </Card>
          <Card>
            <div className="border-b border-border px-5 py-3 text-[13px] font-semibold">Recent jobs</div>
            <ul className="divide-y divide-border">
              {jobs.filter((j) => j.status !== "queued" && j.status !== "running").slice(0, 30).map((j) => <JobRow key={j.id} j={j} />)}
            </ul>
          </Card>
        </TabsContent>

        <TabsContent value="schedules" className="mt-4">
          <Card className="overflow-hidden">
            {!schedules.data?.length ? (
              <div className="p-5"><EmptyState icon={<CalendarClock />} title="No schedules" action={<Button variant="secondary" onClick={() => nav("/launch")}>Schedule a run</Button>}>
                Recurring soak tests and nightly regressions live here. Create one from the Launch page.
              </EmptyState></div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full min-w-[720px] text-[13px]">
                  <thead className="border-b border-border bg-surface-2/40">
                    <tr><th className={cn(th, "pl-5")}>Scenario</th><th className={th}>Next run</th><th className={th}>Repeat</th><th className={th}>Remaining</th><th className={th}>Overrides</th><th className={th}>Last job</th><th /></tr>
                  </thead>
                  <tbody>
                    {schedules.data.map((s: Schedule) => (
                      <tr key={s.id} className="border-b border-border last:border-0">
                        <td className="px-3 py-2.5 pl-5 font-mono">{s.scenario}</td>
                        <td className="px-3 py-2.5 num">{fmtTime(s.next_at)}</td>
                        <td className="px-3 py-2.5">{s.every_minutes ? `every ${fmtDuration(s.every_minutes * 60)}` : "once"}</td>
                        <td className="px-3 py-2.5 num">{s.remaining ?? "∞"}</td>
                        <td className="max-w-[240px] truncate px-3 py-2.5 font-mono text-[11.5px] text-subtle">{typeof s.overrides === "string" ? s.overrides : JSON.stringify(s.overrides)}</td>
                        <td className="px-3 py-2.5 font-mono text-[11.5px] text-subtle">{s.last_job ?? "—"}</td>
                        <td className="px-3 py-2.5 pr-5 text-right"><Button size="icon-sm" variant="ghost" aria-label="Delete schedule" onClick={() => del.mutate(s.id)}><Trash2 /></Button></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Card>
        </TabsContent>
      </Tabs>
    </>
  );
}
