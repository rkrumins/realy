import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { History, ListOrdered, Play, Plus, Search, X } from "lucide-react";
import * as React from "react";
import { useNavigate, useSearchParams } from "react-router";
import { toast } from "sonner";
import { EmptyState, ErrorState, PageHeader, StatusBadge } from "@/components/common";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/controls";
import { Dialog, DialogClose, DialogContent } from "@/components/ui/dialog";
import { Input, Label, NumberField } from "@/components/ui/input";
import { Select } from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { api } from "@/lib/api";
import { cn } from "@/lib/cn";
import { fmtAgo, fmtCompact, fmtDuration, fmtMs, fmtNum, fmtPct, fmtTime } from "@/lib/format";
import type { Job } from "@/lib/types";

const th = "px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wider text-subtle whitespace-nowrap";

function duration(j: Job) {
  return j.started_at && j.finished_at ? j.finished_at - j.started_at : null;
}

function HistoryTable({ rows }: { rows: Job[] }) {
  const nav = useNavigate();
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[860px] text-[13px]">
        <thead className="border-b border-border bg-surface-2/40">
          <tr>
            <th className={cn(th, "pl-5")}>Status</th><th className={th}>Job</th>
            <th className={cn(th, "text-right")}>Items</th><th className={cn(th, "text-right")}>Items/s</th>
            <th className={cn(th, "text-right")}>p50</th><th className={cn(th, "text-right")}>p99</th>
            <th className={cn(th, "text-right")}>In budget</th><th className={cn(th, "text-right")}>Took</th>
            <th className={cn(th, "pr-5 text-right")}>Started</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((j) => (
            <tr key={j.id} onClick={() => nav(`/jobs/${encodeURIComponent(j.id)}`)} tabIndex={0}
              onKeyDown={(e) => { if (e.key === "Enter") nav(`/jobs/${encodeURIComponent(j.id)}`); }}
              className="cursor-pointer border-b border-border last:border-0 transition-colors hover:bg-surface-2/60 focus-visible:bg-surface-2/60">
              <td className="px-3 py-2.5 pl-5"><StatusBadge status={j.status} /></td>
              <td className="max-w-[280px] px-3 py-2.5">
                <div className="truncate font-mono text-[12.5px] text-fg">{j.kind}</div>
                <div className="truncate font-mono text-[11px] text-subtle">{j.id}</div>
              </td>
              <td className="px-3 py-2.5 text-right num">{fmtCompact(j.result?.processed)}{j.result?.failed ? <span className="text-fail"> −{fmtCompact(j.result.failed)}</span> : null}</td>
              <td className="px-3 py-2.5 text-right num">{fmtNum(j.result?.throughput)}</td>
              <td className="px-3 py-2.5 text-right num text-muted">{fmtMs(j.result?.p50_ms)}</td>
              <td className="px-3 py-2.5 text-right num">{fmtMs(j.result?.p99_ms)}</td>
              <td className="px-3 py-2.5 text-right num">{fmtPct(j.result?.within_budget, 1)}</td>
              <td className="px-3 py-2.5 text-right num text-muted">{fmtDuration(duration(j))}</td>
              <td className="px-3 py-2.5 pr-5 text-right text-muted" title={fmtTime(j.started_at ?? j.created_at)}>{fmtAgo(j.started_at ?? j.created_at)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function QueueRow({ j, onAbort, phase }: { j: Job; onAbort?: () => void; phase?: string }) {
  return (
    <li className="flex flex-wrap items-center gap-x-3 gap-y-1 px-5 py-3 text-[13px]">
      <StatusBadge status={j.status} phase={phase} />
      <span className="font-mono text-fg">{j.kind}</span>
      <span className="font-mono text-[11.5px] text-subtle">{j.id}</span>
      {Object.keys(j.params || {}).length > 0 && <span className="truncate font-mono text-[11px] text-subtle">{JSON.stringify(j.params)}</span>}
      {j.error && <span className="text-[12px] text-fail">{j.error}</span>}
      <span className="ml-auto text-[12px] text-subtle">{j.started_at ? `started ${fmtAgo(j.started_at)}` : `queued ${fmtAgo(j.created_at)}`}</span>
      {onAbort && <Button size="sm" variant="ghost" onClick={onAbort}><X />{j.status === "running" ? "Abort" : "Cancel"}</Button>}
    </li>
  );
}

/** Data-driven "new job" form: the backend lists the job kinds and their numeric parameters. */
function NewJobDialog({ open, onOpenChange }: { open: boolean; onOpenChange: (v: boolean) => void }) {
  const qc = useQueryClient();
  const nav = useNavigate();
  const kinds = useQuery({ queryKey: ["kinds"], queryFn: api.kinds, enabled: open, staleTime: 60_000 });
  const [kind, setKind] = React.useState("");
  const [params, setParams] = React.useState<Record<string, number | undefined>>({});
  const def = kinds.data?.find((k) => k.kind === kind) ?? kinds.data?.[0];
  React.useEffect(() => {
    if (def) {
      setKind(def.kind);
      setParams(Object.fromEntries(def.params.map((p) => [p.name, p.default])));
    }
  }, [def?.kind]); // eslint-disable-line react-hooks/exhaustive-deps
  const create = useMutation({
    mutationFn: () => api.createJob(kind, Object.fromEntries(Object.entries(params).filter(([, v]) => v !== undefined))),
    onSuccess: (j) => {
      toast.success("Job queued", { description: `${j.kind} · ${j.id}`, action: { label: "Watch live", onClick: () => nav("/live") } });
      qc.invalidateQueries({ queryKey: ["jobs"] });
      onOpenChange(false);
    },
    onError: (e) => toast.error("Could not start the job", { description: (e as Error).message }),
  });
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent title="New job" description="Jobs run one at a time; the rest wait in the queue." aria-describedby={undefined}>
        <form className="space-y-4 px-5 py-4" onSubmit={(e) => { e.preventDefault(); create.mutate(); }}>
          {kinds.isLoading ? <Skeleton className="h-24" /> : kinds.error ? <ErrorState error={kinds.error} /> : (
            <>
              <Label>Kind
                <Select value={kind} onChange={setKind} ariaLabel="Job kind"
                  options={(kinds.data ?? []).map((k) => ({ value: k.kind, label: k.label, hint: k.description }))} />
              </Label>
              <div className="grid grid-cols-2 gap-3">
                {def?.params.map((p) => (
                  <NumberField key={p.name} label={p.label} unit={p.unit} hint={p.hint} min={p.min} max={p.max} step={p.step}
                    value={params[p.name]} onChange={(v) => setParams((s) => ({ ...s, [p.name]: v }))} />
                ))}
              </div>
            </>
          )}
          <div className="-mx-5 flex justify-end gap-2 border-t border-border px-5 pt-3">
            <DialogClose asChild><Button type="button" variant="ghost">Cancel</Button></DialogClose>
            <Button type="submit" variant="gradient" disabled={!kind || create.isPending} data-testid="start-job"><Play />Start</Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  );
}

export default function Jobs() {
  const [params, setParams] = useSearchParams();
  const qc = useQueryClient();
  const tab = params.get("tab") || "history";
  const jobs = useQuery({ queryKey: ["jobs"], queryFn: () => api.jobs(500), refetchInterval: 5000 });
  const [q, setQ] = React.useState("");
  const [status, setStatus] = React.useState("");
  const newOpen = params.get("new") === "1";
  const setNewOpen = (v: boolean) => setParams((p) => { if (v) p.set("new", "1"); else p.delete("new"); return p; }, { replace: true });

  const abort = useMutation({
    mutationFn: (id: string) => api.abortJob(id),
    onSuccess: (r) => { toast.success(`Abort: ${r.result}`); qc.invalidateQueries({ queryKey: ["jobs"] }); },
    onError: (e) => toast.error("Abort failed", { description: (e as Error).message }),
  });

  const all = jobs.data?.jobs ?? [];
  const history = all.filter((j) => j.status !== "queued" && j.status !== "running")
    .filter((j) => (!q || `${j.id} ${j.kind}`.toLowerCase().includes(q.toLowerCase())) && (!status || j.status === status));
  const queue = jobs.data?.queue ?? [];
  const running = all.filter((j) => j.status === "running");
  const cur = jobs.data?.current;

  return (
    <>
      <PageHeader eyebrow="Jobs" title="Jobs and queue"
        subtitle="Every job keeps its parameters, step timings, result and log. Open one for its report."
        actions={<Button variant="gradient" onClick={() => setNewOpen(true)}><Plus />New job</Button>} />
      <Tabs value={tab} onValueChange={(t) => setParams((p) => { p.set("tab", t); return p; }, { replace: true })}>
        <TabsList>
          <TabsTrigger value="history"><History />History <span className="text-subtle">{history.length || ""}</span></TabsTrigger>
          <TabsTrigger value="queue"><ListOrdered />Queue <span className="text-subtle">{queue.length + running.length || ""}</span></TabsTrigger>
        </TabsList>

        <TabsContent value="history">
          <div className="mb-3 flex flex-wrap items-center gap-2">
            <div className="relative w-full max-w-xs">
              <Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-subtle" />
              <Input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Search id or kind…" aria-label="Search jobs" className="pl-9" />
            </div>
            <Select className="w-40" value={status} onChange={setStatus} ariaLabel="Status filter" options={[
              { value: "", label: "any status" }, { value: "done", label: "done" }, { value: "failed", label: "failed" },
              { value: "aborted", label: "aborted" }, { value: "cancelled", label: "cancelled" }, { value: "interrupted", label: "interrupted" },
            ]} />
            {(q || status) && <Button variant="ghost" size="sm" onClick={() => { setQ(""); setStatus(""); }}><X />Clear filters</Button>}
            <span className="ml-auto text-[12.5px] text-subtle num">{history.length} jobs</span>
          </div>
          {jobs.error && <div className="mb-3"><ErrorState error={jobs.error} /></div>}
          <Card className="overflow-hidden">
            {jobs.isLoading ? <div className="space-y-2 p-4">{Array.from({ length: 8 }, (_, i) => <Skeleton key={i} className="h-10" />)}</div>
              : history.length === 0 ? (
                <div className="p-5">
                  {all.length === 0
                    ? <EmptyState title="No jobs yet" icon={<History />} action={<Button variant="gradient" onClick={() => setNewOpen(true)}><Plus />New job</Button>}>Start one; it shows up here with its result when it finishes.</EmptyState>
                    : <EmptyState title="No jobs match" icon={<Search />}>Clear the filters to see all {all.length} jobs.</EmptyState>}
                </div>
              ) : <HistoryTable rows={history} />}
          </Card>
        </TabsContent>

        <TabsContent value="queue">
          <Card>
            <div className="flex flex-wrap items-center gap-2 border-b border-border px-5 py-2.5">
              <span className="text-[13px] font-semibold">Running and queued</span>
              <span className="text-[12px] text-subtle">one job at a time; the rest wait their turn</span>
            </div>
            {running.length + queue.length === 0 ? <div className="px-5 py-6 text-[13px] text-subtle">Nothing running or queued.</div> : (
              <ul className="divide-y divide-border">
                {running.map((j) => <QueueRow key={j.id} j={j} phase={cur?.id === j.id ? cur.phase : undefined} onAbort={() => abort.mutate(j.id)} />)}
                {queue.map((j) => <QueueRow key={j.id} j={j} onAbort={() => abort.mutate(j.id)} />)}
              </ul>
            )}
          </Card>
        </TabsContent>
      </Tabs>
      <NewJobDialog open={newOpen} onOpenChange={setNewOpen} />
    </>
  );
}
