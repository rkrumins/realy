/** Run lifecycle controls shared by Runs, Live and the command palette: Stop all (with confirmation)
 *  and the pipeline banner that shows who holds the pipeline lease when it is not this console. */
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Lock, OctagonX } from "lucide-react";
import * as React from "react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/controls";
import { Dialog, DialogClose, DialogContent } from "@/components/ui/dialog";
import { api } from "@/lib/api";
import { fmtAgo } from "@/lib/format";
import type { LeaseHolder, Status } from "@/lib/types";

export function StopAllButton({ size = "default", label = "Stop all" }: { size?: "default" | "sm"; label?: string }) {
  const qc = useQueryClient();
  const [open, setOpen] = React.useState(false);
  const [pause, setPause] = React.useState(true);
  const stop = useMutation({
    mutationFn: () => api.stopAll(pause),
    onSuccess: (r) => {
      const n = Object.keys(r.results ?? {}).length;
      toast.success("Stop all sent", {
        description: `${n} run(s)/job(s) aborted or cancelled${r.schedules_paused?.length ? `, ${r.schedules_paused.length} schedule(s) paused` : ""}. Runs drain and write partial reports.`,
      });
      setOpen(false);
      qc.invalidateQueries({ queryKey: ["runs"] });
      qc.invalidateQueries({ queryKey: ["schedules"] });
    },
    onError: (e) => toast.error("Stop all failed", { description: (e as Error).message }),
  });
  return (
    <>
      <Button variant="danger" size={size} onClick={() => setOpen(true)} data-testid="stop-all"><OctagonX />{label}</Button>
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent title="Stop everything?" aria-describedby={undefined}
          description="Aborts the running run (in this console or any other process holding the pipeline), cancels every queued job and broadcasts an abort on bench.control.">
          <div className="space-y-4 px-5 py-4 text-[13px]">
            <label className="flex items-center gap-2.5">
              <Checkbox checked={pause} onCheckedChange={(v) => setPause(Boolean(v))} />
              Also pause all schedules, so nothing new starts
            </label>
            <p className="text-[12px] text-muted">Aborted runs still drain in-flight decisions and write a partial report with verdict ABORTED.</p>
          </div>
          <div className="flex justify-end gap-2 border-t border-border px-5 py-3">
            <DialogClose asChild><Button variant="ghost">Cancel</Button></DialogClose>
            <Button variant="danger" onClick={() => stop.mutate()} disabled={stop.isPending} data-testid="confirm-stop-all">
              <OctagonX />Stop all
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </>
  );
}

/** Shown when the pipeline is held by a run this console does not own (CLI, bench Job, another console). */
export function PipelineBanner({ status }: { status?: Pick<Status, "current" | "lease" | "blocked_by"> | null }) {
  const qc = useQueryClient();
  const lease: LeaseHolder | null | undefined = status?.blocked_by?.run_id ? status.blocked_by : status?.lease;
  const mine = status?.current?.run_id && lease?.run_id === status.current.run_id;
  const abort = useMutation({
    mutationFn: (id: string) => api.abort(id),
    onSuccess: (r) => { toast.success(`Abort: ${r.result}`); qc.invalidateQueries({ queryKey: ["runs"] }); },
    onError: (e) => toast.error("Abort failed", { description: (e as Error).message }),
  });
  if (!lease?.run_id || mine) return null;
  return (
    <div className="mb-4 flex flex-wrap items-center gap-x-3 gap-y-2 rounded-xl border border-info/30 bg-info/8 px-4 py-2.5 text-[13px]" data-testid="pipeline-banner">
      <Lock className="size-4 text-info" />
      <span>
        Pipeline busy: <span className="font-mono">{lease.run_id}</span>
        {lease.scenario && <> ({lease.scenario}{lease.phase ? `, ${lease.phase.toLowerCase()}` : ""})</>}
        {lease.owner && <> owned by <span className="font-mono">{lease.owner}</span></>}
        {lease.started_at ? <>, started {fmtAgo(lease.started_at)}</> : null}.
        {status?.blocked_by?.run_id && <span className="text-muted"> Queued jobs wait until it finishes.</span>}
      </span>
      <Button size="sm" variant="outline" className="ml-auto" onClick={() => abort.mutate(lease.run_id)} disabled={abort.isPending}>
        <OctagonX />Abort it
      </Button>
    </div>
  );
}
