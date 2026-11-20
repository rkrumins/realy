import { useQuery } from "@tanstack/react-query";
import { Command } from "cmdk";
import { BookOpen, FileBarChart2, Moon, OctagonX, Rocket, Search } from "lucide-react";
import { useNavigate } from "react-router";
import { toast } from "sonner";
import { api } from "@/lib/api";
import { activeRun, useStatus } from "@/lib/hooks";
import { toggleTheme } from "@/lib/theme";
import { Dialog, DialogContent } from "../ui/dialog";
import { FLAT_NAV } from "./nav";

const itemCls =
  "flex cursor-pointer items-center gap-3 rounded-lg px-3 py-2 text-[13.5px] text-fg data-[selected=true]:bg-surface-2 [&_svg]:size-4 [&_svg]:shrink-0 [&_svg]:text-subtle";
const groupCls =
  "[&_[cmdk-group-heading]]:px-3 [&_[cmdk-group-heading]]:pb-1 [&_[cmdk-group-heading]]:pt-3 [&_[cmdk-group-heading]]:text-[10.5px] [&_[cmdk-group-heading]]:font-semibold [&_[cmdk-group-heading]]:uppercase [&_[cmdk-group-heading]]:tracking-[0.14em] [&_[cmdk-group-heading]]:text-subtle";

export function CommandPalette({ open, onOpenChange }: { open: boolean; onOpenChange: (v: boolean) => void }) {
  const nav = useNavigate();
  const status = useStatus();
  const scenarios = useQuery({ queryKey: ["scenarios"], queryFn: api.scenarios, enabled: open, staleTime: 30_000 });
  const runs = useQuery({ queryKey: ["runs"], queryFn: () => api.runs(200), enabled: open, staleTime: 10_000 });
  const docs = useQuery({ queryKey: ["docs"], queryFn: api.docs, enabled: open, retry: 0, staleTime: 300_000 });
  const go = (to: string) => {
    onOpenChange(false);
    nav(to);
  };
  const cur = activeRun(status);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="top-[12vh] max-w-xl overflow-hidden p-0" aria-describedby={undefined}>
        <Command loop className="flex max-h-[min(560px,70vh)] flex-col">
          <div className="flex items-center gap-3 border-b border-border px-4">
            <Search className="size-4 text-subtle" />
            <Command.Input autoFocus placeholder="Launch a scenario, open a run, read a doc…"
              className="h-12 flex-1 bg-transparent text-[14px] text-fg outline-none placeholder:text-subtle" />
          </div>
          <Command.List className="overflow-y-auto p-2">
            <Command.Empty className="px-3 py-8 text-center text-[13px] text-muted">No matches.</Command.Empty>
            {cur && (
              <Command.Group heading="Current run" className={groupCls}>
                <Command.Item className={itemCls} onSelect={() => go("/live")} value={`live ${cur.scenario}`}>
                  <Rocket />Watch {cur.scenario} live
                </Command.Item>
                <Command.Item className={itemCls} value="abort current run stop"
                  onSelect={async () => {
                    onOpenChange(false);
                    try {
                      await api.abort(cur.job_id ?? cur.run_id ?? "");
                      toast.success("Abort requested");
                    } catch (e) {
                      toast.error(`Abort failed: ${(e as Error).message}`);
                    }
                  }}>
                  <OctagonX />Abort current run
                </Command.Item>
                <Command.Item className={itemCls} value="stop all abort everything cancel queue pause schedules"
                  onSelect={async () => {
                    onOpenChange(false);
                    try {
                      const r = await api.stopAll(true);
                      toast.success(`Stop all: ${Object.keys(r.results ?? {}).length} run(s)/job(s), ${r.schedules_paused?.length ?? 0} schedule(s) paused`);
                    } catch (e) {
                      toast.error(`Stop all failed: ${(e as Error).message}`);
                    }
                  }}>
                  <OctagonX />Stop all (abort run, cancel queue, pause schedules)
                </Command.Item>
              </Command.Group>
            )}
            <Command.Group heading="Go to" className={groupCls}>
              {FLAT_NAV.map((n) => (
                <Command.Item key={n.to} className={itemCls} value={`go ${n.label} ${n.hint}`} onSelect={() => go(n.to)}>
                  <n.icon />
                  <span>{n.label}</span>
                  <span className="ml-auto truncate text-[12px] text-subtle">{n.hint}</span>
                </Command.Item>
              ))}
              <Command.Item className={itemCls} value="toggle theme dark light" onSelect={() => { toggleTheme(); onOpenChange(false); }}>
                <Moon />Toggle theme
              </Command.Item>
            </Command.Group>
            {!!scenarios.data?.length && (
              <Command.Group heading="Launch scenario" className={groupCls}>
                {scenarios.data.map((s) => (
                  <Command.Item key={s.name} className={itemCls} value={`launch ${s.name} ${s.topology ?? ""}`}
                    onSelect={() => go(`/launch?scenario=${encodeURIComponent(s.name)}`)}>
                    <Rocket />
                    <span className="font-mono text-[12.5px]">{s.name}</span>
                    <span className="ml-auto truncate text-[12px] text-subtle">{s.topology} · {s.tps ?? "?"} TPS</span>
                  </Command.Item>
                ))}
              </Command.Group>
            )}
            {!!runs.data?.history?.length && (
              <Command.Group heading="Runs" className={groupCls}>
                {runs.data.history.slice(0, 60).map((r) => (
                  <Command.Item key={r.run_id} className={itemCls} value={`run ${r.run_id} ${r.verdict ?? ""}`} onSelect={() => go(`/runs/${encodeURIComponent(r.run_id)}`)}>
                    <FileBarChart2 />
                    <span className="truncate font-mono text-[12.5px]">{r.run_id}</span>
                    <span className={r.verdict === "PASS" ? "ml-auto text-[12px] text-pass" : r.verdict ? "ml-auto text-[12px] text-fail" : "ml-auto text-[12px] text-subtle"}>
                      {r.verdict ?? r.status}
                    </span>
                  </Command.Item>
                ))}
              </Command.Group>
            )}
            {!!docs.data?.sections?.length && (
              <Command.Group heading="Docs" className={groupCls}>
                {docs.data.sections.flatMap((s) => s.items.map((it) => (
                  <Command.Item key={it.path} className={itemCls} value={`doc ${it.title} ${it.path}`} onSelect={() => go(`/docs?path=${encodeURIComponent(it.path)}`)}>
                    <BookOpen />{it.title}
                    <span className="ml-auto truncate text-[12px] text-subtle">{s.title}</span>
                  </Command.Item>
                )))}
              </Command.Group>
            )}
          </Command.List>
        </Command>
      </DialogContent>
    </Dialog>
  );
}
