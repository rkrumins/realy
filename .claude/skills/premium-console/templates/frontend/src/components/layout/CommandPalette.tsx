import { useQuery } from "@tanstack/react-query";
import { Command } from "cmdk";
import { FileBarChart2, Moon, OctagonX, Radio, Search } from "lucide-react";
import { useNavigate } from "react-router";
import { toast } from "sonner";
import { api } from "@/lib/api";
import { useStatus } from "@/lib/hooks";
import { toggleTheme } from "@/lib/theme";
import { Dialog, DialogContent } from "../ui/dialog";
import { FLAT_NAV } from "./nav";

const itemCls =
  "flex cursor-pointer items-center gap-3 rounded-lg px-3 py-2 text-[13.5px] text-fg data-[selected=true]:bg-surface-2 [&_svg]:size-4 [&_svg]:shrink-0 [&_svg]:text-subtle";
const groupCls =
  "[&_[cmdk-group-heading]]:px-3 [&_[cmdk-group-heading]]:pb-1 [&_[cmdk-group-heading]]:pt-3 [&_[cmdk-group-heading]]:text-[10.5px] [&_[cmdk-group-heading]]:font-semibold [&_[cmdk-group-heading]]:uppercase [&_[cmdk-group-heading]]:tracking-[0.14em] [&_[cmdk-group-heading]]:text-subtle";

/** ⌘K / Ctrl K: jump to any page or recent job, act on the running job, toggle the theme. */
export function CommandPalette({ open, onOpenChange }: { open: boolean; onOpenChange: (v: boolean) => void }) {
  const nav = useNavigate();
  const status = useStatus();
  const jobs = useQuery({ queryKey: ["jobs"], queryFn: () => api.jobs(200), enabled: open, staleTime: 10_000 });
  const go = (to: string) => {
    onOpenChange(false);
    nav(to);
  };
  const cur = status?.current;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="top-[12vh] max-w-xl overflow-hidden p-0" aria-describedby={undefined}>
        <Command loop className="flex max-h-[min(560px,70vh)] flex-col">
          <div className="flex items-center gap-3 border-b border-border px-4">
            <Search className="size-4 text-subtle" />
            <Command.Input autoFocus placeholder="Jump to a page or a job…" aria-label="Command"
              className="h-12 flex-1 bg-transparent text-[14px] text-fg outline-none placeholder:text-subtle" />
          </div>
          <Command.List className="overflow-y-auto p-2">
            <Command.Empty className="px-3 py-8 text-center text-[13px] text-muted">No matches.</Command.Empty>
            {cur && (
              <Command.Group heading="Running now" className={groupCls}>
                <Command.Item className={itemCls} onSelect={() => go("/live")} value={`live watch ${cur.kind}`}>
                  <Radio />Watch {cur.kind} live
                </Command.Item>
                <Command.Item className={itemCls} value="abort current job stop cancel"
                  onSelect={async () => {
                    onOpenChange(false);
                    try {
                      await api.abortJob(cur.id);
                      toast.success("Abort requested");
                    } catch (e) {
                      toast.error(`Abort failed: ${(e as Error).message}`);
                    }
                  }}>
                  <OctagonX />Abort the running job
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
            {!!jobs.data?.jobs?.length && (
              <Command.Group heading="Jobs" className={groupCls}>
                {jobs.data.jobs.slice(0, 60).map((j) => (
                  <Command.Item key={j.id} className={itemCls} value={`job ${j.id} ${j.kind} ${j.status}`} onSelect={() => go(`/jobs/${encodeURIComponent(j.id)}`)}>
                    <FileBarChart2 />
                    <span className="truncate font-mono text-[12.5px]">{j.id}</span>
                    <span className="truncate text-[12.5px] text-muted">{j.kind}</span>
                    <span className={j.status === "done" ? "ml-auto text-[12px] text-pass" : j.status === "failed" ? "ml-auto text-[12px] text-fail" : "ml-auto text-[12px] text-subtle"}>
                      {j.status}
                    </span>
                  </Command.Item>
                ))}
              </Command.Group>
            )}
          </Command.List>
        </Command>
      </DialogContent>
    </Dialog>
  );
}
