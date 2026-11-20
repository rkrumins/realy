import { Database, HardDrive, RefreshCw, Server, Workflow } from "lucide-react";
import { ErrorState, LegendDots, PageHeader, Stat, Unavailable } from "@/components/common";
import { Badge, Dot } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/controls";
import { cn } from "@/lib/cn";
import { fmtBytes, fmtMs, fmtNum, isNum } from "@/lib/format";
import { useEnv } from "@/lib/hooks";
import type { EnvComponent } from "@/lib/types";

const ICONS: Record<string, typeof Server> = { store: Database, worker: Workflow, disk: HardDrive };

function fmtDetail(v: unknown): string {
  if (v === null || v === undefined) return "—";
  if (typeof v === "boolean") return v ? "yes" : "no";
  if (typeof v === "number") return fmtNum(v, v % 1 ? 2 : 0);
  if (Array.isArray(v)) return v.length ? v.join(", ") : "none";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

/** Unit-aware detail rows: `*_bytes`, `*_pct`, `*_ms` keys are formatted and the suffix leaves the label. */
function detailRow(k: string, v: unknown): [string, string] {
  if (typeof v === "number") {
    if (k.endsWith("_bytes")) return [k.slice(0, -6), fmtBytes(v)];
    if (k.endsWith("_pct")) return [k.slice(0, -4), `${fmtNum(v, 1)}%`];
    if (k.endsWith("_ms")) return [k.slice(0, -3), fmtMs(v)];
  }
  return [k, fmtDetail(v)];
}

function ComponentCard({ c }: { c: EnvComponent }) {
  const Icon = ICONS[c.id] ?? Server;
  const tone = c.status === "up" ? "pass" : c.status === "degraded" ? "warn" : "fail";
  return (
    <Card className="flex flex-col p-4" data-testid={`env-${c.id}`}>
      <div className="flex items-start gap-3">
        <div className={cn("flex size-9 items-center justify-center rounded-xl", c.status === "up" ? "bg-pass/12 text-pass" : c.status === "degraded" ? "bg-warn/12 text-warn" : "bg-fail/12 text-fail")}>
          <Icon className="size-[18px]" />
        </div>
        <div className="min-w-0 flex-1">
          <div className="text-[14px] font-semibold">{c.name}</div>
          <div className="truncate text-[12px] text-subtle">{c.kind}{c.version ? ` · ${c.version}` : ""}</div>
        </div>
        <Badge tone={tone}><Dot tone={tone} pulse={c.status !== "up"} />{c.status}</Badge>
      </div>
      <div className="mt-3 flex-1">
        {Object.entries(c.details ?? {}).slice(0, 5).map(([k, v]) => {
          const [label, text] = detailRow(k, v);
          return <Stat key={k} label={label.replace(/_/g, " ")} value={<span className={cn((k === "error" || k === "reason") && c.status !== "up" && "text-fail", "break-all")}>{text}</span>} />;
        })}
      </div>
      <div className="mt-2 flex items-center justify-between border-t border-border pt-2 text-[11.5px] text-subtle">
        <span className="truncate font-mono">{c.url ?? ""}</span>
        <span className="num">{isNum(c.latency_ms) ? `probe ${fmtMs(c.latency_ms)}` : ""}</span>
      </div>
    </Card>
  );
}

export default function Environment() {
  const env = useEnv();
  return (
    <>
      <PageHeader eyebrow="Environment" title="Environment and dependencies"
        subtitle="Every dependency this service needs, probed in parallel with a short timeout: a dead one shows as down instead of hanging the page."
        actions={<Button variant="secondary" onClick={() => env.refetch()} disabled={env.isFetching}><RefreshCw className={cn(env.isFetching && "animate-spin")} />Refresh</Button>} />
      {env.isLoading ? (
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3">{Array.from({ length: 3 }, (_, i) => <Skeleton key={i} className="h-48 rounded-2xl" />)}</div>
      ) : env.error ? <ErrorState error={env.error} /> : !env.data ? <Unavailable what="Environment" /> : (
        <>
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3">
            {env.data.components.map((c) => <ComponentCard key={c.id} c={c} />)}
          </div>
          <div className="mt-4 flex flex-wrap items-center justify-between gap-2">
            <span className="text-[12px] text-subtle">checked {new Date(env.data.checked_at).toLocaleTimeString("en-GB")} · refreshes every 15 s</span>
            <LegendDots items={[{ label: "up", className: "bg-pass" }, { label: "degraded (reachable, impaired)", className: "bg-warn" }, { label: "down (unreachable or failing)", className: "bg-fail" }]} />
          </div>
        </>
      )}
    </>
  );
}
