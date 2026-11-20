import { Link } from "react-router";
import { cn } from "@/lib/cn";
import { fmtDuration, fmtMs, fmtNum } from "@/lib/format";
import { activeRun, useElapsed, useLiveSeries, useStatus } from "@/lib/hooks";
import { Dot } from "../ui/badge";
import { Tooltip } from "../ui/tooltip";

function avg(xs: (number | null)[]) {
  const v = xs.filter((x): x is number => x !== null);
  return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null;
}

/** Top-bar pill: phase · TPS · e2e p99 vs budget for the running job; "idle" otherwise. */
export function RunStatusPill({ compact }: { compact?: boolean }) {
  const status = useStatus();
  const cur = activeRun(status);
  const series = useLiveSeries(30, cur?.run_id ?? null);
  const elapsed = useElapsed(cur?.started_at);
  const queued = status?.queue?.length ?? 0;

  if (!cur) {
    return (
      <Link to="/launch" className="inline-flex h-8 items-center gap-2 rounded-full border border-border bg-surface-2/60 px-3 text-[12.5px] text-muted transition hover:border-border-strong hover:text-fg">
        <Dot tone="neutral" />
        <span>Idle</span>
        {queued > 0 && <span className="text-subtle">· {queued} queued</span>}
      </Link>
    );
  }
  const last = series.slice(-5);
  const tps = avg(last.map((p) => p.emitted));
  const p99 = [...series].reverse().find((p) => p.e2e_p99 !== null)?.e2e_p99 ?? null;
  const budget = cur.budget?.e2e_p99_ms ?? null;
  const over = p99 !== null && budget !== null && p99 > budget;
  const phase = (cur.phase || "running").toLowerCase();

  return (
    <Tooltip content={<span>{cur.scenario} · {cur.run_id ?? "preparing"}{queued ? ` · ${queued} queued` : ""}</span>} side="bottom">
      <Link to="/live" className={cn(
        "group inline-flex h-8 items-center gap-2.5 rounded-full border px-3 text-[12.5px] transition",
        over ? "border-fail/30 bg-fail/10" : "border-accent/30 bg-accent/10 hover:bg-accent/15",
      )}>
        <Dot tone={over ? "fail" : "accent"} pulse />
        <span className="font-medium text-fg">{phase}</span>
        {!compact && <span className="max-w-[140px] truncate text-muted">{cur.scenario}</span>}
        <span className="h-3.5 w-px bg-border-strong" />
        <span className="num text-fg">{tps === null ? "—" : fmtNum(tps)}<span className="text-subtle"> TPS</span></span>
        {!compact && (
          <>
            <span className="h-3.5 w-px bg-border-strong" />
            <span className={cn("num", over ? "text-fail" : "text-fg")}>
              p99 {fmtMs(p99)}{budget !== null && <span className="text-subtle"> / {fmtNum(budget)}</span>}
            </span>
            {elapsed !== null && <span className="num text-subtle">{fmtDuration(elapsed)}</span>}
          </>
        )}
      </Link>
    </Tooltip>
  );
}
