import { Link } from "react-router";
import { cn } from "@/lib/cn";
import { fmtDuration, fmtMs, fmtNum } from "@/lib/format";
import { useElapsed, useLiveSeries, useStatus } from "@/lib/hooks";
import { Dot } from "../ui/badge";
import { Tooltip } from "../ui/tooltip";

function avg(xs: (number | null)[]) {
  const v = xs.filter((x): x is number => x !== null);
  return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null;
}

/** Top-bar pill: phase · throughput · p99 vs budget · elapsed for the running job; "Idle" otherwise. */
export function StatusPill({ compact }: { compact?: boolean }) {
  const status = useStatus();
  const cur = status?.current ?? null;
  const series = useLiveSeries(30, cur?.id ?? null);
  const elapsed = useElapsed(cur?.started_at);
  const queued = status?.queue?.length ?? 0;

  if (!cur) {
    return (
      <Link to="/jobs" className="inline-flex h-8 items-center gap-2 rounded-full border border-border bg-surface-2/60 px-3 text-[12.5px] text-muted transition hover:border-border-strong hover:text-fg">
        <Dot tone="neutral" />
        <span>Idle</span>
        {queued > 0 && <span className="text-subtle">· {queued} queued</span>}
      </Link>
    );
  }
  const tput = avg(series.slice(-5).map((p) => p.throughput));
  const p99 = [...series].reverse().find((p) => p.p99_ms !== null)?.p99_ms ?? null;
  const budget = cur.budget_ms;
  const over = p99 !== null && budget !== null && p99 > budget;
  const phase = (cur.phase || "running").toLowerCase();

  return (
    <Tooltip content={<span>{cur.kind} · {cur.id}{queued ? ` · ${queued} queued` : ""}</span>} side="bottom">
      <Link to="/live" className={cn(
        "group inline-flex h-8 items-center gap-2.5 rounded-full border px-3 text-[12.5px] transition",
        over ? "border-fail/30 bg-fail/10" : "border-accent/30 bg-accent/10 hover:bg-accent/15",
      )}>
        <Dot tone={over ? "fail" : "accent"} pulse />
        <span className="font-medium text-fg">{phase}</span>
        {!compact && <span className="max-w-[140px] truncate text-muted">{cur.kind}</span>}
        <span className="h-3.5 w-px bg-border-strong" />
        <span className="num text-fg">{tput === null ? "—" : fmtNum(tput)}<span className="text-subtle"> /s</span></span>
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
