import { ArrowDownToLine, Pause, Play, Search, TriangleAlert } from "lucide-react";
import * as React from "react";
import { Link } from "react-router";
import { cn } from "@/lib/cn";
import { fmtMs, fmtUsd } from "@/lib/format";
import type { LiveDecision } from "@/lib/types";
import { Badge } from "./ui/badge";
import { Input } from "./ui/input";

export const OUTCOME_TONE: Record<string, "pass" | "warn" | "fail" | "neutral"> = { APPROVE: "pass", REVIEW: "warn", DECLINE: "fail" };

export function OutcomeBadge({ outcome }: { outcome: string | null | undefined }) {
  if (!outcome) return <Badge tone="neutral">no decision</Badge>;
  return <Badge tone={OUTCOME_TONE[outcome] ?? "neutral"} className="font-semibold tracking-wide">{outcome.toLowerCase()}</Badge>;
}

export function ScoreBar({ score, className }: { score: number | null | undefined; className?: string }) {
  const s = typeof score === "number" ? Math.max(0, Math.min(1, score)) : null;
  const tone = s === null ? "bg-subtle" : s >= 0.8 ? "bg-fail" : s >= 0.5 ? "bg-warn" : "bg-pass";
  return (
    <div className={cn("flex items-center gap-2", className)}>
      <div className="h-1.5 w-14 overflow-hidden rounded-full bg-surface-3">
        <div className={cn("h-full rounded-full", tone)} style={{ width: `${(s ?? 0) * 100}%` }} />
      </div>
      <span className="w-9 text-right font-mono text-[11.5px] text-muted">{s === null ? "—" : s.toFixed(2)}</span>
    </div>
  );
}

/** Rate-limited sample (~20/s) of live decisions. Click one to open it in the Explorer. */
export function DecisionFeed({ decisions, runId, className, max = 60 }: { decisions: LiveDecision[]; runId: string | null; className?: string; max?: number }) {
  const [paused, setPaused] = React.useState(false);
  const [frozen, setFrozen] = React.useState<LiveDecision[]>([]);
  const [filter, setFilter] = React.useState<"all" | "flagged" | "degraded">("all");
  React.useEffect(() => {
    if (!paused) setFrozen(decisions);
  }, [decisions, paused]);
  const rows = (paused ? frozen : decisions)
    .filter((d) => !runId || d.run_id === runId)
    .filter((d) => filter === "all" || (filter === "flagged" ? d.outcome !== "APPROVE" : d.degraded))
    .slice(0, max);

  return (
    <div className={cn("flex min-h-0 flex-col", className)}>
      <div className="flex items-center gap-1.5 pb-2">
        {(["all", "flagged", "degraded"] as const).map((f) => (
          <button key={f} onClick={() => setFilter(f)}
            className={cn("rounded-full px-2.5 py-1 text-[11.5px] font-medium transition cursor-pointer",
              filter === f ? "bg-accent/15 text-accent" : "text-muted hover:bg-surface-2")}>
            {f}
          </button>
        ))}
        <button onClick={() => setPaused(!paused)} aria-label={paused ? "Resume feed" : "Pause feed"}
          className="ml-auto inline-flex items-center gap-1 rounded-full px-2.5 py-1 text-[11.5px] text-muted hover:bg-surface-2 cursor-pointer">
          {paused ? <Play className="size-3" /> : <Pause className="size-3" />}{paused ? "resume" : "pause"}
        </button>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto rounded-xl border border-border" data-testid="decision-feed">
        {rows.length === 0 ? (
          <div className="flex h-full min-h-40 items-center justify-center px-4 text-center text-[12.5px] text-subtle">
            Decisions appear here as the pipeline produces them.
          </div>
        ) : (
          <ul className="divide-y divide-border">
            {rows.map((d) => (
              <li key={`${d.run_id}-${d.event_id}`}>
                <Link to={`/explorer?run=${encodeURIComponent(d.run_id)}&event=${d.event_id}`}
                  className="grid grid-cols-[72px_1fr_auto] items-center gap-x-3 gap-y-0.5 px-3 py-2 text-[12.5px] transition hover:bg-surface-2">
                  <OutcomeBadge outcome={d.outcome} />
                  <div className="min-w-0">
                    <div className="flex items-center gap-1.5 truncate">
                      <span className="font-medium text-fg num">{fmtUsd(d.amount_usd)}</span>
                      <span className="truncate text-subtle">{(d.payment_type || "").toLowerCase().replace(/_/g, " ")} · {(d.channel || "").toLowerCase()}{d.merchant_country ? ` · ${d.merchant_country}` : ""}</span>
                      {d.degraded && <TriangleAlert className="size-3.5 shrink-0 text-warn" aria-label="degraded" />}
                    </div>
                    {d.rule_hits.length > 0 && (
                      <div className="mt-0.5 flex gap-1 overflow-hidden">
                        {d.rule_hits.slice(0, 3).map((r) => (
                          <span key={r} className="truncate rounded bg-surface-3 px-1.5 font-mono text-[10.5px] text-muted">{r}</span>
                        ))}
                        {d.rule_hits.length > 3 && <span className="text-[10.5px] text-subtle">+{d.rule_hits.length - 3}</span>}
                      </div>
                    )}
                  </div>
                  <div className="flex flex-col items-end gap-0.5">
                    <ScoreBar score={d.score} />
                    <span className="font-mono text-[10.5px] text-subtle">{d.hot_path_ms !== null ? fmtMs(d.hot_path_ms) : "—"}</span>
                  </div>
                </Link>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

function lineTone(l: string) {
  if (/error|traceback|exception|failed|abort/i.test(l)) return "text-fail";
  if (/warn|degraded|guardrail/i.test(l)) return "text-warn";
  if (l.includes("[console]")) return "text-accent";
  if (/verdict|PASS|complete/i.test(l)) return "text-pass";
  return "text-muted";
}

/** Streaming log with follow mode (stops following when you scroll up) and a filter. */
export function LogConsole({ lines, className, height = 320 }: { lines: { line: string; ts?: number }[]; className?: string; height?: number }) {
  const ref = React.useRef<HTMLDivElement>(null);
  const [follow, setFollow] = React.useState(true);
  const [q, setQ] = React.useState("");
  const shown = q ? lines.filter((l) => l.line.toLowerCase().includes(q.toLowerCase())) : lines;
  React.useEffect(() => {
    if (follow && ref.current) ref.current.scrollTop = ref.current.scrollHeight;
  }, [shown.length, follow]);
  return (
    <div className={cn("flex flex-col", className)}>
      <div className="mb-2 flex items-center gap-2">
        <div className="relative flex-1">
          <Search className="pointer-events-none absolute left-2.5 top-1/2 size-3.5 -translate-y-1/2 text-subtle" />
          <Input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Filter log…" className="h-8 pl-8 text-[12.5px]" />
        </div>
        <button onClick={() => setFollow(true)}
          className={cn("inline-flex h-8 items-center gap-1.5 rounded-lg px-2.5 text-[12px] cursor-pointer", follow ? "text-accent" : "text-muted hover:bg-surface-2")}>
          <ArrowDownToLine className="size-3.5" />follow
        </button>
      </div>
      <div ref={ref} style={{ height }} data-testid="log-console"
        onScroll={(e) => {
          const el = e.currentTarget;
          const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 24;
          if (!atBottom && follow) setFollow(false);
          if (atBottom && !follow) setFollow(true);
        }}
        className="overflow-auto rounded-xl border border-border bg-[color-mix(in_oklab,var(--card-2)_70%,black_8%)] p-3 font-mono text-[11.5px] leading-[1.6]">
        {shown.length === 0 ? (
          <div className="text-subtle">No log lines yet.</div>
        ) : (
          shown.map((l, i) => (
            <div key={i} className={cn("whitespace-pre-wrap break-all", lineTone(l.line))}>
              {l.ts ? <span className="mr-2 select-none text-subtle">{new Date(l.ts * 1000).toLocaleTimeString("en-GB")}</span> : null}
              {l.line}
            </div>
          ))
        )}
      </div>
    </div>
  );
}
