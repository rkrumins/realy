/** Streaming lists: a log console with follow mode, and a generic live event feed with filters and pause. */
import { ArrowDownToLine, Pause, Play, Search } from "lucide-react";
import * as React from "react";
import { cn } from "@/lib/cn";
import { Input } from "./ui/input";

function lineTone(l: string) {
  if (/error|traceback|exception|failed|abort/i.test(l)) return "text-fail";
  if (/warn|degraded|retry/i.test(l)) return "text-warn";
  if (/^\[console\]/.test(l)) return "text-accent";
  if (/complete|finished|success|\bdone\b/i.test(l)) return "text-pass";
  return "text-muted";
}

/** Streaming log with follow mode (stops following when you scroll up, resumes at the bottom) and a filter. */
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
          <Input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Filter log…" aria-label="Filter log" className="h-8 pl-8 text-[12.5px]" />
        </div>
        <button type="button" onClick={() => setFollow(true)} aria-pressed={follow}
          className={cn("inline-flex h-8 items-center gap-1.5 rounded-lg px-2.5 text-[12px] cursor-pointer", follow ? "text-accent" : "text-muted hover:bg-surface-2")}>
          <ArrowDownToLine className="size-3.5" />follow
        </button>
      </div>
      <div ref={ref} style={{ height }} data-testid="log-console" role="log" aria-live="off"
        onScroll={(e) => {
          const el = e.currentTarget;
          const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 24;
          if (!atBottom && follow) setFollow(false);
          if (atBottom && !follow) setFollow(true);
        }}
        className="overflow-auto rounded-xl border border-border bg-[color-mix(in_oklab,var(--card-2)_70%,black_8%)] p-3 font-mono text-[11.5px] leading-[1.6]">
        {shown.length === 0 ? (
          <div className="text-subtle">{q ? "No line matches the filter." : "No log lines yet."}</div>
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

/**
 * Live event feed: newest first, filter chips, pause (freezes the list so you can read or click a row).
 * Generic over the item type; `render` draws one row.
 */
export function EventFeed<T>({ items, filters, render, itemKey, empty = "Events appear here as they happen.", className, max = 60 }: {
  items: T[]; filters: { value: string; label: string; test: (t: T) => boolean }[]; render: (t: T) => React.ReactNode;
  itemKey: (t: T) => string; empty?: React.ReactNode; className?: string; max?: number;
}) {
  const [paused, setPaused] = React.useState(false);
  const [frozen, setFrozen] = React.useState<T[]>([]);
  const [filter, setFilter] = React.useState(filters[0]?.value ?? "");
  React.useEffect(() => {
    if (!paused) setFrozen(items);
  }, [items, paused]);
  const f = filters.find((x) => x.value === filter);
  const rows = (paused ? frozen : items).filter((t) => !f || f.test(t)).slice(0, max);
  return (
    <div className={cn("flex min-h-0 flex-col", className)}>
      <div className="flex items-center gap-1.5 pb-2" role="toolbar" aria-label="Feed filters">
        {filters.map((x) => (
          <button key={x.value} type="button" onClick={() => setFilter(x.value)} aria-pressed={filter === x.value}
            className={cn("rounded-full px-2.5 py-1 text-[11.5px] font-medium transition cursor-pointer",
              filter === x.value ? "bg-accent/15 text-accent" : "text-muted hover:bg-surface-2")}>
            {x.label}
          </button>
        ))}
        <button type="button" onClick={() => setPaused(!paused)} aria-label={paused ? "Resume feed" : "Pause feed"}
          className="ml-auto inline-flex items-center gap-1 rounded-full px-2.5 py-1 text-[11.5px] text-muted hover:bg-surface-2 cursor-pointer">
          {paused ? <Play className="size-3" /> : <Pause className="size-3" />}{paused ? "resume" : "pause"}
        </button>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto rounded-xl border border-border">
        {rows.length === 0 ? (
          <div className="flex h-full min-h-40 items-center justify-center px-4 text-center text-[12.5px] text-subtle">{empty}</div>
        ) : (
          <ul className="divide-y divide-border">{rows.map((t) => <li key={itemKey(t)}>{render(t)}</li>)}</ul>
        )}
      </div>
    </div>
  );
}
