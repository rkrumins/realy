/**
 * Ranked insight cards: what the data tells you, most severe first, each with evidence, a next step and
 * a link to the section that proves it. The backend ranks; the UI only renders (and counts by severity).
 */
import { AlertOctagon, AlertTriangle, CheckCircle2, Info } from "lucide-react";
import * as React from "react";
import { Badge } from "@/components/ui/badge";
import { Card, CardBody, CardHeader } from "@/components/ui/card";
import { cn } from "@/lib/cn";
import type { Insight } from "@/lib/types";

const SEV: Record<Insight["severity"], { icon: typeof Info; cls: string; ring: string; tone: "fail" | "warn" | "neutral" | "pass" }> = {
  critical: { icon: AlertOctagon, cls: "text-fail", ring: "border-fail/35 bg-fail/[0.06]", tone: "fail" },
  warning: { icon: AlertTriangle, cls: "text-warn", ring: "border-warn/30 bg-warn/[0.05]", tone: "warn" },
  info: { icon: Info, cls: "text-info", ring: "border-border bg-surface-2/40", tone: "neutral" },
  good: { icon: CheckCircle2, cls: "text-pass", ring: "border-pass/25 bg-pass/[0.05]", tone: "pass" },
};
const ORDER: Insight["severity"][] = ["critical", "warning", "info", "good"];

export function Insights({ items, className, limit = 6, subtitle = "ranked findings: evidence, where to look, and a next step for each" }: {
  items: Insight[] | undefined; className?: string; limit?: number; subtitle?: string;
}) {
  const [all, setAll] = React.useState(false);
  if (!items?.length) return null;
  const counts = items.reduce<Record<string, number>>((a, i) => ({ ...a, [i.severity]: (a[i.severity] ?? 0) + 1 }), {});
  const shown = all ? items : items.slice(0, limit);
  return (
    <Card className={className}>
      <CardHeader title="Insights" subtitle={subtitle}
        actions={<div className="flex flex-wrap gap-1.5">
          {ORDER.filter((k) => counts[k]).map((k) => <Badge key={k} tone={SEV[k].tone}>{counts[k]} {k}</Badge>)}
        </div>} />
      <CardBody>
        <ul className="grid grid-cols-1 gap-2.5 lg:grid-cols-2">
          {shown.map((i, n) => {
            const s = SEV[i.severity];
            const Icon = s.icon;
            const body = (
              <div className={cn("h-full rounded-xl border p-3.5 transition", s.ring, i.anchor && "hover:border-accent/50")}>
                <div className="flex gap-2.5">
                  <Icon className={cn("mt-0.5 size-4 shrink-0", s.cls)} aria-label={i.severity} />
                  <div className="min-w-0">
                    <div className="text-[13px] font-medium leading-snug text-fg">{i.title}</div>
                    {i.detail && <div className="mt-1 text-[12px] leading-snug text-muted">{i.detail}</div>}
                    {i.action && <div className="mt-1.5 text-[12px] leading-snug text-fg/80"><span className="text-subtle">Next: </span>{i.action}</div>}
                    {i.area && <div className="mt-1.5 text-[10.5px] uppercase tracking-wider text-subtle">{i.area}</div>}
                  </div>
                </div>
              </div>
            );
            return <li key={n}>{i.anchor ? <a href={`#${i.anchor}`} className="block h-full">{body}</a> : body}</li>;
          })}
        </ul>
        {items.length > limit && (
          <button type="button" onClick={() => setAll((a) => !a)} className="mt-3 text-[12.5px] font-medium text-accent hover:underline cursor-pointer">
            {all ? "Show fewer" : `Show all ${items.length} insights`}
          </button>
        )}
      </CardBody>
    </Card>
  );
}
