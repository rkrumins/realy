/**
 * Why a decision came out as it did (policy path, every rule with the values it saw, the model's
 * feature attribution), and why the event is labelled as it is (ground truth, campaign role and
 * step, what the bank learns and when, and what a training set cut N days later makes of it).
 * Backed by python/realy/explain.py through the Explorer's decision endpoint.
 */
import { BookOpen, Brain, Check, Gavel, Minus, ShieldQuestion } from "lucide-react";
import * as React from "react";
import { Badge } from "@/components/ui/badge";
import { Card, CardBody, CardHeader } from "@/components/ui/card";
import { Segmented } from "@/components/ui/controls";
import { cn } from "@/lib/cn";
import { isNum } from "@/lib/format";
import type { Explanation, LabelStory } from "@/lib/types";

function val(v: unknown): string {
  if (typeof v === "number") return Number.isInteger(v) ? String(v) : v.toFixed(Math.abs(v) < 10 ? 3 : 1);
  return String(v);
}

export function LabelStoryCard({ story, maturity, onMaturity }: { story?: LabelStory; maturity: number; onMaturity: (d: number) => void }) {
  if (!story) return null;
  const legitInTraining = story.training.startsWith("LEGITIMATE");
  return (
    <Card>
      <CardHeader title="Label story" icon={<BookOpen />} subtitle="what really happened, what the bank learns, and when"
        actions={<Segmented value={String(maturity)} onChange={(v) => onMaturity(Number(v))}
          options={[{ value: "30", label: "30 d" }, { value: "60", label: "60 d" }, { value: "90", label: "90 d" }]} />} />
      <CardBody className="space-y-3 text-[13px]">
        <div className="flex flex-wrap items-center gap-2">
          <Badge tone={story.truth === "fraud" ? "fail" : "pass"}>{story.truth}</Badge>
          <span className="font-medium text-fg">{story.title}</span>
        </div>
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
          <div className="rounded-xl border border-border bg-surface-2/40 p-3">
            <div className="text-[11px] uppercase tracking-wider text-subtle">what the bank learns</div>
            <div className="mt-1 text-fg">{story.observed}</div>
          </div>
          <div className={cn("rounded-xl border p-3", legitInTraining && story.truth === "fraud" ? "border-warn/40 bg-warn/[0.07]" : "border-border bg-surface-2/40")}>
            <div className="text-[11px] uppercase tracking-wider text-subtle">in a training set cut {story.maturity_days} days later</div>
            <div className={cn("mt-1", legitInTraining && story.truth === "fraud" ? "text-warn" : "text-fg")}>{story.training}</div>
          </div>
        </div>
        {(story.campaign_id || story.label_source) && (
          <div className="flex flex-wrap gap-x-5 gap-y-1 text-[12px] text-muted">
            {story.campaign_id && <span>campaign <span className="font-mono text-fg">{story.campaign_id}</span></span>}
            {story.role && <span>role <span className="font-mono text-fg">{story.role}</span>{story.step ? ` · step ${story.step.index} of ${story.step.of}` : ""}</span>}
            {story.label_source && <span>label via <span className="text-fg">{story.label_source}</span>{isNum(story.label_delay_days) ? ` after ${story.label_delay_days} days` : ""}</span>}
          </div>
        )}
      </CardBody>
    </Card>
  );
}

export function ExplanationCard({ x }: { x?: Explanation | null }) {
  const [allRules, setAllRules] = React.useState(false);
  if (!x) return null;
  if (x.error) return <Card className="p-5 text-[12.5px] text-muted">Explanation unavailable: {x.error}</Card>;
  const rules = x.rules ?? [];
  const hits = rules.filter((r) => r.hit);
  const served = new Set(x.served_rule_hits ?? []);
  const agrees = hits.length === served.size && hits.every((r) => served.has(r.id));
  const shown = allRules ? rules : hits;
  const a = x.attribution;
  const maxAbs = Math.max(1e-9, ...(a?.top ?? []).map((t) => Math.abs(t.contribution)));
  return (
    <Card>
      <CardHeader title="Why this decision" icon={<Gavel />}
        subtitle={`rule set ${x.rule_set ?? "?"}${x.rules_source === "current" ? " (the current file: this run predates rule snapshots)" : " (as the run used it)"} re-evaluated on the served features; the model attributed with ${a?.method ?? "integrated gradients"}`}
        actions={<Badge tone={agrees ? "pass" : "warn"}>{agrees ? "matches what was served" : "differs from served hits"}</Badge>} />
      <CardBody className="space-y-5">
        {x.policy && (
          <div className="rounded-xl border border-accent/30 bg-accent/[0.06] p-3.5">
            <div className="text-[11px] uppercase tracking-wider text-subtle">policy path · {x.policy.branch}</div>
            <div className="mt-1 text-[13.5px] font-medium text-fg">{x.policy.outcome}: {x.policy.why}</div>
            <div className="mt-1 text-[11.5px] text-muted">
              precedence: DECLINE rule → ALLOW rule → score ≥ {x.policy.thresholds.decline}
              {isNum(x.policy.thresholds.challenge) ? <> → CHALLENGE rule → score ≥ {x.policy.thresholds.challenge} (if it can be stepped up)</> : " → CHALLENGE rule"}
              {" "}→ REVIEW rule → score ≥ {x.policy.thresholds.review} → approve
              {x.policy.challengeable === false && <span className="text-subtle"> · this payment cannot be stepped up</span>}
            </div>
            {x.policy.expected && x.policy.outcome && x.policy.expected !== x.policy.outcome && (
              <div className="mt-1.5 text-[11.5px] text-warn">re-derived {x.policy.expected}, served {x.policy.outcome} (e.g. a live threshold change or the deadline policy)</div>
            )}
          </div>
        )}
        {x.stepup && (
          <div className={cn("rounded-xl border p-3.5 text-[12.5px]", x.stepup.challenged ? "border-info/35 bg-info/[0.06]" : "border-border bg-surface-2/40")}>
            <div className="flex items-center gap-1.5 text-[11px] uppercase tracking-wider text-subtle"><ShieldQuestion className="size-3.5" />step-up</div>
            <div className="mt-1 text-fg">
              {x.stepup.challenged ? x.stepup.final
                : <>not challenged; had it been, the payer would <b>{x.stepup.result}</b> the step-up</>}
            </div>
            <div className="mt-0.5 text-[11px] text-subtle">simulated by the instrument from the payer's population (scenario fraud.challenge, the typology's rates); the pipeline never sees it</div>
          </div>
        )}
        <div>
          <div className="mb-2 flex items-center justify-between">
            <div className="text-[12px] font-semibold uppercase tracking-[0.1em] text-subtle">rules ({hits.length} of {rules.length} fired)</div>
            <button type="button" onClick={() => setAllRules((v) => !v)} className="text-[12px] text-accent hover:underline">{allRules ? "fired only" : "show all rules"}</button>
          </div>
          {shown.length === 0 ? <div className="text-[12.5px] text-subtle">No rule fired.</div> : (
            <ul className="divide-y divide-border rounded-xl border border-border">
              {shown.map((r) => (
                <li key={r.id} className="flex gap-3 p-3">
                  {r.hit ? <Check className="mt-0.5 size-4 shrink-0 text-accent" aria-label="fired" /> : <Minus className="mt-0.5 size-4 shrink-0 text-subtle" aria-label="did not fire" />}
                  <div className="min-w-0 flex-1">
                    <div className="flex flex-wrap items-center gap-2"><span className="font-mono text-[12.5px] font-medium">{r.id}</span>
                      <Badge tone={r.action === "DECLINE" ? "fail" : r.action === "REVIEW" ? "warn" : r.action === "CHALLENGE" ? "info" : "neutral"}>{r.action}</Badge></div>
                    <code className="mt-1 block whitespace-pre-wrap break-words font-mono text-[11.5px] text-muted">{r.when}</code>
                    {Object.keys(r.values).length > 0 && (
                      <div className="mt-1.5 flex flex-wrap gap-1.5">
                        {Object.entries(r.values).map(([k, v]) => (
                          <span key={k} className="rounded-md border border-border bg-surface-2 px-1.5 py-0.5 font-mono text-[11px]">{k} = <b className="text-fg">{val(v)}</b></span>
                        ))}
                      </div>
                    )}
                  </div>
                </li>
              ))}
            </ul>
          )}
        </div>
        {a && (
          <div>
            <div className="mb-1 flex flex-wrap items-baseline justify-between gap-2">
              <div className="flex items-center gap-1.5 text-[12px] font-semibold uppercase tracking-[0.1em] text-subtle"><Brain className="size-3.5" />what moved the score</div>
              <div className="text-[12px] text-muted num">average customer {a.baseline_score.toFixed(3)} → this event {a.score.toFixed(3)}{Math.abs(a.score - a.served_score) > 0.002 ? ` (served ${a.served_score.toFixed(3)})` : ""}</div>
            </div>
            <ul className="space-y-1.5">
              {a.top.map((t) => {
                const w = (Math.abs(t.contribution) / maxAbs) * 50;
                const up = t.contribution > 0;
                return (
                  <li key={t.feature} className="grid grid-cols-[minmax(0,11rem)_minmax(0,1fr)_minmax(0,9rem)] items-center gap-2 text-[12px]">
                    <span className="truncate font-mono" title={t.feature}>{t.feature}</span>
                    <div className="relative h-3 rounded bg-surface-3/60">
                      <div className="absolute inset-y-0 left-1/2 w-px bg-border-strong" />
                      <div className={cn("absolute inset-y-0 rounded", up ? "left-1/2 bg-fail/70" : "right-1/2 bg-pass/70")} style={{ width: `${w}%` }} />
                    </div>
                    <span className="truncate text-right font-mono text-[11px] text-muted" title={`typical ${val(t.typical)}`}>{val(t.value)} <span className="text-subtle">vs {val(t.typical)}</span></span>
                  </li>
                );
              })}
            </ul>
            <div className="mt-1.5 text-[11px] text-subtle">red pushes towards fraud, green away; contributions to the logit sum to the difference from the average customer ({a.model_version ?? "model"})</div>
          </div>
        )}
      </CardBody>
    </Card>
  );
}
