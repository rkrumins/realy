/**
 * Report analysis: ranked insights (python/realy/report/insights.py), the model's honesty card
 * (in- / out-of-sample, AUCs, fallbacks), label coverage at the training cut, the base-rate
 * projection, and a business view whose cost assumptions are editable here (recomputed live).
 */
import { AlertOctagon, AlertTriangle, CheckCircle2, Info, Scale, ShieldQuestion, Tags, Wallet } from "lucide-react";
import * as React from "react";
import { Link } from "react-router";
import { MiniBar } from "@/components/common";
import { Badge } from "@/components/ui/badge";
import { Card, CardBody, CardHeader } from "@/components/ui/card";
import { NumberField } from "@/components/ui/input";
import { cn } from "@/lib/cn";
import { fmtNum, fmtPct, fmtUsd, isNum } from "@/lib/format";
import type { Detection, Insight, InsightsConfig, Report, StepUp, StepUpSplit } from "@/lib/types";

const SEV: Record<Insight["severity"], { icon: typeof Info; cls: string; ring: string; label: string }> = {
  critical: { icon: AlertOctagon, cls: "text-fail", ring: "border-fail/35 bg-fail/[0.06]", label: "critical" },
  warning: { icon: AlertTriangle, cls: "text-warn", ring: "border-warn/30 bg-warn/[0.05]", label: "warning" },
  info: { icon: Info, cls: "text-info", ring: "border-border bg-surface-2/40", label: "info" },
  good: { icon: CheckCircle2, cls: "text-pass", ring: "border-pass/25 bg-pass/[0.05]", label: "good" },
};

function insightHref(i: Insight, runId: string): string | null {
  const l = i.link;
  if (!l) return null;
  switch (l.kind) {
    case "explorer": return `/explorer?run=${encodeURIComponent(runId)}${l.query ? `&q=${encodeURIComponent(l.query)}` : ""}`;
    case "compare": return l.a && l.b ? `/compare?a=${encodeURIComponent(l.a)}&b=${encodeURIComponent(l.b)}` : null;
    case "rule": return "#rules";
    case "labels": return "#labels";
    case "startup": return "#startup";
    case "latency": return "#latency";
    case "verify": return "#verify";
    case "models": return "#evaluation";
    case "stepup": return "#stepup";
    default: return null;
  }
}

export function Insights({ items, runId }: { items: Insight[] | undefined; runId: string }) {
  const [all, setAll] = React.useState(false);
  if (!items?.length) return null;
  const counts = items.reduce<Record<string, number>>((a, i) => ({ ...a, [i.severity]: (a[i.severity] ?? 0) + 1 }), {});
  const shown = all ? items : items.slice(0, 6);
  return (
    <Card className="mt-6">
      <CardHeader title="Insights" subtitle="what this run tells you, ranked: evidence, a drill-down and a next step for each finding"
        actions={<div className="flex flex-wrap gap-1.5">
          {(["critical", "warning", "info", "good"] as const).filter((k) => counts[k]).map((k) => (
            <Badge key={k} tone={k === "critical" ? "fail" : k === "warning" ? "warn" : k === "good" ? "pass" : "neutral"}>{counts[k]} {k}</Badge>
          ))}
        </div>} />
      <CardBody>
        <ul className="grid grid-cols-1 gap-2.5 lg:grid-cols-2">
          {shown.map((i, n) => {
            const s = SEV[i.severity];
            const href = insightHref(i, runId);
            const Icon = s.icon;
            const body = (
              <div className={cn("h-full rounded-xl border p-3.5 transition", s.ring, href && "hover:border-accent/50")}>
                <div className="flex gap-2.5">
                  <Icon className={cn("mt-0.5 size-4 shrink-0", s.cls)} aria-label={s.label} />
                  <div className="min-w-0">
                    <div className="text-[13px] font-medium leading-snug text-fg">{i.title}</div>
                    {i.detail && <div className="mt-1 text-[12px] leading-snug text-muted">{i.detail}</div>}
                    {i.action && <div className="mt-1.5 text-[12px] leading-snug text-fg/80"><span className="text-subtle">Next: </span>{i.action}</div>}
                    <div className="mt-1.5 text-[10.5px] uppercase tracking-wider text-subtle">{i.area}</div>
                  </div>
                </div>
              </div>
            );
            return <li key={n}>{href ? (href.startsWith("#") ? <a href={href} className="block h-full">{body}</a> : <Link to={href} className="block h-full">{body}</Link>) : body}</li>;
          })}
        </ul>
        {items.length > 6 && (
          <button type="button" onClick={() => setAll((a) => !a)} className="mt-3 text-[12.5px] font-medium text-accent hover:underline">
            {all ? "Show fewer" : `Show all ${items.length} insights`}
          </button>
        )}
      </CardBody>
    </Card>
  );
}

export function ModelEvaluation({ d }: { d: Detection }) {
  const m = d.model;
  const e = d.evaluation;
  if (!m && !e) return null;
  const total = (m?.scored ?? 0) + (m?.rules_only_fallback ?? 0);
  const fb = total ? (m?.rules_only_fallback ?? 0) / total : null;
  const tone = e?.sample === "out-of-sample" ? "pass" : e?.sample === "in-sample" ? "fail" : "warn";
  return (
    <Card id="evaluation" className="scroll-mt-20">
      <CardHeader title="Model & evaluation" subtitle="is this number honest? the served model, its training data and how it ranks fraud"
        actions={e?.sample && <Badge tone={tone}>{e.sample}</Badge>} />
      <CardBody className="grid grid-cols-2 gap-4 md:grid-cols-4">
        <div><div className="text-[11px] uppercase tracking-wider text-subtle">served model</div><div className="mt-1 font-mono text-[12.5px] text-fg">{e?.model_version ?? "—"}</div>
          {e?.labels && <div className="text-[11.5px] text-muted">labels: {e.labels}</div>}</div>
        <div><div className="text-[11px] uppercase tracking-wider text-subtle">ROC-AUC / PR-AUC</div><div className="mt-1 text-[18px] font-semibold num">{isNum(m?.roc_auc) ? m!.roc_auc.toFixed(3) : "—"} <span className="text-muted">/ {isNum(m?.pr_auc) ? m!.pr_auc.toFixed(3) : "—"}</span></div>
          <div className="text-[11.5px] text-muted">on model-scored decisions</div></div>
        <div><div className="text-[11px] uppercase tracking-wider text-subtle">rules-only fallbacks</div><div className={cn("mt-1 text-[18px] font-semibold num", isNum(fb) && fb > 0.01 && "text-warn")}>{fmtPct(fb, 2)}</div>
          <div className="text-[11.5px] text-muted">{fmtNum(m?.rules_only_fallback)} decisions; rules alone flagged {fmtPct(m?.fallback_recall_flagged, 0)} of their fraud</div></div>
        <div><div className="text-[11px] uppercase tracking-wider text-subtle">trained on</div>
          <div className="mt-1 text-[12px] text-muted">{e?.trained_on?.length ? e.trained_on.map((t) => <div key={t.run_id} className="font-mono text-[11.5px]">{t.run_id}{isNum(t.seed) ? ` · seed ${t.seed}` : ""}</div>) : (e?.reason ?? "—")}</div></div>
        {e?.reason && e.sample !== "out-of-sample" && <div className="col-span-2 rounded-lg border border-warn/30 bg-warn/[0.06] p-2.5 text-[12px] text-warn md:col-span-4">{e.reason}</div>}
      </CardBody>
    </Card>
  );
}

export function LabelCoverage({ d }: { d: Detection }) {
  const l = d.labels;
  if (!l) return null;
  const rows = Object.entries(l.by_type).sort((a, b) => a[1] - b[1]);
  return (
    <Card id="labels" className="scroll-mt-20">
      <CardHeader title="Labels: what training would see" icon={<Tags />}
        subtitle={`share of this run's fraud labelled by a training set cut ${l.maturity_days} days after the window (late chargebacks and unreported fraud train as legitimate)`}
        actions={<Badge tone={l.coverage >= 0.85 ? "pass" : "warn"}>{fmtPct(l.coverage, 0)} overall</Badge>} />
      <CardBody>
        <div className="grid grid-cols-1 gap-x-8 gap-y-1.5 sm:grid-cols-2">
          {rows.map(([name, c]) => (
            <div key={name} className="flex items-center gap-2 text-[12.5px]">
              <span className="w-36 truncate font-mono text-[12px]">{name}</span>
              <MiniBar value={c} tone={c >= 0.85 ? "pass" : undefined} className="flex-1" />
              <span className="w-12 text-right num">{fmtPct(c, 0)}</span>
            </div>
          ))}
        </div>
        <div className="mt-3 text-[12px] text-muted">{fmtPct(l.reported, 1)} of fraud is ever reported; the rest is never labelled.</div>
      </CardBody>
    </Card>
  );
}

export function BaseRateAndImpact({ r }: { r: Report }) {
  const d = r.detection;
  const cfg: InsightsConfig = r.insights_config ?? {};
  const [p, setP] = React.useState(cfg.production_base_rate ?? 0.001);
  const [review, setReview] = React.useState(cfg.costs?.review_cost_usd ?? 4);
  const [perHour, setPerHour] = React.useState(cfg.costs?.reviews_per_analyst_hour ?? 30);
  const [friction, setFriction] = React.useState(cfg.costs?.decline_friction_usd ?? 2.5);
  if (!isNum(d?.recall_flagged) || !isNum(d?.false_positive_rate)) return null;
  const rec = d.recall_flagged!, fpr = d.false_positive_rate!;
  const precAt = (rec * p) / (rec * p + fpr * (1 - p));
  const recD = d.recall_declined ?? 0;
  const fprD = d.legit_events ? (d.legit_declined ?? 0) / d.legit_events : 0;
  const precDAt = recD * p + fprD * (1 - p) > 0 ? (recD * p) / (recD * p + fprD * (1 - p)) : null;
  const measured = r.summary.measured_s ?? null;
  const reviews = (r.summary.outcomes?.REVIEW ?? 0);
  const perHr = isNum(measured) && measured > 0 ? (reviews / measured) * 3600 : null;
  const reviewCost = reviews * review;
  const frictionCost = (d.legit_declined ?? 0) * friction;
  const loss = d.fraud_usd_paid ?? d.fraud_usd_approved ?? 0;      // the money that moved
  const st = d.stepup;
  const prevented = (d.fraud_usd_total ?? 0) - loss;
  const events = (d.fraud_events ?? 0) + (d.legit_events ?? 0);
  const total = loss + reviewCost + frictionCost;
  return (
    <div className="mt-4 grid grid-cols-1 gap-4 xl:grid-cols-2">
      <Card>
        <CardHeader title="At a real fraud rate" icon={<Scale />}
          subtitle="recall and false-positive rate carry over to a production portfolio; precision does not (this world is enriched with fraud)" />
        <CardBody>
          <NumberField label="production fraud rate" unit="%" value={p * 100} step={0.05} className="mb-3 max-w-56"
            onChange={(v) => setP(Math.max(0.001, Math.min(50, v ?? 0.1)) / 100)} />
          <table className="w-full text-[12.5px]">
            <thead className="text-left text-[11px] uppercase tracking-wider text-subtle"><tr><th className="py-1 font-medium" /><th className="py-1 text-right font-medium">measured</th><th className="py-1 text-right font-medium">at {fmtPct(p, 2)}</th></tr></thead>
            <tbody>
              <tr className="border-t border-border"><td className="py-1.5">flags that are fraud</td><td className="text-right num">{fmtPct(d.precision_flagged, 1)}</td>
                <td className="text-right num font-medium">{fmtPct(precAt, 1)} <span className="text-muted">(1 in {fmtNum(Math.max(1, Math.round(1 / precAt)))})</span></td></tr>
              <tr className="border-t border-border"><td className="py-1.5">declines that are fraud</td><td className="text-right num">{d.fraud_declined !== undefined && (d.fraud_declined + (d.legit_declined ?? 0)) > 0 ? fmtPct(d.fraud_declined / (d.fraud_declined + (d.legit_declined ?? 0)), 1) : "—"}</td>
                <td className="text-right num font-medium">{fmtPct(precDAt, 1)}</td></tr>
            </tbody>
          </table>
          <div className="mt-2 text-[11.5px] text-subtle">precision = recall·p / (recall·p + FPR·(1 − p)); recall {fmtPct(rec, 1)}, FPR {fmtPct(fpr, 2)}</div>
        </CardBody>
      </Card>
      <Card>
        <CardHeader title="Business impact" icon={<Wallet />} subtitle="this run's outcomes priced with your assumptions (defaults from contracts/insights.yaml)" />
        <CardBody>
          <div className="mb-3 grid grid-cols-1 gap-2 sm:grid-cols-3">
            <NumberField label="review cost" unit="$" value={review} step={0.5} onChange={(v) => setReview(v ?? 0)} />
            <NumberField label="reviews / analyst-hour" value={perHour} step={5} onChange={(v) => setPerHour(v ?? 1)} />
            <NumberField label="good decline cost" unit="$" value={friction} step={0.5} onChange={(v) => setFriction(v ?? 0)} />
          </div>
          <table className="w-full text-[12.5px]">
            <tbody>
              <tr><td className="py-1">fraud prevented</td><td className="text-right num text-pass">{fmtUsd(prevented)}</td></tr>
              <tr className="border-t border-border"><td className="py-1">fraud lost (paid{isNum(d.fraud_usd_paid) ? ", incl. card payments reviewed after the fact" : ""})</td><td className="text-right num">{fmtUsd(loss)}</td></tr>
              <tr className="border-t border-border"><td className="py-1">reviews: {fmtNum(reviews)}{isNum(perHr) ? ` (${fmtNum(perHr)}/h → ${fmtNum(perHr / Math.max(perHour, 1), 1)} analysts)` : ""}</td><td className="text-right num">{fmtUsd(reviewCost)}</td></tr>
              <tr className="border-t border-border"><td className="py-1">good customers declined: {fmtNum(d.legit_declined)}</td><td className="text-right num">{fmtUsd(frictionCost)}</td></tr>
              {st && <tr className="border-t border-border"><td className="py-1">good customers stepped up: {fmtNum(st.legit_challenged)} ({fmtNum(st.legit_failed)} failed it)</td><td className="text-right num text-muted">friction</td></tr>}
              <tr className="border-t border-border font-medium"><td className="py-1.5">total cost</td><td className="text-right num">{fmtUsd(total)} <span className="text-[11.5px] font-normal text-muted">({fmtUsd(events ? (total / events) * 1000 : 0)} per 1,000 payments)</span></td></tr>
            </tbody>
          </table>
          {st && st.legit_abandoned > 0 && (
            <div className="mt-2 text-[11.5px] text-muted">not in the total: {fmtNum(st.legit_abandoned)} good customers abandoned at a step-up, {fmtUsd(st.lost_sales_usd)} of sales the merchants lost</div>
          )}
        </CardBody>
      </Card>
    </div>
  );
}

function Split({ x, total }: { x: StepUpSplit; total: number }) {
  const seg = [["pass", x.pass, "bg-pass/70"], ["fail", x.fail, "bg-fail/70"], ["abandon", x.abandon, "bg-warn/70"]] as const;
  return (
    <div>
      <div className="flex h-2.5 overflow-hidden rounded-full bg-surface-3/60">
        {seg.map(([k, v, c]) => v > 0 && <div key={k} className={c} style={{ width: `${(v / Math.max(1, total)) * 100}%` }} title={`${k} ${v}`} />)}
      </div>
      <div className="mt-1 flex gap-3 text-[11px] text-muted num">
        {seg.map(([k, v]) => <span key={k}>{k} {fmtNum(v)}</span>)}
      </div>
    </div>
  );
}

/** Step-up (CHALLENGE) economics: who was challenged, who got through, and what it cost. */
export function StepUpCard({ s }: { s: StepUp }) {
  const types = Object.entries(s.by_type);
  return (
    <Card id="stepup" className="mt-4 scroll-mt-20">
      <CardHeader title="Step-up" icon={<ShieldQuestion />}
        subtitle="3DS / SCA challenges online, PIN prompts in store: a challenge stops fraud that has no OTP or PIN, not a phished account or a scam victim" />
      <CardBody className="space-y-5">
        <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
          <div><div className="text-[11.5px] text-muted">challenged</div><div className="text-[18px] font-semibold num">{fmtNum(s.challenged)}</div>
            <div className="text-[11px] text-subtle num">{fmtPct(s.legit_challenge_rate, 2)} of good customers · {fmtPct(s.fraud_challenge_rate, 1)} of fraud</div></div>
          <div><div className="text-[11.5px] text-muted">fraud stopped at the step-up</div><div className="text-[18px] font-semibold text-pass num">{fmtNum(s.fraud_stopped)}</div>
            <div className="text-[11px] text-subtle num">of {fmtNum(s.fraud_challenged)} challenged</div></div>
          <div><div className="text-[11.5px] text-muted">fraud that passed it</div><div className={cn("text-[18px] font-semibold num", s.fraud_passed > 0 && "text-warn")}>{fmtNum(s.fraud_passed)}</div>
            <div className="text-[11px] text-subtle num">{fmtUsd(s.fraud_usd_passed)} paid</div></div>
          <div><div className="text-[11.5px] text-muted">good customers who gave up</div><div className={cn("text-[18px] font-semibold num", s.legit_abandoned > 0 && "text-warn")}>{fmtNum(s.legit_abandoned)}</div>
            <div className="text-[11px] text-subtle num">{fmtUsd(s.lost_sales_usd)} of lost sales</div></div>
        </div>
        <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
          <div><div className="mb-1.5 text-[12px] font-medium text-fg">good customers ({fmtNum(s.legit_challenged)})</div><Split x={s.legit} total={s.legit_challenged} /></div>
          <div><div className="mb-1.5 text-[12px] font-medium text-fg">fraud ({fmtNum(s.fraud_challenged)})</div><Split x={s.fraud} total={s.fraud_challenged} /></div>
        </div>
        {types.length > 0 && (
          <div className="grid grid-cols-1 gap-x-6 gap-y-3 sm:grid-cols-2 xl:grid-cols-3">
            {types.map(([name, t]) => (
              <div key={name}>
                <div className="mb-1 flex items-baseline justify-between text-[12px]"><span className="font-medium text-fg">{name.replace(/_/g, " ")}</span>
                  <span className="text-[11px] text-subtle num">{fmtNum(t.challenged)} challenged{t.usd_passed > 0 ? ` · ${fmtUsd(t.usd_passed)} got through` : ""}</span></div>
                <Split x={t} total={t.challenged} />
              </div>
            ))}
          </div>
        )}
        <div className="text-[11px] text-subtle">{s.note}</div>
      </CardBody>
    </Card>
  );
}
