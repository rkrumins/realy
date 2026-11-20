import { ExplanationCard, LabelStoryCard } from "@/components/explain";
import { useQuery } from "@tanstack/react-query";
import { ChevronDown, ChevronRight, CreditCard, ExternalLink, Fingerprint, Flag, Layers, Network, ScanSearch, Search, ShieldAlert, Store, Timer, X } from "lucide-react";
import * as React from "react";
import { useSearchParams } from "react-router";
import { CartesianGrid, ResponsiveContainer, Scatter, ScatterChart, Tooltip, XAxis, YAxis, ZAxis } from "recharts";
import { EmptyState, ErrorState, PageHeader, Stat, Unavailable } from "@/components/common";
import { OutcomeBadge, ScoreBar } from "@/components/feeds";
import { SpanWaterfall } from "@/components/waterfall";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardBody, CardHeader } from "@/components/ui/card";
import { Segmented, Skeleton } from "@/components/ui/controls";
import { Input } from "@/components/ui/input";
import { Select } from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { api, isNotFound } from "@/lib/api";
import { cn } from "@/lib/cn";
import { fmtFeature, fmtMs, fmtNum, fmtPct, fmtScore, fmtSimTime, fmtUsd, isNum, titleCase } from "@/lib/format";
import type { DecisionDetail, EntityDef, EntityResponse, EntityRow, ExplorerViews, Lens, SearchRow, TrailRow, ViewColumn, ViewContext } from "@/lib/types";

const OUTCOME_COLOR: Record<string, string> = { APPROVE: "var(--pass)", CHALLENGE: "var(--info)", REVIEW: "var(--warn)", DECLINE: "var(--fail)" };
const UNSCORED: Record<string, string> = {
  RULES_ONLY_FALLBACK: "rules only: the model missed its deadline",
  PROVISIONAL: "provisional (rules only)",
  ECHO: "echo: not scored",
};
type OpenEntity = (entity: string, id: string) => void;

function useParam(k: string) {
  const [p, setP] = useSearchParams();
  const v = p.get(k);
  const set = React.useCallback((val: string | null, also?: Record<string, string | null>) => setP((cur) => {
    if (val) cur.set(k, val); else cur.delete(k);
    for (const [kk, vv] of Object.entries(also ?? {})) { if (vv) cur.set(kk, vv); else cur.delete(kk); }
    return cur;
  }, { replace: true }), [k, setP]);
  return [v, set] as const;
}

const short = (id: string) => (id.length > 8 ? `…${id.slice(-6)}` : id);

/** An entity id that opens that entity's trail. */
function IdLink({ entity, id, onEntity, full, className }: { entity: string; id: string | null | undefined; onEntity: OpenEntity; full?: boolean; className?: string }) {
  if (!id) return <span className="text-subtle">—</span>;
  return (
    <button type="button" title={`${entity} ${id}`} onClick={(e) => { e.stopPropagation(); onEntity(entity, id); }}
      className={cn("font-mono text-[11.5px] text-accent hover:underline cursor-pointer", className)}>{full || entity === "ip" ? id : short(id)}</button>
  );
}

function ScoreCell({ r, className }: { r: Pick<SearchRow, "score" | "path" | "model_version">; className?: string }) {
  if (isNum(r.score)) return <span title={r.model_version ? `model ${r.model_version}` : undefined}><ScoreBar score={r.score} className={className} /></span>;
  return <span className="text-[11px] text-muted">{(r.path && UNSCORED[r.path]) || "no score"}</span>;
}

function FateNote({ r }: { r: Pick<SearchRow, "outcome" | "fate" | "stepup"> }) {
  if (r.outcome === "CHALLENGE" && r.stepup) return <span className="text-[10.5px] text-muted">step-up {r.stepup}</span>;
  if (r.outcome === "REVIEW" && r.fate && r.fate !== "held for review") return <span className="text-[10.5px] text-muted" title={r.fate}>paid</span>;
  return null;
}

function ResultsTable({ rows, onPick, selected, onEntity }: { rows: SearchRow[]; onPick: (id: string) => void; selected: string | null; onEntity: OpenEntity }) {
  return (
    <div className={cn("overflow-auto", selected ? "max-h-[340px]" : "max-h-[640px]")}>
      <table className="w-full min-w-[960px] text-[12.5px]">
        <thead className="sticky top-0 z-10 border-y border-border bg-surface-2 text-left text-[11px] uppercase tracking-wider text-subtle">
          <tr><th className="px-5 py-2 font-medium">Event time</th><th className="px-3 py-2 font-medium">Event</th><th className="px-3 py-2 font-medium">Customer</th>
            <th className="px-3 py-2 text-right font-medium">Amount</th><th className="px-3 py-2 font-medium">Type</th><th className="px-3 py-2 font-medium">Outcome</th>
            <th className="px-3 py-2 font-medium">Score</th><th className="px-3 py-2 font-medium">Truth</th><th className="px-5 py-2 text-right font-medium">e2e</th></tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.event_id} onClick={() => onPick(r.event_id)} data-testid="search-row"
              className={cn("cursor-pointer border-b border-border last:border-0 hover:bg-surface-2/60", selected === r.event_id && "bg-accent/8")}>
              <td className="whitespace-nowrap px-5 py-2 font-mono text-[11.5px] text-muted">{fmtSimTime(r.event_time_us)}</td>
              <td className="px-3 py-2 font-mono text-[11.5px]">{r.event_id}</td>
              <td className="px-3 py-2"><IdLink entity="customer" id={r.customer_id} onEntity={onEntity} full /></td>
              <td className="px-3 py-2 text-right num">{fmtUsd(r.amount_usd)}</td>
              <td className="px-3 py-2 text-muted">{[titleCase(r.payment_type.toLowerCase().replace(/_/g, " ")), r.channel.toLowerCase().replace(/_/g, " "), r.merchant_country].filter(Boolean).join(" · ")}</td>
              <td className="px-3 py-2"><div className="flex items-center gap-1.5"><OutcomeBadge outcome={r.outcome} /><FateNote r={r} /></div></td>
              <td className="px-3 py-2"><ScoreCell r={r} /></td>
              <td className="px-3 py-2">{r.is_fraud ? <Badge tone="fail"><ShieldAlert />{r.fraud_type.replace(/_/g, " ")}</Badge> : <span className="text-subtle">genuine</span>}</td>
              <td className="px-5 py-2 text-right num text-muted">{fmtMs(r.e2e_ms)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function FeatureGroups({ features }: { features: NonNullable<DecisionDetail["features"]> }) {
  const groups = new Map<string, typeof features>();
  for (const f of features) {
    const e = f.entity || "derived";
    if (!groups.has(e)) groups.set(e, []);
    groups.get(e)!.push(f);
  }
  return (
    <div className="grid grid-cols-1 gap-x-6 gap-y-5 md:grid-cols-2 xl:grid-cols-3">
      {[...groups.entries()].map(([entity, fs]) => (
        <div key={entity} className="min-w-0">
          <div className="mb-1.5 text-[11px] font-semibold uppercase tracking-[0.12em] text-subtle">{entity}</div>
          {fs.map((f) => (
            <div key={f.name} className="flex items-baseline gap-2 border-b border-border py-1 text-[12px] last:border-0"
              title={`${f.source}${f.agg ? ` · ${f.agg}` : ""} · default ${f.default}${f.missing ? " · missing: the entity was absent or unseen, so the default was used" : ""}`}>
              <span className={cn("truncate font-mono text-[11.5px]", f.missing ? "text-subtle" : "text-muted")}>{f.name}</span>
              <span className={cn("ml-auto font-mono text-[11.5px] num", f.missing ? "text-subtle" : "text-fg")}>
                {f.missing ? `n/a (${fmtFeature(f.value)})` : fmtFeature(f.value)}</span>
            </div>
          ))}
        </div>
      ))}
    </div>
  );
}

/** The transaction's fields: the entity ids link to their trails; zero / unspecified values are left out. */
function TransactionFields({ ev, onEntity }: { ev: Record<string, unknown>; onEntity: OpenEntity }) {
  const flag = (k: string) => (typeof ev[k] === "boolean" ? (ev[k] ? "yes" : "no") : undefined);
  const plain: [string, unknown][] = [
    ["amount", fmtUsd(ev.amount_usd as number)], ["payment type", ev.payment_type], ["channel", ev.channel], ["entry mode", ev.entry_mode],
    ["card type", ev.card_type], ["merchant country", ev.merchant_country], ["MCC", ev.mcc], ["recurring", ev.recurring ? "yes" : undefined],
    ["3-D Secure", ev.channel === "ECOM" || ev.channel === "IN_APP" ? String(ev.three_ds ?? "").replace("THREE_DS_", "").toLowerCase() : undefined],
    ["CVV match", ev.card_present === false && ev.card_id ? flag("cvv_match") : undefined],
    ["AVS match", ev.card_present === false && ev.card_id ? flag("avs_match") : undefined],
    ["PIN verified", ev.card_present ? flag("pin_verified") : undefined],
    ["cryptogram valid", ev.card_present ? flag("cryptogram_valid") : undefined],
    ["scheme score", isNum(ev.scheme_score) && ev.scheme_score > 0 ? ev.scheme_score : undefined],
    ["IP country", ev.ip_country || undefined], ["IP ASN", ev.ip_asn || undefined],
    ["event time", fmtSimTime(ev.event_time_us as number)],
  ];
  const ids: [string, string, unknown][] = [["card", "card", ev.card_id], ["account", "account", ev.account_id], ["merchant", "merchant", ev.merchant_id],
    ["terminal", "terminal", ev.terminal_id], ["device", "device", ev.device_id], ["payee", "payee", ev.payee_id], ["IP", "ip", ev.ip]];
  return (
    <>
      {plain.filter(([, v]) => v !== undefined && v !== null && v !== "" && v !== "0").map(([k, v]) => <Stat key={k} label={k} value={String(v)} />)}
      {ids.filter(([, , v]) => v).map(([label, entity, v]) => <Stat key={label} label={label} value={<IdLink entity={entity} id={String(v)} onEntity={onEntity} full />} />)}
    </>
  );
}

function ScoreHeadline({ d }: { d: DecisionDetail }) {
  const s = d.decision?.score;
  return (
    <div className="text-right">
      <div className="text-[11px] uppercase tracking-wider text-subtle">score</div>
      <div className="text-[28px] font-semibold leading-none num" title={isNum(s) ? String(s) : d.decision?.score_note ?? undefined}>{isNum(s) ? fmtScore(s) : "—"}</div>
      {!isNum(s) && d.decision?.score_note && <div className="mt-1 max-w-[220px] text-[11px] text-muted">{d.decision.score_note}</div>}
      <div className="mt-1 text-[12px] text-muted num">e2e {fmtMs(d.timing.e2e_ms)}</div>
    </div>
  );
}

function DecisionPanel({ run, eventId, onClose, onEntity }: { run: string; eventId: string; onClose: () => void; onEntity: OpenEntity }) {
  const [maturity, setMaturity] = React.useState(30);
  const q = useQuery({ queryKey: ["decision", run, eventId, maturity], queryFn: () => api.decision(run, eventId, maturity), retry: 0,
    placeholderData: (prev) => prev });
  const top = React.useRef<HTMLDivElement>(null);
  React.useEffect(() => { if (q.data) top.current?.scrollIntoView({ behavior: "smooth", block: "start" }); }, [q.data, eventId]);
  if (q.isLoading) return <Card className="p-5"><Skeleton className="h-6 w-48" /><Skeleton className="mt-4 h-64" /></Card>;
  if (q.error) return <Card className="p-5"><ErrorState error={q.error} title="Decision not found" /></Card>;
  const d = q.data as DecisionDetail;
  const ev = d.event;
  const missing = (d.features ?? []).filter((f) => f.missing).length;
  return (
    <div className="scroll-mt-20 space-y-4" data-testid="decision-detail" ref={top}>
      <Card className="overflow-hidden">
        <div className="flex flex-wrap items-start gap-4 p-5">
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <OutcomeBadge outcome={d.decision?.outcome} />
              {d.decision?.degraded && <Badge tone="warn">degraded</Badge>}
              {d.decision && <Badge className="font-mono">{d.decision.path}</Badge>}
              {d.label.is_fraud ? <Badge tone="fail"><ShieldAlert />fraud · {d.label.fraud_type}{d.label.role ? ` · ${d.label.role.replace(/_/g, " ")}` : ""}</Badge> : <Badge tone="pass">genuine</Badge>}
              {d.label.backfill && <Badge tone="neutral">history replay</Badge>}
            </div>
            <h2 className="mt-2 font-mono text-[17px] font-semibold">event {eventId}</h2>
            <div className="mt-1 flex flex-wrap gap-x-4 text-[12.5px] text-muted">
              <span className="inline-flex items-center gap-1">customer <IdLink entity="customer" id={String(ev.customer_id ?? "")} onEntity={onEntity} full /></span>
              {d.label.campaign_id !== "0" && <span className="inline-flex items-center gap-1"><Flag className="size-3.5" />campaign {d.label.campaign_id}</span>}
              {d.decision && <span>model {d.decision.model_version || "—"}</span>}
              {d.decision?.revisions && d.decision.revisions > 1 && <span>{d.decision.revisions} revisions</span>}
            </div>
          </div>
          <ScoreHeadline d={d} />
          <Button variant="ghost" size="icon-sm" aria-label="Close" onClick={onClose}><X /></Button>
        </div>
        {!!d.warnings?.length && <div className="border-t border-border bg-warn/[0.06] px-5 py-2 text-[12px] text-muted">{d.warnings.join(" · ")}</div>}
        <div className="grid grid-cols-1 border-t border-border md:grid-cols-2">
          <div className="border-border p-5 md:border-r">
            <div className="mb-2 flex items-center gap-2 text-[12px] font-semibold uppercase tracking-[0.1em] text-subtle"><CreditCard className="size-3.5" />Transaction</div>
            <TransactionFields ev={ev} onEntity={onEntity} />
          </div>
          <div className="p-5">
            <div className="mb-2 flex items-center gap-2 text-[12px] font-semibold uppercase tracking-[0.1em] text-subtle"><Fingerprint className="size-3.5" />Rules fired</div>
            {d.decision?.rule_hits.length ? (
              <div className="flex flex-wrap gap-1.5">{d.decision.rule_hits.map((r) => <span key={r} className="rounded-md border border-border bg-surface-2 px-2 py-0.5 font-mono text-[11.5px]">{r}</span>)}</div>
            ) : <div className="text-[12.5px] text-subtle">No rule fired.</div>}
            {!!d.decision?.reason_codes.length && (
              <>
                <div className="mb-2 mt-4 text-[12px] font-semibold uppercase tracking-[0.1em] text-subtle">Reason codes</div>
                <div className="flex flex-wrap gap-1.5">{d.decision.reason_codes.map((r) => <Badge key={r} tone="accent" className="font-mono">{r}</Badge>)}</div>
              </>
            )}
            <div className="mb-1 mt-4 flex items-center gap-2 text-[12px] font-semibold uppercase tracking-[0.1em] text-subtle"><Timer className="size-3.5" />Timing</div>
            <Stat label="ingress mode" value={d.decision?.ingress_mode ?? "—"} />
            <Stat label="produce ack" value={isNum(d.timing.ack_us) ? fmtMs(d.timing.ack_us / 1000) : "—"} />
            <Stat label="intended → sent" value={isNum(d.timing.t_sent_us) && isNum(d.timing.t_intended_us) ? fmtMs((d.timing.t_sent_us - d.timing.t_intended_us) / 1000) : "—"} />
            <Stat label="end to end" value={fmtMs(d.timing.e2e_ms)} />
          </div>
        </div>
      </Card>
      <div className="grid grid-cols-1 gap-4 2xl:grid-cols-2">
        <LabelStoryCard story={d.label.story} maturity={maturity} onMaturity={setMaturity} />
        <ExplanationCard x={d.explanation} />
      </div>
      <Card>
        <CardHeader title="Span waterfall" subtitle="every service appends spans tagged with its clock domain; cross-domain gaps are transit" />
        <CardBody><SpanWaterfall detail={d} /></CardBody>
      </Card>
      <Card>
        <CardHeader title={`Features (${d.features?.length ?? 0})`}
          subtitle={d.features ? `${missing} n/a: the entity was absent from the payment or not yet seen, so the default was used · grouped by entity`
            : d.features_note ?? "this decision did not carry its feature vector"} />
        {d.features && <CardBody><FeatureGroups features={d.features} /></CardBody>}
      </Card>
    </div>
  );
}

/** A trail row opened in place: the decision's policy branch, the rules with the values they saw, the
 *  step-up's fate and every feature, without leaving the trail. */
function DecisionInline({ run, eventId, onOpen }: { run: string; eventId: string; onOpen: () => void }) {
  const q = useQuery({ queryKey: ["decision", run, eventId, 30], queryFn: () => api.decision(run, eventId, 30), retry: 0 });
  if (q.isLoading) return <div className="p-4"><Skeleton className="h-28" /></div>;
  if (q.error) return <div className="p-4"><ErrorState error={q.error} title="Decision not available" /></div>;
  const d = q.data as DecisionDetail;
  const x = d.explanation;
  const fired = (x?.rules ?? []).filter((r) => r.hit);
  return (
    <div className="space-y-4 bg-surface-2/30 p-4" data-testid="trail-drilldown">
      <div className="flex flex-wrap items-start gap-4">
        <div className="min-w-0 flex-1 space-y-1.5 text-[12.5px]">
          <div className="flex flex-wrap items-center gap-2">
            <OutcomeBadge outcome={d.decision?.outcome} />
            {d.decision && <Badge className="font-mono">{d.decision.path}</Badge>}
            {x?.policy && <span className="text-muted">{x.policy.branch}: {x.policy.why}</span>}
          </div>
          {x?.stepup?.challenged && <div className="text-muted">step-up: {x.stepup.final}</div>}
          <div className="text-muted">{d.label.story?.observed ?? (d.label.is_fraud ? `fraud (${d.label.fraud_type})` : "genuine")}</div>
          {fired.length > 0 && (
            <div className="space-y-1 pt-1">{fired.map((r) => (
              <div key={r.id} className="flex flex-wrap items-baseline gap-2">
                <span className="rounded-md border border-border bg-surface px-1.5 py-0.5 font-mono text-[11px]">{r.id}</span>
                <span className="text-[11px] uppercase tracking-wide text-subtle">{r.action}</span>
                <span className="font-mono text-[11px] text-muted">{Object.entries(r.values ?? {}).map(([k, v]) => `${k}=${fmtFeature(v)}`).join("  ")}</span>
              </div>
            ))}</div>
          )}
        </div>
        <div className="flex flex-col items-end gap-2">
          <ScoreHeadline d={d} />
          <Button size="sm" variant="secondary" onClick={onOpen}><ExternalLink />Full decision</Button>
        </div>
      </div>
      {d.features ? <FeatureGroups features={d.features} /> : <div className="text-[12px] text-subtle">{d.features_note ?? "No feature vector recorded for this decision."}</div>}
    </div>
  );
}

function ContextChips({ row, ctx, onEntity }: { row: TrailRow; ctx: ViewContext | undefined; onEntity: OpenEntity }) {
  if (!ctx) return <span className="text-subtle">—</span>;
  return (
    <div className="flex min-w-[340px] max-w-[520px] flex-wrap items-center gap-x-2.5 gap-y-0.5">
      <span className="text-[10.5px] font-semibold uppercase tracking-wide text-subtle">{ctx.title}</span>
      {ctx.columns.map((c) => {
        const v = row.ctx?.[c.key];
        if (c.link && typeof v === "string") return <span key={c.key} className="text-[11px] text-muted">{c.label} <IdLink entity={c.link} id={v} onEntity={onEntity} /></span>;
        const missing = v === null || v === undefined;
        if (missing && c.kind === "feature" && row.features_recorded === false) return null;
        return <span key={c.key} className={cn("text-[11px]", missing ? "text-subtle" : "text-muted")} title={c.key}>{c.label} <span className={cn("num", !missing && "text-fg")}>{fmtFeature(v, c.format)}</span></span>;
      })}
    </div>
  );
}

/** Every event of the entity in order: what each decision saw (the state the earlier events built, and
 *  the context that matters for that kind of payment) and what it did. Click a row to open it in place. */
function DecisionTrail({ run, rows, state, contexts, onPick, onEntity, entity }: {
  run: string; rows: TrailRow[]; state: ViewColumn[]; contexts: ViewContext[]; onPick: (id: string) => void; onEntity: OpenEntity; entity: string;
}) {
  const [open, setOpen] = React.useState<string | null>(null);
  // the opened row spans the whole (wide, horizontally scrolling) table: keep its content in view
  const wrap = React.useRef<HTMLDivElement>(null);
  const [width, setWidth] = React.useState(0);
  React.useEffect(() => {
    const el = wrap.current;
    if (!el) return;
    const ro = new ResizeObserver(() => setWidth(el.clientWidth));
    ro.observe(el);
    return () => ro.disconnect();
  }, [rows.length]);
  if (!rows.length) return null;
  const byName = new Map(contexts.map((c) => [c.name, c]));
  const cols = 9 + state.length;
  return (
    <div className="mt-4">
      <div className="mb-1.5 flex flex-wrap items-baseline justify-between gap-2">
        <div className="text-[12px] font-semibold uppercase tracking-[0.1em] text-subtle">decision trail</div>
        <div className="text-[11.5px] text-muted">{entity === "customer" ? "one Kafka key per customer: each event is scored with the state the earlier ones built · " : ""}click a row to see the decision in place</div>
      </div>
      <div ref={wrap} className="max-h-[560px] overflow-auto rounded-xl border border-border">
        <table className="w-full min-w-[1180px] text-[12px]">
          <thead className="sticky top-0 z-10 bg-surface-2 text-left text-[10.5px] uppercase tracking-wider text-subtle">
            <tr><th className="w-6 px-2 py-2" /><th className="px-2 py-2 font-medium">Time</th><th className="px-2 py-2 font-medium">{entity === "customer" ? "Card" : "Customer"}</th>
              <th className="px-2 py-2 text-right font-medium">Amount</th><th className="px-2 py-2 font-medium">Truth</th><th className="px-2 py-2 font-medium">Score</th>
              <th className="px-2 py-2 font-medium">Outcome</th><th className="px-2 py-2 font-medium">Rules</th>
              {state.map((f) => <th key={f.key} className="px-2 py-2 text-right font-medium" title={f.key}>{f.label}</th>)}
              <th className="px-2 py-2 font-medium">What it saw</th></tr>
          </thead>
          <tbody>
            {rows.map((r) => {
              const isOpen = open === r.event_id;
              const other = entity === "customer" ? ["card", r.card_id] as const : ["customer", r.customer_id] as const;
              return (
                <React.Fragment key={r.event_id}>
                  <tr onClick={() => setOpen(isOpen ? null : r.event_id)} data-testid="trail-row"
                    className={cn("cursor-pointer border-t border-border align-top hover:bg-surface-2/60", r.is_fraud && "bg-fail/[0.05]", isOpen && "bg-accent/8")}>
                    <td className="px-2 py-1.5 text-subtle">{isOpen ? <ChevronDown className="size-3.5" /> : <ChevronRight className="size-3.5" />}</td>
                    <td className="whitespace-nowrap px-2 py-1.5 font-mono text-[11px] text-muted">
                      {fmtSimTime(r.event_time_us).slice(5)}
                      <div className="font-sans text-[10.5px] text-subtle">{r.payment_type.toLowerCase().replace(/_/g, " ")} · {r.channel.toLowerCase().replace(/_/g, " ")}</div>
                    </td>
                    <td className="whitespace-nowrap px-2 py-1.5"><IdLink entity={other[0]} id={other[1]} onEntity={onEntity} /></td>
                    <td className="px-2 py-1.5 text-right num">{fmtUsd(r.amount_usd)}</td>
                    <td className="px-2 py-1.5">{r.is_fraud ? <Badge tone="fail" title={r.role ?? undefined}>{r.fraud_type.replace(/_/g, " ")}</Badge> : <span className="text-subtle">genuine</span>}
                      {r.label && <div className="text-[10.5px] text-subtle">{r.label.fraud ? "reported" : "labelled genuine"} after {fmtNum(r.label.after_days, 0)} d</div>}</td>
                    <td className="px-2 py-1.5"><ScoreCell r={r} className="w-24" /></td>
                    <td className="px-2 py-1.5"><div className="flex items-center gap-1"><OutcomeBadge outcome={r.outcome} /><FateNote r={r} /></div></td>
                    <td className="max-w-[200px] truncate px-2 py-1.5 font-mono text-[10.5px]" title={(r.rule_hits ?? []).join(", ")}>{(r.rule_hits ?? []).join(", ") || <span className="text-subtle">—</span>}</td>
                    {state.map((f) => {
                      const v = r.state?.[f.key];
                      return <td key={f.key} className={cn("px-2 py-1.5 text-right num text-[11.5px]", (v === null || v === undefined || v === 0) && "text-subtle")}
                        title={r.features_recorded === false ? "feature vector not recorded for this decision" : v === null ? "n/a: defaulted" : undefined}>
                        {r.features_recorded === false ? "·" : fmtFeature(v, f.format)}</td>;
                    })}
                    <td className="px-2 py-1.5"><ContextChips row={r} ctx={r.context ? byName.get(r.context) : undefined} onEntity={onEntity} /></td>
                  </tr>
                  {isOpen && (
                    <tr className="border-t border-border"><td colSpan={cols} className="p-0"><div className="sticky left-0" style={{ width: width || undefined }}><DecisionInline run={run} eventId={r.event_id} onOpen={() => onPick(r.event_id)} /></div></td></tr>
                  )}
                </React.Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function SummaryTiles({ s, entity }: { s: EntityResponse["summary"]; entity: string }) {
  if (!s.events) return null;
  const span = ((s.last_us ?? 0) - (s.first_us ?? 0)) / 3.6e9;
  const items: [string, React.ReactNode][] = [
    ["events", fmtNum(s.events)], ...(entity !== "card" ? [["cards", fmtNum(s.cards)] as [string, React.ReactNode]] : []),
    ["fraud events", fmtNum(s.fraud_events)], ["approved", fmtNum(s.approve)], ["stepped up", fmtNum(s.challenge)],
    ["reviewed", fmtNum(s.review)], ["declined", fmtNum(s.decline)], ["spend", fmtUsd(s.amount_usd)],
    ["fraud $ paid", fmtUsd(s.fraud_usd_paid)], ["good stopped", fmtNum(s.good_stopped)],
    ["over", span < 48 ? `${fmtNum(span, 1)} h` : `${fmtNum(span / 24, 1)} days`],
  ];
  return (
    <div className="mt-4 grid grid-cols-3 gap-x-4 gap-y-2 rounded-xl border border-border bg-surface-2/40 p-3 sm:grid-cols-6 xl:grid-cols-11">
      {items.map(([k, v]) => <div key={k} className="min-w-0"><div className="truncate text-[11px] text-subtle">{k}</div><div className="text-[14px] font-semibold num">{v}</div></div>)}
    </div>
  );
}

/** generator internals a profile need not show */
const HIDDEN_PROFILE = new Set(["popularity", "index", "slot", "activity_per_day"]);

function ProfileLine({ p, onEntity }: { p: Record<string, unknown> | null; onEntity: OpenEntity }) {
  if (!p) return null;
  if (typeof p.error === "string") return <span className="text-subtle">profile unavailable</span>;
  const parts: React.ReactNode[] = [];
  for (const [k, v] of Object.entries(p)) {
    if (k.endsWith("_id") || HIDDEN_PROFILE.has(k) || v === null || v === undefined || v === "" || Array.isArray(v)) continue;
    if (k === "owner") { parts.push(<span key={k}>owner <IdLink entity="customer" id={String(v)} onEntity={onEntity} full /></span>); continue; }
    if (k === "merchant") { parts.push(<span key={k}>merchant <IdLink entity="merchant" id={String(v)} onEntity={onEntity} /></span>); continue; }
    if (k === "note") { parts.push(<span key={k} className="text-warn">{String(v)}</span>); continue; }
    const val = typeof v === "number" ? (k === "mcc" ? String(v) : k.includes("usd") ? fmtUsd(v) : k.includes("rate") ? fmtPct(v, 2) : fmtNum(v, Number.isInteger(v) ? 0 : 1)) : typeof v === "boolean" ? (v ? "yes" : "no") : String(v);
    parts.push(<span key={k}>{k.replace(/_/g, " ")} <span className="text-fg">{val}</span></span>);
  }
  const lists: [string, string, unknown][] = [["cards", "card", p.cards], ["devices", "device", p.devices], ["regular payees", "payee", p.payees], ["accounts", "account", p.accounts]];
  return (
    <div className="space-y-1">
      <div className="flex flex-wrap gap-x-3 gap-y-0.5">{parts}</div>
      {lists.filter(([, , v]) => Array.isArray(v) && v.length).map(([label, ent, v]) => (
        <div key={label} className="flex flex-wrap items-center gap-x-2">
          <span>{label}</span>
          {(v as (string | { id: string; type?: string })[]).map((x) => {
            const id = typeof x === "string" ? x : x.id;
            return <span key={id} className="inline-flex items-center gap-1"><IdLink entity={ent} id={id} onEntity={onEntity} />{typeof x !== "string" && x.type ? <span className="text-subtle">{x.type.toLowerCase()}</span> : null}</span>;
          })}
        </div>
      ))}
    </div>
  );
}

function RelatedPivots({ related, onEntity }: { related: EntityResponse["related"]; onEntity: OpenEntity }) {
  const entries = Object.entries(related).filter(([, r]) => r.distinct > 0);
  if (!entries.length) return null;
  return (
    <div className="mt-4 grid grid-cols-1 gap-2 sm:grid-cols-2 xl:grid-cols-4">
      {entries.map(([ent, r]) => (
        <div key={ent} className="min-w-0 rounded-xl border border-border px-3 py-2">
          <div className="flex items-baseline justify-between gap-2"><span className="text-[11px] uppercase tracking-wider text-subtle">{ent === "ip" ? "IPs" : `${ent}s`}</span><span className="text-[13px] font-semibold num">{fmtNum(r.distinct)}</span></div>
          <div className="mt-1 space-y-0.5">{r.top.slice(0, 3).map((t) => (
            <div key={t.id} className="flex items-center gap-2 text-[11.5px]">
              <IdLink entity={ent} id={ent === "ip" ? t.display : t.id} onEntity={onEntity} />
              <span className="ml-auto text-subtle num">{fmtNum(t.events)} ev{t.fraud ? <span className="text-fail"> · {fmtNum(t.fraud)} fraud</span> : null}</span>
            </div>
          ))}</div>
        </div>
      ))}
    </div>
  );
}

function CommonPointCard({ cp }: { cp: NonNullable<EntityResponse["common_point"]> }) {
  if (cp.error || !cp.cards_seen) return null;
  const hot = isNum(cp.lift) && cp.lift >= 3 && (cp.cards_later_fraud ?? 0) >= 3;
  return (
    <div className={cn("mt-4 rounded-xl border p-3 text-[12.5px]", hot ? "border-fail/40 bg-fail/[0.05]" : "border-border")}>
      <div className="flex flex-wrap items-baseline gap-x-4 gap-y-1">
        <span className="font-semibold">Common point of purchase</span>
        <span className="text-muted">{fmtNum(cp.cards_seen)} cards used here · <span className={cn(hot && "font-semibold text-fail")}>{fmtNum(cp.cards_later_fraud)} later used fraudulently</span>
          {" "}({fmtPct(cp.share, 1)} vs {fmtPct(cp.baseline_share, 1)} of all active cards{isNum(cp.lift) ? `, ${fmtNum(cp.lift, 1)}× the base rate` : ""})</span>
      </div>
      {cp.by_type && Object.keys(cp.by_type).length > 0 && (
        <div className="mt-1.5 flex flex-wrap gap-1.5">{Object.entries(cp.by_type).map(([t, n]) => <Badge key={t} tone="fail">{t.replace(/_/g, " ")} · {fmtNum(n)}</Badge>)}</div>
      )}
      {cp.note && <div className="mt-1.5 text-[11.5px] text-subtle">{cp.note}</div>}
    </div>
  );
}

const PAGE = 500;

function EntityPanel({ run, entity, id, onPick, onEntity, onClose }: { run: string; entity: string; id: string; onPick: (id: string) => void; onEntity: OpenEntity; onClose: () => void }) {
  const [offset, setOffset] = React.useState(0);
  React.useEffect(() => setOffset(0), [entity, id]);
  const q = useQuery({ queryKey: ["entity", run, entity, id, offset], queryFn: () => api.entity(run, entity, id, offset, PAGE), retry: 0, placeholderData: (p) => p });
  const d = q.data;
  const rows = d?.events ?? [];
  const pts = rows.map((r) => ({ t: r.event_time_us / 1e6, amount: r.amount_usd, outcome: r.outcome ?? "none", id: r.event_id, fraud: r.is_fraud }));
  return (
    <Card className="mb-4" data-testid="entity-panel">
      <CardHeader icon={entity === "merchant" || entity === "terminal" ? <Store /> : entity === "device" || entity === "ip" ? <Network /> : <Layers />}
        title={<span>{d?.title ?? titleCase(entity)} <span className="font-mono">{d?.display ?? id}</span></span>}
        subtitle={d ? <ProfileLine p={d.profile} onEntity={onEntity} /> : "loading…"}
        actions={<Button variant="ghost" size="icon-sm" onClick={onClose} aria-label="Close entity"><X /></Button>} />
      <CardBody>
        {q.isLoading ? <Skeleton className="h-40" /> : q.error ? <ErrorState error={q.error} /> : !d ? null : (
          <>
            {d.note && <div className="mb-3 text-[12.5px] text-muted">{d.note}</div>}
            {rows.length > 0 && (
              <>
                <ResponsiveContainer width="100%" height={170}>
                  <ScatterChart margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
                    <CartesianGrid stroke="var(--grid)" />
                    <XAxis dataKey="t" type="number" domain={["dataMin", "dataMax"]} tickLine={false} axisLine={false} tick={{ fill: "var(--fg-subtle)", fontSize: 11 }}
                      tickFormatter={(v) => new Date(v * 1000).toISOString().slice(5, 16).replace("T", " ")} minTickGap={60} />
                    <YAxis dataKey="amount" tickLine={false} axisLine={false} width={56} tick={{ fill: "var(--fg-subtle)", fontSize: 11 }} tickFormatter={(v) => fmtUsd(v)} />
                    <ZAxis range={[50, 50]} />
                    <Tooltip isAnimationActive={false} content={({ active, payload }) => {
                      const x = active && payload?.[0]?.payload as (typeof pts)[number] | undefined;
                      return x ? <div className="rounded-xl border border-border bg-surface px-3 py-2 text-[12px] shadow-xl"><div className="font-mono">{x.id}</div><div className="text-muted">{fmtUsd(x.amount)} · {x.outcome.toLowerCase()}{x.fraud ? " · fraud" : ""}</div></div> : null;
                    }} />
                    {["APPROVE", "CHALLENGE", "REVIEW", "DECLINE", "none"].map((o) => (
                      <Scatter key={o} data={pts.filter((p2) => p2.outcome === o)} fill={OUTCOME_COLOR[o] ?? "var(--fg-subtle)"} isAnimationActive={false}
                        onClick={(dd) => onPick((dd as unknown as { id: string }).id)} className="cursor-pointer" />
                    ))}
                  </ScatterChart>
                </ResponsiveContainer>
                <div className="mt-1 flex flex-wrap gap-4 text-[11px] text-subtle">
                  {["APPROVE", "CHALLENGE", "REVIEW", "DECLINE"].map((o) => <span key={o} className="inline-flex items-center gap-1.5"><span className="size-2 rounded-full" style={{ background: OUTCOME_COLOR[o] }} />{o.toLowerCase()}</span>)}
                  {d.summary.payment_types && <span>{Object.entries(d.summary.payment_types).map(([k, v]) => `${fmtNum(v)} ${k.toLowerCase().replace(/_/g, " ")}`).join(" · ")}</span>}
                </div>
              </>
            )}
            <SummaryTiles s={d.summary} entity={entity} />
            {d.common_point && <CommonPointCard cp={d.common_point} />}
            <RelatedPivots related={d.related} onEntity={onEntity} />
            <DecisionTrail run={run} rows={rows} state={d.columns.state} contexts={d.columns.contexts} onPick={onPick} onEntity={onEntity} entity={entity} />
            {(d.total > PAGE || offset > 0) && (
              <div className="mt-2 flex flex-wrap items-center gap-2 text-[12px] text-muted">
                <span>events {fmtNum(offset + 1)}–{fmtNum(Math.min(offset + PAGE, d.total))} of {fmtNum(d.total)}</span>
                <Button size="sm" variant="secondary" disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - PAGE))}>Earlier</Button>
                <Button size="sm" variant="secondary" disabled={!d.truncated} onClick={() => setOffset(offset + PAGE)}>Later</Button>
              </div>
            )}
            {d.features_note && <div className="mt-2 text-[11.5px] text-subtle">{d.features_note}</div>}
          </>
        )}
      </CardBody>
    </Card>
  );
}

const SHOW_OPTS = [{ value: "all", label: "all" }, { value: "fraud", label: "with fraud" }, { value: "mixed", label: "genuine + fraud" },
  { value: "shared", label: "shared" }, { value: "flagged", label: "flagged" }];
const SORT_OPTS = [{ value: "events", label: "events" }, { value: "fraud", label: "fraud" }, { value: "shared", label: "shared" },
  { value: "flagged", label: "flagged" }, { value: "max_score", label: "max score" }, { value: "amount", label: "amount" }, { value: "recent", label: "recent" }];

/** Entities of the run (customers, cards, devices, payees, …), to find the ones worth reading. */
function Entities({ run, views, entity, setEntity, show, setShow, sort, setSort, q, onEntity }: {
  run: string; views: ExplorerViews | undefined; entity: string; setEntity: (e: string) => void; show: string; setShow: (s: string) => void;
  sort: string; setSort: (s: string) => void; q: string; onEntity: OpenEntity;
}) {
  const def: EntityDef | undefined = views?.entities[entity];
  const res = useQuery({ queryKey: ["entities", run, entity, show, sort, q], queryFn: () => api.entities(run, entity, sort, show, 100, q), retry: 0, enabled: Boolean(def) });
  const st = res.data?.stats;
  const shared = def?.shared_by.map((c) => c.replace(/_id$/, "").replace("ip_v4", "ip")) ?? [];
  const cpp = Boolean(def?.common_point);
  const showOpts = cpp ? [...SHOW_OPTS, { value: "common_point", label: "common point" }] : SHOW_OPTS;
  const sortOpts = cpp ? [...SORT_OPTS, { value: "common_point", label: "common point" }] : SORT_OPTS;
  return (
    <Card className="overflow-hidden">
      <CardHeader icon={<Layers />} title={def ? `${def.title}s` : "Entities"}
        subtitle={st ? `${fmtNum(st.entities)} ${entity}s${q ? ` in events matching “${q}”` : ""} · ${fmtNum(st.with_2_plus ?? 0)} with 2+ events · most ${fmtNum(st.max_events ?? 0)} · ${fmtNum(st.with_fraud ?? 0)} with fraud · ${fmtNum(st.shared ?? 0)} shared by ${st.shared_min ?? 2}+ ${st.shared_by ?? ""}s` : "per-entity decisions"} />
      <div className="flex flex-wrap items-center gap-2 px-5 pb-3">
        <div className="max-w-full overflow-x-auto"><Segmented size="sm" value={entity} onChange={setEntity}
          options={Object.values(views?.entities ?? {}).map((e) => ({ value: e.name, label: e.title }))} /></div>
        <div className="max-w-full overflow-x-auto"><Segmented size="sm" value={show} onChange={setShow} options={showOpts} /></div>
        <span className="text-[11.5px] text-subtle">sort</span>
        <div className="max-w-full overflow-x-auto"><Segmented size="sm" value={sort} onChange={setSort} options={sortOpts} /></div>
      </div>
      {res.isLoading || !views ? <div className="space-y-2 px-5 pb-5">{Array.from({ length: 8 }, (_, i) => <Skeleton key={i} className="h-8" />)}</div>
        : res.error ? <div className="px-5 pb-5">{isNotFound(res.error) ? <Unavailable what="Entities" /> : <ErrorState error={res.error} />}</div>
          : !res.data?.rows.length ? <div className="px-5 pb-5"><EmptyState icon={<Layers />} title={`No ${entity}s match`}>Try another filter or lens.</EmptyState></div> : (
            <div className="max-h-[640px] overflow-auto">
              <table className="w-full min-w-[960px] text-[12.5px]" data-testid="entities-table">
                <thead className="sticky top-0 z-10 border-y border-border bg-surface-2 text-left text-[11px] uppercase tracking-wider text-subtle">
                  <tr><th className="px-5 py-2 font-medium">{def?.title}</th><th className="px-3 py-2 text-right font-medium">Events</th>
                    {shared.map((c) => <th key={c} className="px-3 py-2 text-right font-medium">{c === "ip" ? "IPs" : `${c}s`}</th>)}
                    {cpp && <th className="px-3 py-2 text-right font-medium" title="cards used here that later had fraud elsewhere">later-fraud cards</th>}
                    <th className="px-3 py-2 font-medium">Fraud</th><th className="px-3 py-2 text-right font-medium">Flagged</th><th className="px-3 py-2 text-right font-medium">Declined</th>
                    <th className="px-3 py-2 text-right font-medium">Stepped up</th><th className="px-3 py-2 font-medium">Max score</th><th className="px-3 py-2 text-right font-medium">Amount</th>
                    <th className="px-5 py-2 text-right font-medium">Active</th></tr>
                </thead>
                <tbody>
                  {res.data.rows.map((c: EntityRow) => {
                    const hours = (c.last_us - c.first_us) / 3.6e9;
                    return (
                      <tr key={c.id} onClick={() => onEntity(entity, entity === "ip" ? c.display : c.id)} className="cursor-pointer border-b border-border last:border-0 hover:bg-surface-2/60">
                        <td className="px-5 py-2 font-mono text-[12px] text-accent">{c.display}</td>
                        <td className="px-3 py-2 text-right num font-medium">{fmtNum(c.events)}</td>
                        {shared.map((k) => <td key={k} className={cn("px-3 py-2 text-right num", (c.distinct[k] ?? 0) >= (def?.shared_min ?? 2) && "font-semibold text-warn")}>{fmtNum(c.distinct[k] ?? 0)}</td>)}
                        {cpp && <td className="px-3 py-2 text-right num">{c.common_point ? <span className={cn(isNum(c.common_point.lift) && c.common_point.lift >= 3 && "font-semibold text-fail")}>{fmtNum(c.common_point.cards_later_fraud)} / {fmtNum(c.common_point.cards_seen)}{isNum(c.common_point.lift) ? ` · ${fmtNum(c.common_point.lift, 1)}×` : ""}</span> : "—"}</td>}
                        <td className="px-3 py-2">{c.fraud_events ? <span className="inline-flex flex-wrap items-center gap-1"><span className="num">{fmtNum(c.fraud_events)}</span>{c.fraud_types.map((t) => <Badge key={t} tone="fail">{t.replace(/_/g, " ")}</Badge>)}</span> : <span className="text-subtle">—</span>}</td>
                        <td className="px-3 py-2 text-right num">{fmtNum(c.flagged)}</td>
                        <td className="px-3 py-2 text-right num">{fmtNum(c.declined)}</td>
                        <td className="px-3 py-2 text-right num">{fmtNum(c.challenged)}</td>
                        <td className="px-3 py-2"><ScoreBar score={c.max_score} className="w-24" /></td>
                        <td className="px-3 py-2 text-right num">{fmtUsd(c.amount_usd)}</td>
                        <td className="px-5 py-2 text-right num text-muted">{hours < 48 ? `${fmtNum(hours, 1)} h` : `${fmtNum(hours / 24, 1)} d`}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
      {st?.note && <div className="border-t border-border px-5 py-2.5 text-[11.5px] text-subtle">{st.note}</div>}
    </Card>
  );
}

function Campaigns({ run, onPick, onEntity, sel, setSel }: { run: string; onPick: (id: string) => void; onEntity: OpenEntity; sel: string | null; setSel: (c: string | null) => void }) {
  const q = useQuery({ queryKey: ["campaigns", run], queryFn: () => api.campaigns(run), retry: 0 });
  const ev = useQuery({ queryKey: ["campaign", run, sel], queryFn: () => api.campaign(run, sel!), enabled: Boolean(sel) });
  if (q.isLoading) return <Skeleton className="h-64" />;
  if (q.error) return isNotFound(q.error) ? <Unavailable what="Campaigns" /> : <ErrorState error={q.error} />;
  const cs = q.data?.campaigns ?? [];
  if (!cs.length) return <EmptyState icon={<Flag />} title="No fraud campaigns in this run">Pick a fraud pack (e.g. mixed-realistic) to see campaigns.</EmptyState>;
  const firstStop = (ev.data?.events ?? []).findIndex((e) => e.is_fraud && e.fate !== null && ["declined", "held for review", "abandoned at step-up"].includes(e.fate));
  return (
    <div className="grid grid-cols-1 gap-4 xl:grid-cols-[minmax(0,1.3fr)_minmax(0,1fr)]">
      <Card className="overflow-hidden">
        <div className="max-h-[620px] overflow-auto">
          <table className="w-full min-w-[820px] text-[12.5px]">
            <thead className="sticky top-0 z-10 border-b border-border bg-surface-2 text-left text-[11px] uppercase tracking-wider text-subtle">
              <tr><th className="px-5 py-2 font-medium">Campaign</th><th className="px-3 py-2 font-medium">Typology</th><th className="px-3 py-2 text-right font-medium">Events</th>
                <th className="px-3 py-2 text-right font-medium">Stepped up</th><th className="px-3 py-2 text-right font-medium">Declined</th><th className="px-3 py-2 text-right font-medium">Stopped</th>
                <th className="px-3 py-2 text-right font-medium" title="fraud events paid out before the first one was stopped">Paid first</th><th className="px-5 py-2 text-right font-medium">$ paid / total</th></tr>
            </thead>
            <tbody>
              {cs.map((c) => (
                <tr key={c.campaign_id} onClick={() => setSel(c.campaign_id)} className={cn("cursor-pointer border-b border-border last:border-0 hover:bg-surface-2/60", sel === c.campaign_id && "bg-accent/8")}>
                  <td className="px-5 py-2 font-mono">{c.campaign_id}</td>
                  <td className="px-3 py-2">{titleCase(c.fraud_type.replace(/_/g, " "))}{c.customers && c.customers > 1 ? <span className="text-subtle"> · {fmtNum(c.customers)} customers</span> : null}</td>
                  <td className="px-3 py-2 text-right num">{fmtNum(c.events)}{c.story_events ? <span className="text-subtle"> +{fmtNum(c.story_events)}</span> : null}</td>
                  <td className="px-3 py-2 text-right num">{fmtNum(c.challenged ?? 0)}</td>
                  <td className="px-3 py-2 text-right num">{fmtNum(c.declined)}</td>
                  <td className="px-3 py-2 text-right num">{fmtPct((c.stopped ?? c.declined) / Math.max(1, c.events), 0)}</td>
                  <td className="px-3 py-2 text-right num">{fmtNum(c.approved_before_first_stop ?? c.approved_before_first_decline)}</td>
                  <td className="px-5 py-2 text-right num">{fmtUsd(c.usd_paid ?? c.usd_approved)} <span className="text-subtle">/ {fmtUsd(c.usd_total)}</span></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>
      <Card>
        <CardHeader title={sel ? `Campaign ${sel}` : "Campaign timeline"} subtitle={sel ? "events in order with each one's role; genuine story events are marked; the first stop is where the campaign was halted" : "select a campaign"}
          actions={sel ? <Button variant="ghost" size="icon-sm" onClick={() => setSel(null)} aria-label="Close campaign"><X /></Button> : undefined} />
        <CardBody>
          {!sel ? <div className="py-10 text-center text-[13px] text-subtle">Select a campaign to see its events.</div> : ev.isLoading ? <Skeleton className="h-48" /> : (
            <ol className="relative max-h-[520px] space-y-0 overflow-y-auto border-l border-border pl-4">
              {(ev.data?.events ?? []).map((e, i) => (
                <li key={e.event_id} className={cn("relative py-1.5", i === firstStop && "rounded-lg bg-pass/[0.06]")}>
                  <span className="absolute -left-[21px] top-3 size-2.5 rounded-full border-2 border-surface" style={{ background: OUTCOME_COLOR[e.outcome ?? ""] ?? "var(--fg-subtle)" }} />
                  <button onClick={() => onPick(e.event_id)} className="flex w-full flex-wrap items-center gap-x-3 gap-y-0.5 rounded-lg px-2 py-1 text-left text-[12.5px] hover:bg-surface-2 cursor-pointer">
                    <span className="font-mono text-[11px] text-subtle">{fmtSimTime(e.event_time_us).slice(5)}</span>
                    <span className="num">{fmtUsd(e.amount_usd)}</span>
                    <span className="truncate text-muted">{(e.role ?? e.payment_type).toLowerCase().replace(/_/g, " ")}</span>
                    {!e.is_fraud && <Badge tone="neutral">genuine story event</Badge>}
                    {i === firstStop && <Badge tone="pass">stopped here</Badge>}
                    <span className="ml-auto flex items-center gap-1.5"><OutcomeBadge outcome={e.outcome} /><FateNote r={e} /></span>
                  </button>
                  <div className="pl-2 text-[11px] text-subtle">customer <IdLink entity="customer" id={e.customer_id} onEntity={onEntity} />{e.payee_id && <> · payee <IdLink entity="payee" id={e.payee_id} onEntity={onEntity} /></>}{e.device_id && <> · device <IdLink entity="device" id={e.device_id} onEntity={onEntity} /></>}</div>
                </li>
              ))}
            </ol>
          )}
        </CardBody>
      </Card>
    </div>
  );
}

/** Use-case presets: one click sets the search, the entity list and its filter. */
function LensBar({ lenses, active, onLens }: { lenses: Lens[]; active: string | null; onLens: (l: Lens | null) => void }) {
  if (!lenses.length) return null;
  return (
    <div className="mb-4 flex max-w-full gap-1.5 overflow-x-auto pb-1" data-testid="lens-bar">
      {lenses.map((l) => (
        <button key={l.name} type="button" title={l.description} onClick={() => onLens(active === l.name ? null : l)}
          className={cn("shrink-0 rounded-full border px-3 py-1 text-[12px] font-medium transition-colors cursor-pointer",
            active === l.name ? "border-accent bg-accent/10 text-fg" : "border-border text-muted hover:border-accent/50 hover:text-fg")}>{l.title}</button>
      ))}
    </div>
  );
}

export default function Explorer() {
  const [run, setRun] = useParam("run");
  const [event, setEvent] = useParam("event");
  const [ent, setEnt] = useParam("entity");
  const [legacyCustomer] = useParam("customer");
  const [tab, setTab] = useParam("tab");
  const [qp, setQp] = useParam("q");
  const [lens, setLens] = useParam("lens");
  const [list, setList] = useParam("list");
  const [show, setShow] = useParam("show");
  const [sort, setSort] = useParam("sort");
  const [camp, setCamp] = useParam("campaign");
  const [text, setText] = React.useState(qp ?? "");
  React.useEffect(() => setText(qp ?? ""), [qp]);
  const query = qp ?? "";
  const runs = useQuery({ queryKey: ["runs"], queryFn: () => api.runs(500) });
  const views = useQuery({ queryKey: ["explorer-views"], queryFn: api.explorerViews, staleTime: 60_000 });
  const typologies = useQuery({ queryKey: ["usecases"], queryFn: api.usecases, staleTime: 60_000 });
  const done = (runs.data?.history ?? []).filter((r) => r.status === "done");
  const runId = run || done[0]?.run_id || "";
  const search = useQuery({ queryKey: ["search", runId, query], queryFn: () => api.search(runId, query, "auto", 100), enabled: Boolean(runId), retry: 0 });

  const opened = ent ?? (legacyCustomer ? `customer:${legacyCustomer}` : null);
  const [openType, openId] = opened ? [opened.slice(0, opened.indexOf(":")), opened.slice(opened.indexOf(":") + 1)] : [null, null];
  const onEntity: OpenEntity = (e, id) => { setEnt(`${e}:${id}`, { customer: null, tab: null }); window.scrollTo({ top: 0, behavior: "smooth" }); };
  const pick = (id: string) => setEvent(id, { tab: null });
  const submit = (v: string) => { setQp(v || null, { lens: null }); setTab(null); };
  const staticLenses = views.data?.lenses ?? [];
  const lenses: Lens[] = [
    ...staticLenses,
    ...(typologies.data?.typologies ?? []).filter((t) => !t.disabled && t.pattern !== "label_only" && !staticLenses.some((l) => l.search === `fraud:${t.name}`))
      .map((t) => ({ name: `t:${t.name}`, title: t.title || titleCase(t.name.replace(/_/g, " ")), search: `fraud:${t.name}`, entity: "customer", show: "fraud", description: t.description })),
  ];
  const applyLens = (l: Lens | null) => {
    if (!l) { setLens(null, { q: null }); return; }
    setLens(l.name, { q: l.search || null, list: l.entity ?? null, show: l.show ?? null, sort: l.sort ?? null, tab: l.entity ? "entities" : null, entity: null, customer: null, event: null });
  };
  const listEntity = list && views.data?.entities[list] ? list : "customer";
  const matched = search.data?.matched ?? {};
  return (
    <>
      <PageHeader eyebrow="Explorer" title="Event & decision explorer"
        subtitle="Look up any event, customer, card, device, IP, payee, merchant or campaign of a run: the transaction, the features it was scored on, the rules that fired, where its latency went, and every entity's decision trail." />
      <div className="mb-3 flex flex-wrap items-center gap-2">
        <Select className="w-full sm:w-[360px]" value={runId} onChange={(v) => setRun(v, { event: null, entity: null, customer: null, campaign: null })} ariaLabel="Run"
          options={done.map((r) => ({ value: r.run_id, label: <span className="font-mono text-[12.5px]">{r.run_id}</span>, hint: `${r.verdict ?? ""} · ${fmtNum(r.decided)} decisions` }))}
          placeholder={runs.isLoading ? "Loading runs…" : "Pick a run"} />
        <form className="relative min-w-[240px] flex-1" onSubmit={(e) => { e.preventDefault(); submit(text.trim()); }}>
          <Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-subtle" />
          <Input value={text} onChange={(e) => setText(e.target.value)} className="pl-9" data-testid="explorer-search"
            placeholder="id, customer:123, payee:…, ip:10.1.2.3, channel:ECOM,IN_APP fraud:account_takeover, rule:CARD_TESTING" />
        </form>
        <Button variant="secondary" onClick={() => submit(text.trim())}><ScanSearch />Search</Button>
      </div>
      <LensBar lenses={lenses} active={lens} onLens={applyLens} />
      {!runId ? <EmptyState icon={<ScanSearch />} title="No completed runs">Run a scenario first; its events and decisions become explorable here.</EmptyState> : (
        <>
          {openType && openId && <EntityPanel run={runId} entity={openType} id={openId} onPick={pick} onEntity={onEntity} onClose={() => setEnt(null, { customer: null })} />}
          {event && <div className="mb-4"><DecisionPanel run={runId} eventId={event} onClose={() => setEvent(null)} onEntity={onEntity} /></div>}
          <Tabs value={tab || "search"} onValueChange={(t) => setTab(t === "search" ? null : t)}>
            <TabsList><TabsTrigger value="search">Events</TabsTrigger><TabsTrigger value="entities">Entities</TabsTrigger><TabsTrigger value="campaigns">Campaigns</TabsTrigger></TabsList>
            <TabsContent value="search" className="mt-4">
              <Card className="min-w-0 self-start overflow-hidden">
                <CardHeader title={query ? `Results for “${query}”` : "Recent and fraud events"}
                  subtitle={search.data ? `${fmtNum(search.data.results.length)} of ${fmtNum(search.data.total)} events${Object.keys(matched).length > 1 ? ` · the id matched as ${Object.entries(matched).map(([k, n]) => `${k} (${fmtNum(n)})`).join(", ")}` : ""}` : undefined} />
                {search.isLoading ? <div className="space-y-2 px-5 pb-5">{Array.from({ length: 8 }, (_, i) => <Skeleton key={i} className="h-8" />)}</div>
                  : search.error ? <div className="px-5 pb-5">{isNotFound(search.error) && !String((search.error as Error).message).includes("run") ? <Unavailable what="Explorer" /> : <ErrorState error={search.error} title="Search failed" />}</div>
                    : !search.data?.results.length ? <div className="px-5 pb-5"><EmptyState icon={<Search />} title="No events match">Try an id from the table, or a prefix like customer:, payee:, device:, ip:, channel:, rule:.</EmptyState></div>
                      : <ResultsTable rows={search.data.results} onPick={pick} selected={event} onEntity={onEntity} />}
              </Card>
            </TabsContent>
            <TabsContent value="entities" className="mt-4">
              <Entities run={runId} views={views.data} entity={listEntity} setEntity={(e) => setList(e, { sort: null, show: null })}
                show={show ?? "all"} setShow={(s) => setShow(s)} sort={sort ?? "events"} setSort={(s) => setSort(s)} q={query} onEntity={onEntity} />
            </TabsContent>
            <TabsContent value="campaigns" className="mt-4">
              <Campaigns run={runId} onPick={pick} onEntity={onEntity} sel={camp} setSel={(c) => setCamp(c)} />
            </TabsContent>
          </Tabs>
        </>
      )}
    </>
  );
}

