import { ExplanationCard, LabelStoryCard } from "@/components/explain";
import { useQuery } from "@tanstack/react-query";
import { CreditCard, Fingerprint, Flag, ScanSearch, Search, ShieldAlert, Timer, User, Users, X } from "lucide-react";
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
import { fmtMs, fmtNum, fmtPct, fmtSimTime, fmtUsd, isNum, titleCase } from "@/lib/format";
import type { CustomerRow, DecisionDetail, SearchRow, TrailRow } from "@/lib/types";

const OUTCOME_COLOR: Record<string, string> = { APPROVE: "var(--pass)", CHALLENGE: "var(--info)", REVIEW: "var(--warn)", DECLINE: "var(--fail)" };

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

function ResultsTable({ rows, onPick, selected, onCustomer }: { rows: SearchRow[]; onPick: (id: string) => void; selected: string | null; onCustomer: (id: string) => void }) {
  return (
    <div className={cn("overflow-auto", selected ? "max-h-[340px]" : "max-h-[640px]")}>
      <table className="w-full min-w-[880px] text-[12.5px]">
        <thead className="sticky top-0 z-10 border-y border-border bg-surface-2 text-left text-[11px] uppercase tracking-wider text-subtle">
          <tr><th className="px-5 py-2 font-medium">Event time</th><th className="px-3 py-2 font-medium">Event</th><th className="px-3 py-2 font-medium">Customer</th>
            <th className="px-3 py-2 text-right font-medium">Amount</th><th className="px-3 py-2 font-medium">Type</th><th className="px-3 py-2 font-medium">Outcome</th>
            <th className="px-3 py-2 font-medium">Score</th><th className="px-3 py-2 font-medium">Label</th><th className="px-5 py-2 text-right font-medium">e2e</th></tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.event_id} onClick={() => onPick(r.event_id)} data-testid="search-row"
              className={cn("cursor-pointer border-b border-border last:border-0 hover:bg-surface-2/60", selected === r.event_id && "bg-accent/8")}>
              <td className="whitespace-nowrap px-5 py-2 font-mono text-[11.5px] text-muted">{fmtSimTime(r.event_time_us)}</td>
              <td className="px-3 py-2 font-mono text-[11.5px]">{r.event_id}</td>
              <td className="px-3 py-2"><button onClick={(e) => { e.stopPropagation(); onCustomer(r.customer_id); }} className="font-mono text-[11.5px] text-accent hover:underline cursor-pointer">{r.customer_id}</button></td>
              <td className="px-3 py-2 text-right num">{fmtUsd(r.amount_usd)}</td>
              <td className="px-3 py-2 text-muted">{titleCase(r.payment_type.toLowerCase())} · {r.channel.toLowerCase()} · {r.merchant_country}</td>
              <td className="px-3 py-2"><OutcomeBadge outcome={r.outcome} /></td>
              <td className="px-3 py-2"><ScoreBar score={r.score} /></td>
              <td className="px-3 py-2">{r.is_fraud ? <Badge tone="fail"><ShieldAlert />{r.fraud_type}</Badge> : <span className="text-subtle">legit</span>}</td>
              <td className="px-5 py-2 text-right num text-muted">{fmtMs(r.e2e_ms)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function DecisionPanel({ run, eventId, onClose, onCustomer }: { run: string; eventId: string; onClose: () => void; onCustomer: (c: string) => void }) {
  const [maturity, setMaturity] = React.useState(30);
  const q = useQuery({ queryKey: ["decision", run, eventId, maturity], queryFn: () => api.decision(run, eventId, maturity), retry: 0,
    placeholderData: (prev) => prev });
  const top = React.useRef<HTMLDivElement>(null);
  React.useEffect(() => { if (q.data) top.current?.scrollIntoView({ behavior: "smooth", block: "start" }); }, [q.data, eventId]);
  if (q.isLoading) return <Card className="p-5"><Skeleton className="h-6 w-48" /><Skeleton className="mt-4 h-64" /></Card>;
  if (q.error) return <Card className="p-5"><ErrorState error={q.error} title="Decision not found" /></Card>;
  const d = q.data as DecisionDetail;
  const ev = d.event;
  const groups = new Map<string, NonNullable<DecisionDetail["features"]>>();
  for (const f of d.features ?? []) {
    const e = f.entity || "derived";
    if (!groups.has(e)) groups.set(e, []);
    groups.get(e)!.push(f);
  }
  const missing = (d.features ?? []).filter((f) => f.missing).length;
  const fields: [string, unknown][] = [
    ["amount", fmtUsd(ev.amount_usd as number)], ["payment type", ev.payment_type], ["channel", ev.channel], ["entry mode", ev.entry_mode],
    ["card type", ev.card_type], ["merchant country", ev.merchant_country], ["MCC", ev.mcc], ["card", ev.card_id], ["account", ev.account_id],
    ["merchant", ev.merchant_id], ["terminal", ev.terminal_id], ["device", ev.device_id], ["payee", ev.payee_id], ["IP", ev.ip],
    ["event time", fmtSimTime(ev.event_time_us as number)],
  ];
  return (
    <div className="scroll-mt-20 space-y-4" data-testid="decision-detail" ref={top}>
      <Card className="overflow-hidden">
        <div className="flex flex-wrap items-start gap-4 p-5">
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <OutcomeBadge outcome={d.decision?.outcome} />
              {d.decision?.degraded && <Badge tone="warn">degraded</Badge>}
              {d.decision && <Badge className="font-mono">{d.decision.path}</Badge>}
              {d.label.is_fraud ? <Badge tone="fail"><ShieldAlert />fraud · {d.label.fraud_type}</Badge> : <Badge tone="pass">legit</Badge>}
            </div>
            <h2 className="mt-2 font-mono text-[17px] font-semibold">event {eventId}</h2>
            <div className="mt-1 flex flex-wrap gap-x-4 text-[12.5px] text-muted">
              <button onClick={() => onCustomer(String(ev.customer_id))} className="inline-flex items-center gap-1 text-accent hover:underline cursor-pointer"><User className="size-3.5" />customer {String(ev.customer_id)}</button>
              {d.label.campaign_id !== "0" && <span className="inline-flex items-center gap-1"><Flag className="size-3.5" />campaign {d.label.campaign_id}</span>}
              {d.decision && <span>model {d.decision.model_version || "—"}</span>}
              {d.decision?.revisions && d.decision.revisions > 1 && <span>{d.decision.revisions} revisions</span>}
            </div>
          </div>
          <div className="text-right">
            <div className="text-[11px] uppercase tracking-wider text-subtle">score</div>
            <div className="text-[28px] font-semibold leading-none num">{isNum(d.decision?.score) ? d.decision!.score.toFixed(3) : "—"}</div>
            <div className="mt-1 text-[12px] text-muted num">e2e {fmtMs(d.timing.e2e_ms)}</div>
          </div>
          <Button variant="ghost" size="icon-sm" aria-label="Close" onClick={onClose}><X /></Button>
        </div>
        <div className="grid grid-cols-1 border-t border-border md:grid-cols-2">
          <div className="border-border p-5 md:border-r">
            <div className="mb-2 flex items-center gap-2 text-[12px] font-semibold uppercase tracking-[0.1em] text-subtle"><CreditCard className="size-3.5" />Transaction</div>
            {fields.filter(([, v]) => v !== undefined && v !== null && v !== "" && v !== "0").map(([k, v]) => <Stat key={k} label={k} value={String(v)} mono={/card|account|merchant|terminal|device|payee|IP/.test(k)} />)}
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
        <CardHeader title={`Features (${d.features?.length ?? 0})`} subtitle={d.features ? `${missing} missing (defaulted) · grouped by entity` : "this decision did not carry its feature vector (feature_sample_permille)"} />
        {d.features && (
          <CardBody className="grid grid-cols-1 gap-x-6 gap-y-5 md:grid-cols-2 xl:grid-cols-3">
            {[...groups.entries()].map(([entity, fs]) => (
              <div key={entity}>
                <div className="mb-1.5 text-[11px] font-semibold uppercase tracking-[0.12em] text-subtle">{entity}</div>
                {fs.map((f) => (
                  <div key={f.name} className="flex items-baseline gap-2 border-b border-border py-1 text-[12px] last:border-0" title={`${f.source}${f.agg ? ` · ${f.agg}` : ""} · default ${f.default}`}>
                    <span className={cn("truncate font-mono text-[11.5px]", f.missing ? "text-warn" : "text-muted")}>{f.name}</span>
                    {f.missing && <Badge tone="warn" className="px-1.5 py-0 text-[10px]">missing</Badge>}
                    <span className="ml-auto font-mono text-[11.5px] text-fg num">{Number.isInteger(f.value) ? fmtNum(f.value) : f.value.toFixed(3)}</span>
                  </div>
                ))}
              </div>
            ))}
          </CardBody>
        )}
      </Card>
    </div>
  );
}

function CustomerPanel({ run, cid, onPick, onClose }: { run: string; cid: string; onPick: (id: string) => void; onClose: () => void }) {
  const q = useQuery({ queryKey: ["customer", run, cid], queryFn: () => api.customer(run, cid), retry: 0 });
  const rows = q.data?.events ?? [];
  const pts = rows.map((r) => ({ t: r.event_time_us / 1e6, amount: r.amount_usd, outcome: r.outcome ?? "none", id: r.event_id, fraud: r.is_fraud }));
  const p = q.data?.customer;
  return (
    <Card className="mb-4">
      <CardHeader icon={<User />} title={<span>Customer <span className="font-mono">{cid}</span></span>}
        subtitle={p ? `${p.segment} · ${p.home_country ?? "?"} · ${fmtNum(p.tenure_days)} days tenure · avg ticket ${fmtUsd(p.avg_ticket_usd)}` : `${rows.length} events in this run`}
        actions={<Button variant="ghost" size="icon-sm" onClick={onClose} aria-label="Close customer"><X /></Button>} />
      <CardBody>
        {q.isLoading ? <Skeleton className="h-40" /> : q.error ? <ErrorState error={q.error} /> : (
          <>
            <ResponsiveContainer width="100%" height={180}>
              <ScatterChart margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
                <CartesianGrid stroke="var(--grid)" />
                <XAxis dataKey="t" type="number" domain={["dataMin", "dataMax"]} tickLine={false} axisLine={false} tick={{ fill: "var(--fg-subtle)", fontSize: 11 }}
                  tickFormatter={(v) => new Date(v * 1000).toISOString().slice(5, 16).replace("T", " ")} minTickGap={60} />
                <YAxis dataKey="amount" tickLine={false} axisLine={false} width={56} tick={{ fill: "var(--fg-subtle)", fontSize: 11 }} tickFormatter={(v) => fmtUsd(v)} />
                <ZAxis range={[60, 60]} />
                <Tooltip isAnimationActive={false} content={({ active, payload }) => {
                  const x = active && payload?.[0]?.payload as (typeof pts)[number] | undefined;
                  return x ? <div className="rounded-xl border border-border bg-surface px-3 py-2 text-[12px] shadow-xl"><div className="font-mono">{x.id}</div><div className="text-muted">{fmtUsd(x.amount)} · {x.outcome.toLowerCase()}{x.fraud ? " · fraud" : ""}</div></div> : null;
                }} />
                {["APPROVE", "CHALLENGE", "REVIEW", "DECLINE", "none"].map((o) => (
                  <Scatter key={o} data={pts.filter((p2) => p2.outcome === o)} fill={OUTCOME_COLOR[o] ?? "var(--fg-subtle)"} isAnimationActive={false}
                    onClick={(d) => onPick((d as unknown as { id: string }).id)} className="cursor-pointer" />
                ))}
              </ScatterChart>
            </ResponsiveContainer>
            <div className="mt-1 flex flex-wrap gap-4 text-[11px] text-subtle">
              {["APPROVE", "CHALLENGE", "REVIEW", "DECLINE"].map((o) => <span key={o} className="inline-flex items-center gap-1.5"><span className="size-2 rounded-full" style={{ background: OUTCOME_COLOR[o] }} />{o.toLowerCase()}</span>)}
              <span>click a point or a row to open the decision</span>
            </div>
            {q.data?.summary && <TrailSummary s={q.data.summary} />}
            <DecisionTrail rows={rows} features={q.data?.features ?? []} onPick={onPick} />
          </>
        )}
      </CardBody>
    </Card>
  );
}

const TRAIL_LABEL: Record<string, string> = {
  cust_count_1h: "cust 1h", cust_count_24h: "cust 24h", cust_amount_sum_24h: "cust $24h", card_count_1h: "card 1h",
  card_small_cnp_count_10m: "small CNP 10m", card_distinct_merchants_10m: "merchants 10m", card_cvv_fail_count_1h: "CVV fails 1h",
  cust_device_novel: "new device", cust_payee_novel: "new payee", amount_to_avg_ratio: "× avg", scheme_score: "scheme",
};

function TrailSummary({ s }: { s: NonNullable<import("@/lib/types").CustomerResponse["summary"]> }) {
  const span = (s.last_us - s.first_us) / 3.6e9;
  const items: [string, React.ReactNode][] = [
    ["events", fmtNum(s.events)], ["cards", fmtNum(s.cards)], ["fraud events", fmtNum(s.fraud_events)],
    ["approved", fmtNum(s.approve)], ["stepped up", fmtNum(s.challenge)], ["reviewed", fmtNum(s.review)], ["declined", fmtNum(s.decline)],
    ["spend", fmtUsd(s.amount_usd)], ["over", span < 48 ? `${fmtNum(span, 1)} h` : `${fmtNum(span / 24, 1)} days`],
  ];
  return (
    <div className="mt-4 grid grid-cols-3 gap-x-4 gap-y-2 rounded-xl border border-border bg-surface-2/40 p-3 sm:grid-cols-5 lg:grid-cols-9">
      {items.map(([k, v]) => <div key={k}><div className="text-[11px] text-subtle">{k}</div><div className="text-[14px] font-semibold num">{v}</div></div>)}
    </div>
  );
}

/** Every event of the customer in order: what the decision saw (state built by the earlier events) and did. */
function DecisionTrail({ rows, features, onPick }: { rows: TrailRow[]; features: string[]; onPick: (id: string) => void }) {
  if (!rows.length) return null;
  const shown = features.filter((f) => f in TRAIL_LABEL);
  return (
    <div className="mt-4">
      <div className="mb-1.5 flex flex-wrap items-baseline justify-between gap-2">
        <div className="text-[12px] font-semibold uppercase tracking-[0.1em] text-subtle">decision trail</div>
        <div className="text-[11.5px] text-muted">one Kafka key per customer: each event is scored with the state the earlier ones built</div>
      </div>
      <div className="max-h-[460px] overflow-auto rounded-xl border border-border">
        <table className="w-full min-w-[1100px] text-[12px]">
          <thead className="sticky top-0 z-10 bg-surface-2 text-left text-[10.5px] uppercase tracking-wider text-subtle">
            <tr><th className="px-3 py-2 font-medium">Time</th><th className="px-2 py-2 font-medium">Card</th><th className="px-2 py-2 text-right font-medium">Amount</th>
              <th className="px-2 py-2 font-medium">Channel</th><th className="px-2 py-2 font-medium">Truth</th><th className="px-2 py-2 font-medium">Score</th>
              <th className="px-2 py-2 font-medium">Outcome</th><th className="px-2 py-2 font-medium">Rules</th>
              {shown.map((f) => <th key={f} className="px-2 py-2 text-right font-medium" title={f}>{TRAIL_LABEL[f]}</th>)}
              <th className="px-3 py-2 font-medium">Label</th></tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.event_id} onClick={() => onPick(r.event_id)}
                className={cn("cursor-pointer border-t border-border hover:bg-surface-2/60", r.is_fraud && "bg-fail/[0.05]")}>
                <td className="whitespace-nowrap px-3 py-1.5 font-mono text-[11px] text-muted">{fmtSimTime(r.event_time_us).slice(5)}</td>
                <td className="whitespace-nowrap px-2 py-1.5 font-mono text-[11px]" title={r.card_id}>{r.card_id === "0" ? "—" : `…${r.card_id.slice(-5)}`}</td>
                <td className="px-2 py-1.5 text-right num">{fmtUsd(r.amount_usd)}</td>
                <td className="px-2 py-1.5 text-muted">{r.channel.toLowerCase().replace(/_/g, " ")}</td>
                <td className="px-2 py-1.5">{r.is_fraud ? <Badge tone="fail">{r.fraud_type.replace(/_/g, " ")}</Badge> : <span className="text-subtle">genuine</span>}</td>
                <td className="px-2 py-1.5">{isNum(r.score) ? <ScoreBar score={r.score} className="w-24" /> : <span className="text-[11px] text-muted">{r.path === "RULES_ONLY_FALLBACK" ? "rules only (model missed its deadline)" : "—"}</span>}</td>
                <td className="px-2 py-1.5"><div className="flex items-center gap-1"><OutcomeBadge outcome={r.outcome} />{r.stepup && <span className="text-[10.5px] text-muted">{r.stepup}</span>}</div></td>
                <td className="max-w-[220px] truncate px-2 py-1.5 font-mono text-[10.5px]" title={(r.rule_hits ?? []).join(", ")}>{(r.rule_hits ?? []).join(", ") || <span className="text-subtle">—</span>}</td>
                {shown.map((f) => {
                  const v = r.features?.[f];
                  return <td key={f} className={cn("px-2 py-1.5 text-right num text-[11.5px]", !v && "text-subtle")}>{isNum(v) ? (Number.isInteger(v) ? fmtNum(v) : fmtNum(v, f === "amount_to_avg_ratio" ? 1 : 0)) : "—"}</td>;
                })}
                <td className="whitespace-nowrap px-3 py-1.5 text-[11px] text-muted">{r.label ? `${r.label.fraud ? "fraud" : "genuine"} after ${fmtNum(r.label.after_days, 0)} d` : "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

const SHOW_OPTS = [{ value: "mixed", label: "genuine + fraud" }, { value: "fraud", label: "with fraud" },
  { value: "multi_card", label: "several cards" }, { value: "all", label: "all" }] as const;
const SORT_OPTS = [{ value: "events", label: "events" }, { value: "fraud", label: "fraud" }, { value: "flagged", label: "flagged" },
  { value: "max_score", label: "max score" }] as const;

/** Customers of the run, to find the ones worth reading: many events, several cards, genuine and fraud. */
function Customers({ run, onCustomer }: { run: string; onCustomer: (cid: string) => void }) {
  const [show, setShow] = React.useState<string>("mixed");
  const [sort, setSort] = React.useState<string>("events");
  const q = useQuery({ queryKey: ["customers", run, show, sort], queryFn: () => api.customers(run, sort, show, 100), retry: 0 });
  const st = q.data?.stats;
  return (
    <Card className="overflow-hidden">
      <CardHeader icon={<Users />} title="Customers"
        subtitle={st ? `${fmtNum(st.customers)} customers · ${fmtNum(st.with_2_plus ?? 0)} with 2+ events · ${fmtNum(st.with_10_plus ?? 0)} with 10+ · median ${fmtNum(st.median_events ?? 0, 0)}, most ${fmtNum(st.max_events ?? 0)} · ${fmtNum(st.mixed ?? 0)} with genuine and fraud activity` : "per-customer decisions"} />
      <div className="flex flex-wrap items-center gap-2 px-5 pb-3">
        <Segmented size="sm" value={show} onChange={setShow} options={SHOW_OPTS.map((o) => ({ ...o }))} />
        <span className="text-[11.5px] text-subtle">sort by</span>
        <Segmented size="sm" value={sort} onChange={setSort} options={SORT_OPTS.map((o) => ({ ...o }))} />
      </div>
      {q.isLoading ? <div className="space-y-2 px-5 pb-5">{Array.from({ length: 8 }, (_, i) => <Skeleton key={i} className="h-8" />)}</div>
        : q.error ? <div className="px-5 pb-5">{isNotFound(q.error) ? <Unavailable what="Customers" /> : <ErrorState error={q.error} />}</div>
          : !q.data?.customers.length ? <div className="px-5 pb-5"><EmptyState icon={<Users />} title="No customers match">Try another filter.</EmptyState></div> : (
            <div className="max-h-[640px] overflow-auto">
              <table className="w-full min-w-[860px] text-[12.5px]">
                <thead className="sticky top-0 z-10 border-y border-border bg-surface-2 text-left text-[11px] uppercase tracking-wider text-subtle">
                  <tr><th className="px-5 py-2 font-medium">Customer</th><th className="px-3 py-2 text-right font-medium">Events</th><th className="px-3 py-2 text-right font-medium">Cards</th>
                    <th className="px-3 py-2 font-medium">Fraud</th><th className="px-3 py-2 text-right font-medium">Flagged</th><th className="px-3 py-2 text-right font-medium">Declined</th>
                    <th className="px-3 py-2 text-right font-medium">Stepped up</th><th className="px-3 py-2 font-medium">Max score</th><th className="px-3 py-2 text-right font-medium">Spend</th>
                    <th className="px-5 py-2 text-right font-medium">Active</th></tr>
                </thead>
                <tbody>
                  {q.data.customers.map((c: CustomerRow) => {
                    const hours = (c.last_us - c.first_us) / 3.6e9;
                    return (
                      <tr key={c.customer_id} onClick={() => onCustomer(c.customer_id)} className="cursor-pointer border-b border-border last:border-0 hover:bg-surface-2/60">
                        <td className="px-5 py-2 font-mono text-[12px] text-accent">{c.customer_id}</td>
                        <td className="px-3 py-2 text-right num font-medium">{fmtNum(c.events)}</td>
                        <td className="px-3 py-2 text-right num">{fmtNum(c.cards)}</td>
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

function Campaigns({ run, onPick }: { run: string; onPick: (id: string) => void }) {
  const q = useQuery({ queryKey: ["campaigns", run], queryFn: () => api.campaigns(run), retry: 0 });
  const [sel, setSel] = React.useState<string | null>(null);
  const ev = useQuery({ queryKey: ["campaign", run, sel], queryFn: () => api.campaign(run, sel!), enabled: Boolean(sel) });
  if (q.isLoading) return <Skeleton className="h-64" />;
  if (q.error) return isNotFound(q.error) ? <Unavailable what="Campaigns" /> : <ErrorState error={q.error} />;
  const cs = q.data?.campaigns ?? [];
  if (!cs.length) return <EmptyState icon={<Flag />} title="No fraud campaigns in this run">Pick a fraud pack (e.g. mixed-realistic) to see campaigns.</EmptyState>;
  return (
    <div className="grid grid-cols-1 gap-4 xl:grid-cols-[minmax(0,1.3fr)_minmax(0,1fr)]">
      <Card className="overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full min-w-[720px] text-[12.5px]">
            <thead className="border-b border-border bg-surface-2/40 text-left text-[11px] uppercase tracking-wider text-subtle">
              <tr><th className="px-5 py-2 font-medium">Campaign</th><th className="px-3 py-2 font-medium">Typology</th><th className="px-3 py-2 text-right font-medium">Events</th>
                <th className="px-3 py-2 text-right font-medium">Flagged</th><th className="px-3 py-2 text-right font-medium">Declined</th><th className="px-3 py-2 text-right font-medium">Approved first</th><th className="px-5 py-2 text-right font-medium">$ approved</th></tr>
            </thead>
            <tbody>
              {cs.map((c) => (
                <tr key={c.campaign_id} onClick={() => setSel(c.campaign_id)} className={cn("cursor-pointer border-b border-border last:border-0 hover:bg-surface-2/60", sel === c.campaign_id && "bg-accent/8")}>
                  <td className="px-5 py-2 font-mono">{c.campaign_id}</td>
                  <td className="px-3 py-2">{titleCase(c.fraud_type)}</td>
                  <td className="px-3 py-2 text-right num">{fmtNum(c.events)}</td>
                  <td className="px-3 py-2 text-right num">{fmtPct(c.flagged / Math.max(1, c.events), 0)}</td>
                  <td className="px-3 py-2 text-right num">{fmtPct(c.declined / Math.max(1, c.events), 0)}</td>
                  <td className="px-3 py-2 text-right num">{fmtNum(c.approved_before_first_decline)}</td>
                  <td className="px-5 py-2 text-right num">{fmtUsd(c.usd_approved)} <span className="text-subtle">/ {fmtUsd(c.usd_total)}</span></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>
      <Card>
        <CardHeader title={sel ? `Campaign ${sel}` : "Campaign timeline"} subtitle={sel ? "events in order; the first decline is where the campaign was stopped" : "select a campaign"} />
        <CardBody>
          {!sel ? <div className="py-10 text-center text-[13px] text-subtle">Select a campaign to see its events.</div> : ev.isLoading ? <Skeleton className="h-48" /> : (
            <ol className="relative max-h-[460px] space-y-0 overflow-y-auto border-l border-border pl-4">
              {(ev.data?.events ?? []).map((e) => (
                <li key={e.event_id} className="relative py-1.5">
                  <span className="absolute -left-[21px] top-3 size-2.5 rounded-full border-2 border-surface" style={{ background: OUTCOME_COLOR[e.outcome ?? ""] ?? "var(--fg-subtle)" }} />
                  <button onClick={() => onPick(e.event_id)} className="flex w-full items-center gap-3 rounded-lg px-2 py-1 text-left text-[12.5px] hover:bg-surface-2 cursor-pointer">
                    <span className="font-mono text-[11px] text-subtle">{fmtSimTime(e.event_time_us).slice(5)}</span>
                    <span className="num">{fmtUsd(e.amount_usd)}</span>
                    <span className="truncate text-muted">{e.payment_type.toLowerCase().replace(/_/g, " ")}</span>
                    <span className="ml-auto"><OutcomeBadge outcome={e.outcome} /></span>
                  </button>
                </li>
              ))}
            </ol>
          )}
        </CardBody>
      </Card>
    </div>
  );
}

export default function Explorer() {
  const [run, setRun] = useParam("run");
  const [event, setEvent] = useParam("event");
  const [customer, setCustomer] = useParam("customer");
  const [tab, setTab] = useParam("tab");
  const [qp, setQp] = useParam("q");
  const [text, setText] = React.useState(qp ?? "");
  const [query, setQuery] = React.useState(qp ?? "");
  const runs = useQuery({ queryKey: ["runs"], queryFn: () => api.runs(500) });
  const done = (runs.data?.history ?? []).filter((r) => r.status === "done");
  const runId = run || done[0]?.run_id || "";
  const search = useQuery({ queryKey: ["search", runId, query], queryFn: () => api.search(runId, query, "auto", 100), enabled: Boolean(runId), retry: 0 });

  const pick = (id: string) => setEvent(id, { tab: null });
  return (
    <>
      <PageHeader eyebrow="Explorer" title="Event & decision explorer"
        subtitle="Look up any event, customer, card or campaign in a run: the transaction, the features it was scored on, the rules that fired and where its latency went, and every customer's decision trail." />
      <div className="mb-4 flex flex-wrap items-center gap-2">
        <Select className="w-full sm:w-[360px]" value={runId} onChange={(v) => setRun(v, { event: null, customer: null })} ariaLabel="Run"
          options={done.map((r) => ({ value: r.run_id, label: <span className="font-mono text-[12.5px]">{r.run_id}</span>, hint: `${r.verdict ?? ""} · ${fmtNum(r.decided)} decisions` }))}
          placeholder={runs.isLoading ? "Loading runs…" : "Pick a run"} />
        <form className="relative min-w-[240px] flex-1" onSubmit={(e) => { e.preventDefault(); setQuery(text.trim()); setQp(text.trim() || null); setTab(null); }}>
          <Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-subtle" />
          <Input value={text} onChange={(e) => setText(e.target.value)} className="pl-9" data-testid="explorer-search"
            placeholder="event id, customer:123, card:…, campaign:…, fraud:card_testing, outcome:DECLINE" />
        </form>
        <Button variant="secondary" onClick={() => { setQuery(text.trim()); setQp(text.trim() || null); setTab(null); }}><ScanSearch />Search</Button>
      </div>
      {!runId ? <EmptyState icon={<ScanSearch />} title="No completed runs">Run a scenario first; its events and decisions become explorable here.</EmptyState> : (
        <Tabs value={tab || "search"} onValueChange={(t) => setTab(t === "search" ? null : t)}>
          <TabsList><TabsTrigger value="search">Events</TabsTrigger><TabsTrigger value="customers">Customers</TabsTrigger><TabsTrigger value="campaigns">Campaigns</TabsTrigger></TabsList>
          <TabsContent value="search" className="mt-4">
            {customer && <CustomerPanel run={runId} cid={customer} onPick={pick} onClose={() => setCustomer(null)} />}
            <div className="grid gap-4">
              <Card className="min-w-0 self-start overflow-hidden">
                <CardHeader title={query ? `Results for “${query}”` : "Recent and fraud events"} subtitle={search.data ? `${search.data.results.length} events` : undefined} />
                {search.isLoading ? <div className="space-y-2 px-5 pb-5">{Array.from({ length: 8 }, (_, i) => <Skeleton key={i} className="h-8" />)}</div>
                  : search.error ? <div className="px-5 pb-5">{isNotFound(search.error) && !String((search.error as Error).message).includes("run") ? <Unavailable what="Explorer" /> : <ErrorState error={search.error} title="Search failed" />}</div>
                    : !search.data?.results.length ? <div className="px-5 pb-5"><EmptyState icon={<Search />} title="No events match">Try an event id from the table, or a prefix like customer:.</EmptyState></div>
                      : <ResultsTable rows={search.data.results} onPick={pick} selected={event} onCustomer={(c) => setCustomer(c)} />}
              </Card>
              {event && <DecisionPanel run={runId} eventId={event} onClose={() => setEvent(null)} onCustomer={(c) => setCustomer(c)} />}
            </div>
          </TabsContent>
          <TabsContent value="customers" className="mt-4">
            <Customers run={runId} onCustomer={(c) => setCustomer(c, { tab: null })} />
          </TabsContent>
          <TabsContent value="campaigns" className="mt-4">
            <Campaigns run={runId} onPick={pick} />
          </TabsContent>
        </Tabs>
      )}
    </>
  );
}
