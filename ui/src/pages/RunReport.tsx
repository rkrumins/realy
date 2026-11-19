import { useQuery } from "@tanstack/react-query";
import {
  ArrowLeft, Copy, Database, Download, FileCode2, FileText, GitCompareArrows, Microscope, ScanSearch, ShieldCheck, ShieldX, Target,
} from "lucide-react";
import { Link, useNavigate, useParams } from "react-router";
import { toast } from "sonner";
import { HBars, LoadCurve, TimeChart } from "@/components/charts";
import { EmptyState, ErrorState, Kpi, MiniBar, SectionTitle, Stat, StatusBadge, TopologyBadge, VerdictBadge } from "@/components/common";
import { StepWaterfall } from "@/components/waterfall";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardBody, CardHeader } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/controls";
import { Dropdown, DropdownContent, DropdownItem, DropdownTrigger } from "@/components/ui/dropdown";
import { api } from "@/lib/api";
import { cn } from "@/lib/cn";
import { fmtCompact, fmtDuration, fmtMs, fmtNum, fmtPct, fmtTime, fmtUsd, isNum, titleCase } from "@/lib/format";
import type { Report, RunDetail, VerifyResult } from "@/lib/types";

function download(name: string, data: unknown) {
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = name;
  a.click();
  setTimeout(() => URL.revokeObjectURL(a.href), 1000);
}

function Hero({ d }: { d: RunDetail }) {
  const s = d.report?.summary;
  const verdict = s?.verdict ?? d.summary.verdict;
  const pass = verdict === "PASS";
  const nav = useNavigate();
  return (
    <div className={cn(
      "relative mb-6 overflow-hidden rounded-2xl border p-5 shadow-card md:p-6",
      !verdict ? "border-border bg-surface" : pass
        ? "border-pass/25 bg-[linear-gradient(115deg,color-mix(in_oklab,var(--pass)_13%,var(--card))_0%,var(--card)_55%)]"
        : "border-fail/25 bg-[linear-gradient(115deg,color-mix(in_oklab,var(--fail)_13%,var(--card))_0%,var(--card)_55%)]",
    )}>
      <div className="flex flex-wrap items-start gap-5">
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            {verdict ? <VerdictBadge verdict={verdict} size="lg" expected={s?.matches_expectation} /> : <StatusBadge status={d.summary.status} />}
            <TopologyBadge topology={d.summary.topology} />
            {d.summary.verify_passed === true && <Badge tone="pass"><ShieldCheck />verified</Badge>}
            {d.summary.verify_passed === false && <Badge tone="fail"><ShieldX />verification failed</Badge>}
            {d.summary.abort_reason && <Badge tone="warn">aborted: {d.summary.abort_reason}</Badge>}
          </div>
          <h1 className="mt-3 font-mono text-[22px] font-semibold tracking-tight text-fg md:text-[26px]">{d.summary.scenario}</h1>
          <div className="mt-1 flex flex-wrap gap-x-4 gap-y-1 text-[12.5px] text-muted">
            <span className="font-mono">{d.run_id}</span>
            <span>{fmtTime(d.summary.started_at)}</span>
            <span>{fmtDuration(d.summary.duration_s)} measured</span>
            {s?.expected_verdict && <span>expected {s.expected_verdict}</span>}
          </div>
          {!!s?.reasons?.length && (
            <ul className="mt-3 space-y-1">
              {s.reasons.map((r) => (
                <li key={r} className="flex gap-2 text-[13px] text-fg"><span className={cn("mt-2 size-1.5 shrink-0 rounded-full", pass ? "bg-pass" : "bg-fail")} />{r}</li>
              ))}
            </ul>
          )}
        </div>
        <div className="flex flex-wrap gap-2">
          <Button variant="secondary" onClick={() => nav(`/explorer?run=${encodeURIComponent(d.run_id)}`)}><ScanSearch />Explore decisions</Button>
          <Button variant="secondary" onClick={() => nav(`/compare?a=${encodeURIComponent(d.run_id)}`)}><GitCompareArrows />Compare</Button>
          <Dropdown>
            <DropdownTrigger asChild><Button variant="outline"><Download />Downloads</Button></DropdownTrigger>
            <DropdownContent align="end">
              {d.summary.has_html && <DropdownItem asChild><a href={`/api/runs/${encodeURIComponent(d.run_id)}/report.html`} target="_blank" rel="noreferrer"><FileText />HTML report</a></DropdownItem>}
              {d.report && <DropdownItem onSelect={() => download(`${d.run_id}-report.json`, d.report)}><FileCode2 />report.json</DropdownItem>}
              {d.verify && <DropdownItem onSelect={() => download(`${d.run_id}-verify.json`, d.verify)}><ShieldCheck />verify.json</DropdownItem>}
              {d.manifest && <DropdownItem onSelect={() => download(`${d.run_id}-manifest.json`, d.manifest)}><Database />manifest.json</DropdownItem>}
              <DropdownItem onSelect={() => { navigator.clipboard?.writeText(d.run_id); toast.success("Run id copied"); }}><Copy />Copy run id</DropdownItem>
            </DropdownContent>
          </Dropdown>
        </div>
      </div>
    </div>
  );
}

function Kpis({ r }: { r: Report }) {
  const s = r.summary;
  const b = r.budget;
  return (
    <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
      <Kpi label="Achieved TPS" value={fmtNum(s.achieved_emit_tps)} sub={`${fmtCompact(s.emitted)} emitted`} />
      <Kpi label="e2e p50" value={fmtMs(s.e2e_p50_ms)} />
      <Kpi label="e2e p99" value={fmtMs(s.e2e_p99_ms)} sub={`budget ${fmtMs(b?.e2e_p99_ms)}`} tone={isNum(s.e2e_p99_ms) && b ? (s.e2e_p99_ms > b.e2e_p99_ms ? "fail" : "pass") : undefined} />
      <Kpi label="e2e p99.9" value={fmtMs(s.e2e_p999_ms)} />
      <Kpi label="Hot path p99" value={fmtMs(s.hot_path_p99_ms)} sub={b ? `budget ${fmtMs(b.hot_path_p99_ms)}` : undefined}
        tone={isNum(s.hot_path_p99_ms) && b ? (s.hot_path_p99_ms > b.hot_path_p99_ms ? "fail" : "pass") : undefined} />
      <Kpi label="Within budget" value={fmtPct(s.within_budget, 2)} sub={b?.min_compliance ? `need ${fmtPct(b.min_compliance, 1)}` : undefined}
        tone={b?.min_compliance ? (s.within_budget >= b.min_compliance ? "pass" : "fail") : undefined} />
      <Kpi label="Missing / dupes" value={`${fmtNum(s.missing)} / ${fmtNum(s.duplicates)}`} sub={`${fmtCompact(s.decided)} decided`} tone={s.missing > 0 ? "fail" : "pass"} />
      <Kpi label="Degraded" value={fmtPct(s.degraded_fraction, 2)} sub={isNum(s.schedule_lag_p99_ms) ? `sched lag p99 ${fmtMs(s.schedule_lag_p99_ms)}` : undefined}
        tone={s.degraded_fraction > 0.01 ? "warn" : undefined} />
    </div>
  );
}

function Timeline({ r }: { r: Report }) {
  const n = Math.max(r.e2e_p99_by_second?.length ?? 0, r.funnel?.emitted?.length ?? 0);
  const warm = Number(r.manifest?.warmup_s ?? 0);
  const rows = Array.from({ length: n }, (_, i) => ({
    t: i, p50: r.e2e_p50_by_second?.[i] ?? null, p99: r.e2e_p99_by_second?.[i] ?? null,
    emitted: r.funnel?.emitted?.[i] ?? null, acked: r.funnel?.acked?.[i] ?? null,
    decided: r.funnel?.decided?.[i] ?? null, backlog: r.backlog?.[i] ?? null,
  }));
  const secs = (v: number | string) => `${v}s`;
  const marks = warm ? [{ x: warm, label: "measure" }] : undefined;
  return (
    <div className="grid grid-cols-1 gap-5 lg:grid-cols-2">
      <Card>
        <CardHeader title="Latency over the run" subtitle="e2e percentiles per second (incl. warm-up)" />
        <CardBody>
          <TimeChart data={rows} series={[{ key: "p50", label: "p50" }, { key: "p99", label: "p99" }]} xFormat={secs} yFormat={(v) => fmtMs(v)}
            budget={r.budget?.e2e_p99_ms} markers={marks} syncId="rep" />
        </CardBody>
      </Card>
      <Card>
        <CardHeader title="Throughput funnel" subtitle="events/s emitted → acked → decided" />
        <CardBody>
          <TimeChart data={rows} series={[{ key: "emitted", label: "emitted" }, { key: "acked", label: "acked" }, { key: "decided", label: "decided" }]}
            xFormat={secs} markers={marks} syncId="rep" />
        </CardBody>
      </Card>
    </div>
  );
}

function Detection({ r }: { r: Report }) {
  const d = r.detection;
  if (!d || !isNum(d.fraud_events)) return null;
  const types = Object.entries(d.by_type ?? {}).sort((a, b) => b[1].events - a[1].events);
  const rules = Object.entries(d.by_rule ?? {}).sort((a, b) => b[1].legit - a[1].legit);
  return (
    <>
      <SectionTitle>Detection quality</SectionTitle>
      <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6">
        <Kpi label="Fraud events" value={fmtNum(d.fraud_events)} sub={`${fmtCompact(d.legit_events)} legit`} />
        <Kpi label="Recall (flagged)" value={fmtPct(d.recall_flagged, 1)} tone="accent" />
        <Kpi label="Recall (declined)" value={fmtPct(d.recall_declined, 1)} />
        <Kpi label="Precision (flagged)" value={fmtPct(d.precision_flagged, 1)} />
        <Kpi label="False-positive rate" value={fmtPct(d.false_positive_rate, 2)} tone={isNum(d.false_positive_rate) && d.false_positive_rate > 0.05 ? "warn" : undefined} />
        <Kpi label="Fraud $ approved" value={fmtUsd(d.fraud_usd_approved)} sub={`of ${fmtUsd(d.fraud_usd_total)}`} />
      </div>
      {types.length > 0 && (
        <div className="mt-4 grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
          {types.map(([name, t]) => (
            <Card key={name} className="p-4">
              <div className="flex items-center justify-between gap-2">
                <span className="text-[13.5px] font-semibold text-fg">{titleCase(name)}</span>
                <Badge>{t.campaigns} campaign{t.campaigns === 1 ? "" : "s"}</Badge>
              </div>
              <div className="mt-3 space-y-2 text-[12px]">
                <div className="flex items-center gap-2"><span className="w-20 text-muted">flagged</span><MiniBar value={t.recall_flagged} /><span className="w-12 text-right num">{fmtPct(t.recall_flagged, 0)}</span></div>
                <div className="flex items-center gap-2"><span className="w-20 text-muted">declined</span><MiniBar value={t.recall_declined} tone="pass" /><span className="w-12 text-right num">{fmtPct(t.recall_declined, 0)}</span></div>
              </div>
              <div className="mt-3 grid grid-cols-3 gap-2 border-t border-border pt-3 text-[11.5px]">
                <div><div className="text-subtle">events</div><div className="font-medium num">{fmtNum(t.events)}</div></div>
                <div><div className="text-subtle">approved first</div><div className="font-medium num">{fmtNum(t.approved_before_first_decline_mean, 1)}</div></div>
                <div><div className="text-subtle">$ approved</div><div className="font-medium num">{fmtUsd(t.usd_approved)}</div></div>
              </div>
            </Card>
          ))}
        </div>
      )}
      {rules.length > 0 && (
        <Card className="mt-4 overflow-hidden">
          <CardHeader title="Rules" subtitle="sorted by legitimate hits: the false-positive drivers first" />
          <div className="overflow-x-auto">
            <table className="w-full min-w-[640px] text-[12.5px]">
              <thead className="border-y border-border bg-surface-2/40 text-left text-[11px] uppercase tracking-wider text-subtle">
                <tr><th className="px-5 py-2 font-medium">Rule</th><th className="px-3 py-2 text-right font-medium">Hits</th><th className="px-3 py-2 text-right font-medium">Fraud</th>
                  <th className="px-3 py-2 text-right font-medium">Legit</th><th className="px-3 py-2 font-medium">Precision</th><th className="px-5 py-2 text-right font-medium">Legit hit rate</th></tr>
              </thead>
              <tbody>
                {rules.map(([id, x]) => (
                  <tr key={id} className="border-b border-border last:border-0">
                    <td className="px-5 py-2 font-mono text-[12px]">{id}</td>
                    <td className="px-3 py-2 text-right num">{fmtNum(x.hits)}</td>
                    <td className="px-3 py-2 text-right num">{fmtNum(x.fraud)}</td>
                    <td className="px-3 py-2 text-right num">{fmtNum(x.legit)}</td>
                    <td className="px-3 py-2"><div className="flex items-center gap-2"><MiniBar value={x.precision} className="w-24" /><span className="num">{fmtPct(x.precision, 1)}</span></div></td>
                    <td className="px-5 py-2 text-right num">{fmtPct(x.legit_hit_rate, 2)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      )}
    </>
  );
}

function Parity({ label, checked, parity }: { label: string; checked?: number; parity?: number }) {
  if (!isNum(checked)) return null;
  const ok = parity === 1;
  return (
    <div className="rounded-xl border border-border bg-surface-2/40 p-3">
      <div className="flex items-center justify-between text-[12px] text-muted">{label}{ok ? <ShieldCheck className="size-4 text-pass" /> : <ShieldX className="size-4 text-fail" />}</div>
      <div className={cn("mt-1 text-[20px] font-semibold num", ok ? "text-pass" : "text-fail")}>{fmtPct(parity, 2)}</div>
      <div className="text-[11.5px] text-subtle num">{fmtNum(checked)} checked</div>
    </div>
  );
}

function Verify({ v }: { v: VerifyResult | null }) {
  if (!v) return (
    <>
      <SectionTitle>Correctness verification</SectionTitle>
      <EmptyState icon={<Microscope />} title="Not verified">Launch the run with “Verify correctness” (or run <code className="font-mono">pulsebench verify</code>) to recompute features, rules and scores independently.</EmptyState>
    </>
  );
  const f = v.features;
  const byEntity = new Map<string, [string, { agreement: number; max_abs_diff: number; source: string }][]>();
  for (const [name, x] of Object.entries(f?.per_feature ?? {})) {
    const e = x.entity ?? "derived";
    if (!byEntity.has(e)) byEntity.set(e, []);
    byEntity.get(e)!.push([name, x]);
  }
  const a = v.audit ?? {};
  return (
    <>
      <SectionTitle actions={v.passed !== undefined && (v.passed ? <Badge tone="pass"><ShieldCheck />passed</Badge> : <Badge tone="fail"><ShieldX />failed</Badge>)}>
        Correctness verification
      </SectionTitle>
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <Parity label="Rule parity (CEL Java vs Python)" checked={v.rules?.checked} parity={v.rules?.parity} />
        <Parity label="Score parity (proxy recompute)" checked={v.score?.checked} parity={v.score?.parity} />
        <Parity label="Outcome re-derivation" checked={v.outcome?.checked} parity={v.outcome?.parity} />
        {f && (
          <div className="rounded-xl border border-border bg-surface-2/40 p-3">
            <div className="text-[12px] text-muted">Feature vectors exactly equal</div>
            <div className="mt-1 text-[20px] font-semibold num">{fmtPct(f.exact_set_agreement, 1)}</div>
            <div className="text-[11.5px] text-subtle num">{fmtNum(f.compared_events)} events compared</div>
          </div>
        )}
      </div>
      {f && (
        <Card className="mt-4">
          <CardHeader title="Per-feature agreement" subtitle={f.note} />
          <CardBody className="grid grid-cols-1 gap-x-8 gap-y-5 md:grid-cols-2 xl:grid-cols-3">
            {[...byEntity.entries()].map(([entity, feats]) => (
              <div key={entity}>
                <div className="mb-2 text-[11px] font-semibold uppercase tracking-[0.12em] text-subtle">{entity}</div>
                <div className="space-y-1.5">
                  {feats.sort((x, y) => x[1].agreement - y[1].agreement).map(([name, x]) => (
                    <div key={name} className="grid grid-cols-[minmax(0,1fr)_80px_46px] items-center gap-2 text-[12px]" title={`max |Δ| ${fmtNum(x.max_abs_diff, 2)} · ${x.source}`}>
                      <span className="truncate font-mono text-[11.5px] text-muted">{name}</span>
                      <MiniBar value={x.agreement} tone={x.agreement >= 0.999 ? "pass" : x.agreement >= 0.9 ? "accent" : "warn"} />
                      <span className="text-right num">{fmtPct(x.agreement, 0)}</span>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </CardBody>
        </Card>
      )}
      {Object.keys(a).length > 0 && (
        <Card className="mt-4">
          <CardHeader title="ClickHouse audit" subtitle="Async audit via the Kafka engine: completeness, duplicate collapse, visibility lag, replay" icon={<Database />}
            actions={a.complete === true ? <Badge tone="pass">complete</Badge> : a.complete === false ? <Badge tone="fail">gaps</Badge> : undefined} />
          <CardBody className="grid grid-cols-1 gap-x-8 md:grid-cols-3">
            <div>
              <Stat label="Emitted" value={fmtNum(a.emitted)} />
              <Stat label="Audited (unique)" value={fmtNum(a.audited_unique)} />
              <Stat label="Audit gaps" value={fmtNum(a.audit_gaps)} />
            </div>
            <div>
              <Stat label="Rows incl. duplicates" value={fmtNum(a.rows_including_duplicates)} />
              <Stat label="After ReplacingMergeTree" value={fmtNum(a.rows_after_replacing_merge)} />
              <Stat label="Visibility p50 / p99" value={`${fmtMs(a.visibility_p50_ms)} / ${fmtMs(a.visibility_p99_ms)}`} />
            </div>
            <div>
              <Stat label="Labels" value={fmtNum(a.labels_total)} />
              <Stat label="Training rows / fraud" value={`${fmtNum(a.training_rows)} / ${fmtNum(a.training_fraud_labels)}`} />
              <Stat label="Replay score parity" value={`${fmtPct(a.replay_score_parity, 2)} of ${fmtNum(a.replay_checked)}`} />
            </div>
            {a.error && <div className="md:col-span-3 text-[12.5px] text-fail">{String(a.error)}</div>}
          </CardBody>
        </Card>
      )}
    </>
  );
}

export default function RunReport() {
  const { id = "" } = useParams();
  const q = useQuery({ queryKey: ["run", id], queryFn: () => api.run(id), refetchInterval: (qq) => (qq.state.data?.report ? false : 5000) });
  if (q.isLoading) return <div className="space-y-4"><Skeleton className="h-44 rounded-2xl" /><div className="grid grid-cols-4 gap-3">{Array.from({ length: 8 }, (_, i) => <Skeleton key={i} className="h-24 rounded-2xl" />)}</div><Skeleton className="h-80 rounded-2xl" /></div>;
  if (q.error) return <><BackLink /><ErrorState error={q.error} title="Run not found" /></>;
  const d = q.data!;
  const r = d.report;
  const levels = r?.levels ?? [];
  const knee = r?.summary.knee;
  const tail = r?.tail_attribution;
  const sus = r?.sustainability;
  const svc = Object.entries(r?.service?.stages ?? {}).sort((a, b) => b[1].p99_median_ms - a[1].p99_median_ms);

  return (
    <>
      <BackLink />
      <Hero d={d} />
      {!r ? (
        <EmptyState title="No report yet" icon={<FileText />}>The run is still in progress (or did not finish). This page refreshes automatically.</EmptyState>
      ) : (
        <>
          <Kpis r={r} />
          <div className="mt-6 grid grid-cols-1 gap-5 xl:grid-cols-[minmax(0,1.5fr)_minmax(0,1fr)]">
            <Card>
              <CardHeader title="Latency by step" subtitle="measured stage decomposition (collector, one clock domain): p50 bars chained along the hot path, p99 whiskers" />
              <CardBody><StepWaterfall stages={r.stages} budget={r.budget?.e2e_p99_ms} /></CardBody>
            </Card>
            <Card>
              <CardHeader title="Tail attribution" icon={<Target />}
                subtitle={tail?.events ? `${fmtNum(tail.events)} events slower than ${fmtMs(tail.threshold_ms)} (the p99): mean share of their latency per stage` : "events above the p99"} />
              <CardBody>
                {tail?.share ? (
                  <>
                    <HBars data={Object.entries(tail.share).sort((a, b) => b[1] - a[1]).filter(([, v]) => v > 0.001).map(([k, v]) => ({ name: k, value: v }))}
                      format={(v) => fmtPct(v, 0)} highlight={tail.dominant} />
                    {tail.dominant && <p className="mt-2 text-[12.5px] text-muted">The tail is dominated by <b className="font-mono text-fg">{tail.dominant}</b>.</p>}
                  </>
                ) : <div className="py-8 text-center text-[13px] text-subtle">No tail attribution.</div>}
              </CardBody>
            </Card>
          </div>

          <SectionTitle>Over time</SectionTitle>
          <Timeline r={r} />

          {levels.length > 1 && (
            <>
              <SectionTitle>Load curve</SectionTitle>
              <Card>
                <CardHeader title="Latency vs offered load" subtitle={knee ? `Knee at level ${knee.level}: ${fmtNum(knee.target_tps)} TPS offered, p99 ${fmtMs(knee.e2e_p99_ms)}` : "No knee inside the ramp: the pipeline kept up at every level"} />
                <CardBody>
                  <LoadCurve budget={r.budget?.e2e_p99_ms}
                    data={levels.map((l) => ({ tps: l.target_tps, p50: l.stages?.e2e?.p50 ?? null, p99: l.stages?.e2e?.p99 ?? null, p999: l.stages?.e2e?.p999 ?? null, decided: l.decided_tps }))}
                    knee={knee ? { tps: knee.target_tps, p99: knee.e2e_p99_ms } : null} />
                  <div className="mt-4 overflow-x-auto">
                    <table className="w-full min-w-[560px] text-[12.5px]">
                      <thead className="text-left text-[11px] uppercase tracking-wider text-subtle"><tr><th className="py-1.5 font-medium">Level</th><th className="text-right font-medium">Offered</th><th className="text-right font-medium">Emitted</th><th className="text-right font-medium">Decided</th><th className="text-right font-medium">p99</th><th className="text-right font-medium">In budget</th></tr></thead>
                      <tbody>
                        {levels.map((l) => (
                          <tr key={l.level} className={cn("border-t border-border", knee?.level === l.level && "bg-fail/8")}>
                            <td className="py-1.5">{l.level}{knee?.level === l.level && <Badge tone="fail" className="ml-2">knee</Badge>}</td>
                            <td className="text-right num">{fmtNum(l.target_tps)}</td><td className="text-right num">{fmtNum(l.emitted_tps)}</td>
                            <td className="text-right num">{fmtNum(l.decided_tps)}</td><td className="text-right num">{fmtMs(l.stages?.e2e?.p99)}</td>
                            <td className="text-right num">{fmtPct(l.within_budget, 1)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </CardBody>
              </Card>
            </>
          )}

          {sus?.p99_by_window_ms && (
            <>
              <SectionTitle>Sustainability</SectionTitle>
              <Card>
                <CardHeader title={sus.sustainable ? "Sustainable at this load" : "Not sustainable at this load"}
                  subtitle={`p99 per ${sus.window_s}s window · slope ${fmtMs(sus.p99_slope_ms_per_window)} per window · max backlog ${fmtNum(sus.max_backlog)} · backlog ${sus.backlog_growing ? "growing" : "stable"}`}
                  actions={<Badge tone={sus.sustainable ? "pass" : "fail"}>{sus.sustainable ? "sustainable" : "degrading"}</Badge>} />
                <CardBody>
                  <TimeChart data={sus.p99_by_window_ms.map((v, i) => ({ t: i * (sus.window_s ?? 10), p99: v }))} series={[{ key: "p99", label: "p99 per window" }]}
                    xFormat={(v) => `${v}s`} yFormat={(v) => fmtMs(v)} height={170} legend={false} budget={r.budget?.e2e_p99_ms} />
                </CardBody>
              </Card>
            </>
          )}

          <Detection r={r} />
          <Verify v={d.verify} />

          {svc.length > 0 && (
            <>
              <SectionTitle>Service-side stages</SectionTitle>
              <Card className="overflow-hidden">
                <CardHeader title="Per-service histograms" subtitle="median and worst of the per-second p99s reported by each service" />
                <div className="overflow-x-auto">
                  <table className="w-full min-w-[560px] text-[12.5px]">
                    <thead className="border-y border-border bg-surface-2/40 text-left text-[11px] uppercase tracking-wider text-subtle">
                      <tr><th className="px-5 py-2 font-medium">Stage</th><th className="px-3 py-2 text-right font-medium">p50 (median)</th><th className="px-3 py-2 text-right font-medium">p99 (median)</th><th className="px-3 py-2 text-right font-medium">p99 (worst)</th><th className="px-5 py-2 text-right font-medium">Samples</th></tr>
                    </thead>
                    <tbody>
                      {svc.map(([k, x]) => (
                        <tr key={k} className="border-b border-border last:border-0">
                          <td className="px-5 py-1.5 font-mono text-[12px]">{k}</td>
                          <td className="px-3 py-1.5 text-right num">{fmtMs(x.p50_median_ms)}</td><td className="px-3 py-1.5 text-right num">{fmtMs(x.p99_median_ms)}</td>
                          <td className="px-3 py-1.5 text-right num text-muted">{fmtMs(x.p99_max_ms)}</td><td className="px-5 py-1.5 text-right num text-muted">{fmtCompact(x.count)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </Card>
            </>
          )}
        </>
      )}
      {d.logs?.length > 0 && (
        <>
          <SectionTitle>Run log</SectionTitle>
          <pre className="max-h-80 overflow-auto rounded-2xl border border-border bg-surface-2 p-4 font-mono text-[11.5px] leading-5 text-muted">{d.logs.join("\n")}</pre>
        </>
      )}
    </>
  );
}

function BackLink() {
  return (
    <Link to="/runs" className="mb-4 inline-flex items-center gap-1.5 text-[13px] text-muted hover:text-fg">
      <ArrowLeft className="size-4" />All runs
    </Link>
  );
}

