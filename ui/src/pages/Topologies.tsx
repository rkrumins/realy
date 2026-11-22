/**
 * Topologies: how each one is built (steps and components), what it can and cannot do and why, when to
 * choose it, the settings of every step, its known limits, and advice from the latest runs on this stack.
 * Everything comes from contracts/topologies.yaml (GET /api/topologies), so this page, the docs reference
 * and the runner never disagree.
 */
import { useQuery } from "@tanstack/react-query";
import {
  ArrowRight, BookOpen, CheckCircle2, CircleDashed, CircleMinus, CircleX, Compass, Lightbulb, Network, Rocket, Settings2, Wrench,
} from "lucide-react";
import * as React from "react";
import { Link, useSearchParams } from "react-router";
import { EmptyState, ErrorState, PageHeader, SectionTitle, TopologyBadge, VerdictBadge } from "@/components/common";
import { TopologyMap, useTopology } from "@/components/Topology";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardBody, CardHeader } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/controls";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Tooltip } from "@/components/ui/tooltip";
import { api } from "@/lib/api";
import { cn } from "@/lib/cn";
import { fmtCompact, fmtMs, fmtPct, isNum } from "@/lib/format";
import { FALLBACK_TOPOLOGIES } from "@/lib/topology";
import type { AdviceItem, CapStatus, TopologiesResponse, TopologyInfo, TopologyStep } from "@/lib/types";

/** The needs an architect starts from, as capabilities of contracts/topologies.yaml (in this order). */
const NEEDS: { cap: string; label: string }[] = [
  { cap: "answers_caller", label: "The caller waits for the answer" },
  { cap: "multi_message", label: "Payments arrive as several messages" },
  { cap: "rescored_stages", label: "Decide again at later stages" },
  { cap: "owned_features_exact", label: "Exact customer and card features" },
  { cap: "shared_features_exact", label: "Exact merchant / device / payee features" },
  { cap: "survives_flink_restart", label: "Keep deciding while Flink restarts" },
];

const STATUS: Record<CapStatus, { label: string; tone: "pass" | "warn" | "fail" | "neutral"; Icon: typeof CheckCircle2 }> = {
  yes: { label: "yes", tone: "pass", Icon: CheckCircle2 },
  partial: { label: "partly", tone: "warn", Icon: CircleMinus },
  no: { label: "no", tone: "fail", Icon: CircleX },
  "n/a": { label: "n/a", tone: "neutral", Icon: CircleDashed },
};

const SEVERITY: Record<AdviceItem["severity"], "fail" | "warn" | "info" | "pass"> = {
  critical: "fail", warning: "warn", info: "info", good: "pass",
};

/** How a topology meets the selected needs: met, partly met, missed (by capability label). */
export function fitOf(t: TopologyInfo, needs: string[], caps: TopologiesResponse["capabilities"]) {
  const met: string[] = [], partly: string[] = [], missed: string[] = [];
  for (const n of needs) {
    const s = t.capabilities[n]?.status;
    const label = caps[n]?.label ?? n;
    if (s === "yes") met.push(label);
    else if (s === "partial") partly.push(label);
    else missed.push(label);
  }
  return { met, partly, missed };
}

function CapCell({ status, note }: { status: CapStatus | undefined; note?: string }) {
  const s = STATUS[status ?? "n/a"];
  return (
    <Tooltip content={<span className="block max-w-[320px] text-[12px] leading-5">{note ?? "not stated"}</span>}>
      <span className={cn("inline-flex cursor-help items-center gap-1.5 text-[12.5px]",
        s.tone === "pass" && "text-pass", s.tone === "warn" && "text-warn", s.tone === "fail" && "text-fail", s.tone === "neutral" && "text-subtle")}>
        <s.Icon className="size-4 shrink-0" aria-hidden />{s.label}
      </span>
    </Tooltip>
  );
}

function RuntimeChips({ t }: { t: TopologyInfo }) {
  const r = t.runtime;
  const chips = [
    ["events reach the scorer by", r.ingress],
    ["decides", r.scorer],
    ["Flink", r.flink_mode],
    ["multi-message", r.flows.replace(/_/g, " ")],
  ];
  return (
    <div className="flex flex-wrap gap-1.5">
      {chips.map(([k, v]) => (
        <span key={k} className="rounded-md border border-border bg-surface-2/60 px-2 py-0.5 text-[11.5px] text-muted">
          {k} <span className="font-medium text-fg">{v}</span>
        </span>
      ))}
    </div>
  );
}

function TopologyCard({ t, caps, needs, selected, onSelect }: {
  t: TopologyInfo; caps: TopologiesResponse["capabilities"]; needs: string[]; selected: boolean; onSelect: () => void;
}) {
  const g = useTopology(t.id);
  const fit = fitOf(t, needs, caps);
  const l = t.latest;
  return (
    <Card className={cn("flex flex-col p-4 transition", selected && "ring-1 ring-accent/40")} data-testid={`topology-${t.id}`}>
      <div className="flex flex-wrap items-center gap-2">
        <h3 className="text-[15px] font-semibold tracking-tight">{t.title}</h3>
        <TopologyBadge topology={t.id} />
        {needs.length > 0 && (fit.missed.length
          ? <Badge tone="fail">misses {fit.missed.length} of {needs.length}</Badge>
          : fit.partly.length ? <Badge tone="warn">partly meets {fit.partly.length}</Badge>
            : <Badge tone="pass">meets all {needs.length}</Badge>)}
      </div>
      <p className="mt-1.5 text-[13px] leading-5 text-muted">{t.tagline}</p>
      {needs.length > 0 && fit.missed.length > 0 && (
        <p className="mt-1.5 text-[12px] text-fail">Cannot: {fit.missed.join("; ")}</p>
      )}
      <div className="mt-3"><RuntimeChips t={t} /></div>
      <div className="pointer-events-none mt-3 rounded-lg bg-surface-2/50 p-1">
        <TopologyMap graph={g.data ?? FALLBACK_TOPOLOGIES[t.id]} snapshots={[]} budgetMs={150} active={false} mini />
      </div>
      <div className="mt-3 rounded-lg border border-border px-3 py-2">
        {l ? (
          <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-[12.5px]">
            <span className="text-subtle">Last on this stack</span>
            <VerdictBadge verdict={l.verdict} />
            <span className="num">e2e p99 <b>{fmtMs(l.e2e_p99_ms)}</b></span>
            <span className="num">{fmtCompact(l.achieved_tps)} TPS</span>
            {isNum(l.exact_set_agreement) && <span className="num">exact {fmtPct(l.exact_set_agreement, 2)}</span>}
            <Link to={`/runs/${encodeURIComponent(l.run_id)}`} className="ml-auto text-accent hover:underline">report</Link>
          </div>
        ) : (
          <div className="flex items-center gap-2 text-[12.5px] text-subtle">
            Not run on this stack yet
            {t.scenarios[0] && <Link className="ml-auto text-accent hover:underline" to={`/launch?scenario=${encodeURIComponent(t.scenarios[0].name)}`}>launch {t.scenarios[0].name}</Link>}
          </div>
        )}
      </div>
      <div className="mt-3 grid flex-1 gap-2 text-[12.5px] sm:grid-cols-2">
        <div><div className="mb-1 text-[11px] font-medium uppercase tracking-wide text-subtle">Choose it when</div>
          <ul className="list-disc space-y-0.5 pl-4 text-muted">{t.choose_when.slice(0, 3).map((c) => <li key={c}>{c}</li>)}</ul></div>
        {t.avoid_when?.length ? <div><div className="mb-1 text-[11px] font-medium uppercase tracking-wide text-subtle">Avoid it when</div>
          <ul className="list-disc space-y-0.5 pl-4 text-muted">{t.avoid_when.map((c) => <li key={c}>{c}</li>)}</ul></div> : null}
      </div>
      <div className="mt-3 flex items-center gap-2 border-t border-border pt-3">
        <span className="text-[12px] text-subtle">{t.scenarios.length} scenario{t.scenarios.length === 1 ? "" : "s"} · {t.advice.length} advice item{t.advice.length === 1 ? "" : "s"}</span>
        <Button size="sm" variant={selected ? "default" : "secondary"} className="ml-auto" onClick={onSelect}>
          How it works <ArrowRight className="size-3.5" />
        </Button>
      </div>
    </Card>
  );
}

function Matrix({ data }: { data: TopologiesResponse }) {
  const rows = Object.entries(data.capabilities);
  return (
    <Card>
      <CardHeader title="What works and what does not" subtitle="Hover a cell for the reason, in terms of how the topology is built." icon={<Compass className="size-4" />} />
      <CardBody className="overflow-x-auto pt-0">
        <table className="w-full min-w-[720px] border-collapse text-left">
          <thead>
            <tr className="border-b border-border text-[12px] text-subtle">
              <th className="py-2 pr-3 font-medium">Capability</th>
              {data.topologies.map((t) => <th key={t.id} className="px-3 py-2 font-medium">{t.title}</th>)}
            </tr>
          </thead>
          <tbody>
            {rows.map(([cid, c]) => (
              <tr key={cid} className="border-b border-border/60 last:border-0">
                <td className="py-2 pr-3 text-[13px]"><Tooltip content={<span className="block max-w-[300px] text-[12px]">{c.why}</span>}><span className="cursor-help">{c.label}</span></Tooltip></td>
                {data.topologies.map((t) => <td key={t.id} className="px-3 py-2"><CapCell status={t.capabilities[cid]?.status} note={t.capabilities[cid]?.note} /></td>)}
              </tr>
            ))}
            <tr className="border-b border-border/60 text-[12.5px] text-muted">
              <td className="py-2 pr-3 text-[13px] text-fg">Scales by</td>
              {data.topologies.map((t) => <td key={t.id} className="px-3 py-2 align-top">{t.scales_by}</td>)}
            </tr>
            <tr className="text-[12.5px] text-muted">
              <td className="py-2 pr-3 text-[13px] text-fg">The tail usually comes from</td>
              {data.topologies.map((t) => <td key={t.id} className="px-3 py-2 align-top">{t.tail}</td>)}
            </tr>
          </tbody>
        </table>
      </CardBody>
    </Card>
  );
}

function Steps({ steps, comps }: { steps: TopologyStep[]; comps: TopologiesResponse["components"] }) {
  return (
    <ol className="space-y-2">
      {steps.map((s, i) => {
        const c = comps[s.component];
        const label = (s.label ?? c?.label ?? s.id).replace("{entity}", "<entity>");
        return (
          <li key={s.id} className="flex gap-3 rounded-lg border border-border px-3 py-2">
            <span className="num mt-0.5 flex size-6 shrink-0 items-center justify-center rounded-full bg-accent/12 text-[12px] font-semibold text-accent">{i + 1}</span>
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-2 text-[13px]">
                <span className="font-semibold">{label}</span>
                {s.for_each && <Badge tone="neutral">one per entity with stream features</Badge>}
                <span className="font-mono text-[11px] text-subtle">{c?.kind}</span>
              </div>
              <p className="mt-0.5 text-[12.5px] leading-5 text-muted">{s.does}</p>
            </div>
          </li>
        );
      })}
    </ol>
  );
}

function Detail({ t, data }: { t: TopologyInfo; data: TopologiesResponse }) {
  const [flows, setFlows] = React.useState(false);
  const withFlows = flows && !!t.steps_with_flows?.length;
  const g = useTopology(t.id, withFlows);
  const steps = withFlows ? t.steps_with_flows! : t.steps;
  const compIds = [...new Set([...t.steps, ...(t.steps_with_flows ?? [])].map((s) => s.component))];
  return (
    <Card id="detail" className="scroll-mt-20">
      <CardHeader title={<span className="flex items-center gap-2">{t.title} <TopologyBadge topology={t.id} /></span>}
        subtitle={`${t.decides}. Flink: ${t.flink}`} icon={<Network className="size-4" />}
        actions={t.steps_with_flows?.length ? (
          <label className="flex cursor-pointer items-center gap-2 text-[12.5px] text-muted">
            <input type="checkbox" checked={flows} onChange={(e) => setFlows(e.target.checked)} className="accent-[var(--accent)]" />
            with multi-message transactions
          </label>) : undefined} />
      <CardBody className="pt-0">
        <div className="mb-4 overflow-x-auto rounded-lg bg-surface-2/50 p-2">
          <TopologyMap graph={g.data ?? FALLBACK_TOPOLOGIES[t.id]} snapshots={[]} budgetMs={150} active={false} />
        </div>
        <Tabs defaultValue="built">
          <TabsList className="flex-wrap">
            <TabsTrigger value="built"><Network className="size-3.5" />How it is built</TabsTrigger>
            <TabsTrigger value="advice"><Lightbulb className="size-3.5" />Advice from your runs{t.advice.length ? ` (${t.advice.length})` : ""}</TabsTrigger>
            <TabsTrigger value="settings"><Settings2 className="size-3.5" />Settings by component</TabsTrigger>
            <TabsTrigger value="limits"><Wrench className="size-3.5" />Limits and enhancements</TabsTrigger>
          </TabsList>
          <TabsContent value="built" className="pt-3">
            <Steps steps={steps} comps={data.components} />
          </TabsContent>
          <TabsContent value="advice" className="pt-3">
            {t.advice.length === 0 ? (
              <EmptyState icon={<Lightbulb className="size-5" />} title="No measured run yet">
                Advice comes from the latest finished run of this topology: its slow tail, fallbacks, capacity and verification.
                {t.scenarios[0] && <> Launch <Link className="text-accent hover:underline" to={`/launch?scenario=${encodeURIComponent(t.scenarios[0].name)}`}>{t.scenarios[0].name}</Link> to get some.</>}
              </EmptyState>
            ) : (
              <div className="space-y-2">
                {t.latest && <p className="text-[12px] text-subtle">From <Link className="text-accent hover:underline" to={`/runs/${encodeURIComponent(t.latest.run_id)}`}>{t.latest.run_id}</Link> ({t.latest.scenario}).</p>}
                {t.advice.map((a, i) => (
                  <div key={i} className="rounded-lg border border-border px-3 py-2.5">
                    <div className="flex flex-wrap items-center gap-2">
                      <Badge tone={SEVERITY[a.severity]}>{a.severity}</Badge>
                      <Badge tone="neutral">{a.area}</Badge>
                      <span className="text-[13px] font-semibold">{a.title}</span>
                    </div>
                    <p className="mt-1 text-[12.5px] text-muted"><span className="text-subtle">Measured:</span> {a.evidence}</p>
                    {a.action && <p className="mt-1 text-[12.5px]"><span className="text-subtle">Try:</span> {a.action}</p>}
                  </div>
                ))}
              </div>
            )}
          </TabsContent>
          <TabsContent value="settings" className="space-y-3 pt-3">
            {compIds.map((cid) => {
              const c = data.components[cid];
              if (!c) return null;
              return (
                <div key={cid} className="rounded-lg border border-border">
                  <div className="border-b border-border px-3 py-2">
                    <div className="flex flex-wrap items-center gap-2"><span className="text-[13px] font-semibold">{c.label}</span>
                      <span className="font-mono text-[11px] text-subtle">{cid} · {c.kind}</span></div>
                    <p className="mt-0.5 text-[12.5px] text-muted">{c.what}</p>
                    <p className="mt-0.5 font-mono text-[11px] text-subtle">{c.code}</p>
                  </div>
                  {c.config?.length ? (
                    <div className="overflow-x-auto">
                      <table className="w-full min-w-[620px] text-left text-[12px]">
                        <thead><tr className="text-subtle"><th className="px-3 py-1.5 font-medium">Setting</th><th className="px-3 py-1.5 font-medium">Set in</th><th className="px-3 py-1.5 font-medium">Default</th><th className="px-3 py-1.5 font-medium">Effect</th></tr></thead>
                        <tbody>{c.config.map((k) => (
                          <tr key={k.key} className="border-t border-border/60 align-top">
                            <td className="px-3 py-1.5 font-mono text-[11.5px]">{k.key}</td>
                            <td className="px-3 py-1.5 text-muted">{k.in}</td>
                            <td className="px-3 py-1.5 font-mono text-[11.5px] text-muted">{k.default ?? ""}</td>
                            <td className="px-3 py-1.5 text-muted">{k.effect}</td>
                          </tr>))}</tbody>
                      </table>
                    </div>
                  ) : null}
                  <p className="px-3 py-2 text-[12px]"><span className="text-subtle">Swap or extend:</span> {c.extend}</p>
                </div>
              );
            })}
          </TabsContent>
          <TabsContent value="limits" className="pt-3">
            {t.enhancements.length === 0 ? <p className="text-[13px] text-subtle">None recorded.</p> : (
              <div className="space-y-2">
                {t.enhancements.map((e) => (
                  <div key={e.title} className="rounded-lg border border-border px-3 py-2.5">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="text-[13px] font-semibold">{e.title}</span>
                      <Badge tone={e.status === "available" ? "pass" : e.status === "planned" ? "info" : "neutral"}>{e.status}</Badge>
                    </div>
                    <p className="mt-1 text-[12.5px] text-muted"><span className="text-subtle">Why:</span> {e.why}</p>
                    <p className="mt-0.5 text-[12.5px]"><span className="text-subtle">Effect:</span> {e.effect}</p>
                  </div>
                ))}
              </div>
            )}
          </TabsContent>
        </Tabs>
      </CardBody>
    </Card>
  );
}

export default function Topologies() {
  const q = useQuery({ queryKey: ["topologies"], queryFn: api.topologies, staleTime: 30_000 });
  const [params, setParams] = useSearchParams();
  const needs = (params.get("need") ?? "").split(",").filter(Boolean);
  const selected = params.get("t");
  const set = (k: string, v: string | null) => {
    const p = new URLSearchParams(params);
    if (v) p.set(k, v); else p.delete(k);
    setParams(p, { replace: true });
  };
  const toggle = (cap: string) => set("need", (needs.includes(cap) ? needs.filter((n) => n !== cap) : [...needs, cap]).join(",") || null);
  const select = (id: string) => {
    set("t", id);
    requestAnimationFrame(() => document.getElementById("detail")?.scrollIntoView({ behavior: "smooth", block: "start" }));
  };

  if (q.isError) return <><PageHeader eyebrow="Configure" title="Topologies" /><ErrorState error={q.error} /></>;
  const data = q.data;
  const current = data?.topologies.find((t) => t.id === selected) ?? data?.topologies[0];
  return (
    <>
      <PageHeader eyebrow="Configure" title="Topologies"
        subtitle="How each topology is built, what it can and cannot do, and what to improve next, measured on this stack."
        actions={<>
          <Button asChild variant="secondary" size="sm"><Link to="/docs?path=reference/topologies.md"><BookOpen className="size-3.5" />Reference</Link></Button>
          <Button asChild variant="secondary" size="sm"><Link to="/docs?path=dev/topologies.md"><Wrench className="size-3.5" />Build your own</Link></Button>
        </>} />
      <Card className="mb-4 p-4">
        <div className="mb-2 flex items-center gap-2 text-[13px] font-semibold"><Compass className="size-4 text-accent" />What does your pipeline need?</div>
        <div className="flex flex-wrap gap-2">
          {NEEDS.map((n) => {
            const on = needs.includes(n.cap);
            return (
              <button key={n.cap} type="button" aria-pressed={on} onClick={() => toggle(n.cap)}
                className={cn("rounded-full border px-3 py-1 text-[12.5px] transition cursor-pointer",
                  on ? "border-accent/60 bg-accent/12 text-fg" : "border-border text-muted hover:border-border-strong hover:text-fg")}>
                {on && <CheckCircle2 className="mr-1 inline size-3.5 text-accent" />}{n.label}
              </button>
            );
          })}
          {needs.length > 0 && <button type="button" onClick={() => set("need", null)} className="text-[12px] text-subtle hover:text-fg">clear</button>}
        </div>
        <p className="mt-2 text-[12px] text-subtle">Pick what matters; each topology says whether it meets it. Most institutions end up with a hybrid (below).</p>
      </Card>
      {!data ? (
        <div className="grid gap-4 xl:grid-cols-2">{[0, 1, 2, 3].map((i) => <Skeleton key={i} className="h-[420px]" />)}</div>
      ) : (
        <>
          <div className="grid gap-4 xl:grid-cols-2">
            {data.topologies.map((t) => (
              <TopologyCard key={t.id} t={t} caps={data.capabilities} needs={needs} selected={current?.id === t.id && !!selected}
                onSelect={() => select(t.id)} />
            ))}
          </div>
          <SectionTitle className="mt-8">Compare</SectionTitle>
          <Matrix data={data} />
          {current && <>
            <SectionTitle className="mt-8">How it works</SectionTitle>
            <Detail key={current.id} t={current} data={data} />
          </>}
          <Card className="mt-6 p-4">
            <div className="flex items-start gap-3">
              <Rocket className="mt-0.5 size-4 shrink-0 text-accent" />
              <div>
                <div className="text-[13px] font-semibold">The hybrid most institutions end up with</div>
                <p className="mt-1 text-[13px] leading-5 text-muted">{data.hybrid}</p>
              </div>
            </div>
          </Card>
        </>
      )}
    </>
  );
}
