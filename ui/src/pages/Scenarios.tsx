import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  AlertTriangle, CheckCircle2, Copy, Cpu, FileCode2, Globe2, Loader2, Network, Plus, Rocket, Save, ShieldAlert, Sparkles, Target, Timer, Trash2, Waves,
 RotateCcw } from "lucide-react";
import * as React from "react";
import { useNavigate, useSearchParams } from "react-router";
import { toast } from "sonner";
import { ErrorState, PageHeader, TopologyBadge, topologyLabel } from "@/components/common";
import { DistributionEditor, RateProfileEditor } from "@/components/editors";
import { FALLBACK_TOPOLOGIES } from "@/lib/topology";
import { TopologyMap, useTopology } from "@/components/Topology";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardBody, CardHeader } from "@/components/ui/card";
import { Skeleton, Slider, Switch } from "@/components/ui/controls";
import { Dialog, DialogContent } from "@/components/ui/dialog";
import { Input, Label, NumberField, Textarea } from "@/components/ui/input";
import { Select } from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ApiError, api } from "@/lib/api";
import { cn } from "@/lib/cn";
import { preview, type LatencyDist } from "@/lib/dist";
import { fmtCompact, fmtDuration, fmtMs, fmtPct, titleCase } from "@/lib/format";
import type { RateProfile } from "@/lib/rate";
import { getIn, modelCapacity, parseYaml, setIn, toYaml, type Doc, type Path } from "@/lib/scenarioDoc";
import { TOPOLOGIES, type BudgetCheck } from "@/lib/types";

const FRAUD_KINDS = ["card_testing", "account_takeover", "lost_stolen", "counterfeit", "merchant_breach", "atm_cashout", "app_scam_mule", "bust_out", "bot_ecom"];
const DEFAULT_MIX: Record<string, number> = { card_testing: 0.22, account_takeover: 0.15, lost_stolen: 0.12, counterfeit: 0.10, merchant_breach: 0.08, atm_cashout: 0.03, app_scam_mule: 0.12, bust_out: 0.06, bot_ecom: 0.12 };
const TOPO_INFO: Record<string, string> = {
  "gateway-kafka": "Async rail: the gateway consumes events.raw; Flink keeps Redis features fresh off the hot path.",
  "gateway-grpc": "Sync rail (card auth style): gRPC Score() into the gateway, audit WAL acked before the reply.",
  "flink-inline": "Flink computes exact features in 7 keyed stages and scores via async I/O; every keyBy is a shuffle.",
  "echo-kafka": "Calibration: the gateway answers immediately, measuring the instrument and Kafka floor.",
  external: "Your own scorer as a black box: it consumes events.raw and publishes decisions; completeness, latency and the audit are measured.",
};

type Upd = (path: Path, v: unknown) => void;

function Num({ doc, upd, path, label, unit, step, hint, placeholder, min }: {
  doc: Doc; upd: Upd; path: Path; label: string; unit?: string; step?: number; hint?: string; placeholder?: string; min?: number;
}) {
  return <NumberField label={label} unit={unit} step={step} hint={hint} placeholder={placeholder} min={min}
    value={getIn<number>(doc, path)} onChange={(v) => upd(path, v)} />;
}

function Section({ title, icon, children, subtitle, className }: { title: string; icon?: React.ReactNode; subtitle?: string; children: React.ReactNode; className?: string }) {
  return (
    <Card className={className}>
      <CardHeader title={title} subtitle={subtitle} icon={icon} />
      <CardBody>{children}</CardBody>
    </Card>
  );
}

function TopologyCard({ t, selected, onClick }: { t: string; selected: boolean; onClick: () => void }) {
  const g = useTopology(t);
  return (
    <button onClick={onClick} className={cn("rounded-xl border p-3 text-left transition cursor-pointer", selected ? "border-accent/60 bg-accent/8 ring-1 ring-accent/30" : "border-border hover:border-border-strong")}>
      <div className="mb-1 flex items-center gap-2"><span className="text-[13.5px] font-semibold">{topologyLabel(t)}</span><span className="font-mono text-[11px] text-subtle">{t}</span>
        {selected && <CheckCircle2 className="ml-auto size-4 text-accent" />}</div>
      <p className="mb-2 text-[12px] leading-5 text-muted">{TOPO_INFO[t]}</p>
      <div className="pointer-events-none rounded-lg bg-surface-2/50 p-1"><TopologyMap graph={g.data ?? FALLBACK_TOPOLOGIES[t]} snapshots={[]} budgetMs={150} active={false} mini /></div>
    </button>
  );
}

function CampaignTimeline({ campaigns, total, warmup }: { campaigns: { kind: string; at_s?: number; count?: number }[]; total: number; warmup: number }) {
  return (
    <div className="relative mt-3 h-14 rounded-lg border border-border bg-surface-2/50">
      <div className="absolute inset-y-0 left-0 rounded-l-lg bg-warn/10" style={{ width: `${(warmup / total) * 100}%` }} />
      <span className="absolute bottom-1 left-2 text-[10px] text-subtle">warm-up</span>
      <span className="absolute bottom-1 right-2 text-[10px] text-subtle">{fmtDuration(total)}</span>
      {campaigns.map((c, i) => {
        const x = Math.min(100, ((c.at_s ?? 0) / total) * 100);
        return (
          <div key={i} className="absolute top-1.5 -translate-x-1/2" style={{ left: `${x}%` }} title={`${c.kind} ×${c.count ?? 1} at ${c.at_s ?? 0}s`}>
            <div className="mx-auto h-5 w-px bg-fail" />
            <span className="block whitespace-nowrap rounded bg-fail/15 px-1 font-mono text-[9.5px] text-fail">{c.kind.split("_")[0]}×{c.count ?? 1}</span>
          </div>
        );
      })}
    </div>
  );
}

function BudgetPanel({ b }: { b: BudgetCheck }) {
  const tone = b.verdict === "FEASIBLE" ? "pass" : b.verdict === "MARGINAL" ? "warn" : "fail";
  return (
    <div className="space-y-4 p-5">
      <div className="flex items-center gap-3"><Badge tone={tone} className="px-3 py-1 text-[13px] font-semibold">{b.verdict}</Badge>
        <span className="text-[12.5px] text-muted">{fmtCompact(b.samples)} Monte Carlo samples · budget {fmtMs(b.budget.e2e_p99_ms)} e2e / {fmtMs(b.budget.hot_path_p99_ms)} hot path</span></div>
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        {(["p50", "p90", "p99", "p99.9"] as const).map((k) => (
          <div key={k} className="rounded-xl border border-border p-3"><div className="text-[10.5px] uppercase tracking-wider text-subtle">e2e {k}</div><div className="mt-1 text-[16px] font-semibold num">{fmtMs(b.e2e?.[k])}</div></div>
        ))}
      </div>
      <div className="grid grid-cols-2 gap-3">
        <div className="rounded-xl border border-border p-3"><div className="text-[10.5px] uppercase tracking-wider text-subtle">within budget</div><div className="mt-1 text-[16px] font-semibold num">{fmtPct(b.within_budget, 2)}</div></div>
        <div className="rounded-xl border border-border p-3"><div className="text-[10.5px] uppercase tracking-wider text-subtle">predicted fallback</div><div className="mt-1 text-[16px] font-semibold num">{fmtPct(b.fallback_rate, 1)}</div></div>
      </div>
      <div>
        <div className="mb-2 text-[12px] font-semibold uppercase tracking-[0.1em] text-subtle">Tail share by stage</div>
        {Object.entries(b.tail_share ?? {}).sort((x, y) => y[1] - x[1]).map(([k, v]) => (
          <div key={k} className="grid grid-cols-[140px_1fr_48px] items-center gap-2 py-0.5 text-[12px]">
            <span className="truncate font-mono text-muted">{k}</span>
            <div className="h-1.5 rounded-full bg-surface-3"><div className="h-full rounded-full bg-accent" style={{ width: `${v * 100}%` }} /></div>
            <span className="text-right num">{fmtPct(v, 0)}</span>
          </div>
        ))}
      </div>
      {b.advice?.length > 0 && <ul className="list-disc space-y-1 pl-5 text-[13px] text-muted">{b.advice.map((a) => <li key={a}>{a}</li>)}</ul>}
    </div>
  );
}

export default function Scenarios() {
  const [params, setParams] = useSearchParams();
  const nav = useNavigate();
  const qc = useQueryClient();
  const list = useQuery({ queryKey: ["scenarios"], queryFn: api.scenarios });
  const name = params.get("name") || "smoke";
  const sc = useQuery({ queryKey: ["scenario", name], queryFn: () => api.scenario(name), retry: 0 });
  const [text, setText] = React.useState("");
  const [orig, setOrig] = React.useState("");
  const [doc, setDoc] = React.useState<Doc>({});
  const [parseErr, setParseErr] = React.useState<string | null>(null);
  const [errors, setErrors] = React.useState<string[]>([]);
  const [budget, setBudget] = React.useState<BudgetCheck | null>(null);
  const [dup, setDup] = React.useState<string | null>(null);
  const [tab, setTab] = React.useState("traffic");

  React.useEffect(() => {
    if (!sc.data) return;
    setText(sc.data.yaml); setOrig(sc.data.yaml); setErrors([]); setBudget(null);
    const p = parseYaml(sc.data.yaml);
    setDoc(p.doc ?? {}); setParseErr(p.error);
  }, [sc.data]);

  const upd: Upd = (path, v) => {
    const next = setIn(doc, path, v);
    setDoc(next); setText(toYaml(next)); setParseErr(null);
  };
  const onYaml = (t: string) => {
    setText(t);
    const p = parseYaml(t);
    setParseErr(p.error);
    if (p.doc) setDoc(p.doc);
  };
  const dirty = text !== orig;
  const docName = String(doc.name ?? name);

  const errList = (e: unknown) => {
    if (e instanceof ApiError && e.detail && typeof e.detail === "object" && Array.isArray((e.detail as { errors?: unknown[] }).errors)) {
      return ((e.detail as { errors: { loc: unknown[]; msg: string }[] }).errors).map((x) => `${x.loc.join(".")}: ${x.msg}`);
    }
    return [(e as Error).message];
  };
  const validate = useMutation({
    mutationFn: () => api.validate(text),
    onSuccess: (r) => { setErrors([]); toast.success("Scenario is valid", { description: `fingerprint ${r.fingerprint}` }); },
    onError: (e) => { setErrors(errList(e)); toast.error("Validation failed"); },
  });
  const check = useMutation({
    mutationFn: () => api.budget(docName, { yaml: text, samples: 50_000 }),
    onSuccess: (b) => { setErrors([]); setBudget(b); },
    onError: (e) => { setErrors(errList(e)); toast.error("Budget check failed"); },
  });
  const save = useMutation({
    mutationFn: (args: { name: string; yaml: string }) => api.saveScenario(args.name, args.yaml),
    onSuccess: (_r, args) => {
      toast.success(`Saved ${args.name}`);
      setOrig(args.yaml); setErrors([]);
      qc.invalidateQueries({ queryKey: ["scenarios"] });
      qc.invalidateQueries({ queryKey: ["scenario", args.name] });
      if (args.name !== name) setParams({ name: args.name });
    },
    onError: (e) => { setErrors(errList(e)); toast.error("Save failed", { description: (e as Error).message }); },
  });

  const current = list.data?.find((s) => s.name === name);
  const reset = useMutation({
    mutationFn: () => api.resetScenario(name),
    onSuccess: () => {
      toast.success(`Reset ${name} to the bundled version`, { description: "Your edit is kept in the scenario directory's .backup/." });
      qc.invalidateQueries({ queryKey: ["scenarios"] });
      qc.invalidateQueries({ queryKey: ["scenario", name] });
    },
    onError: (e) => toast.error("Reset failed", { description: (e as Error).message }),
  });

  const traffic = (getIn<Doc>(doc, ["traffic"]) ?? {}) as Doc;
  const duration = Number(traffic.duration_s ?? 60);
  const warmup = Number(traffic.warmup_s ?? 10);
  const profile = (getIn<RateProfile>(doc, ["traffic", "profile"]) ?? { kind: "constant", tps: 1000 }) as RateProfile;
  const topology = String(doc.topology ?? "gateway-kafka");
  const mix = { ...DEFAULT_MIX, ...(getIn<Record<string, number>>(doc, ["fraud", "mix"]) ?? {}) };
  const mixTotal = Object.values(mix).reduce((a, b) => a + (b || 0), 0) || 1;
  const campaigns = (getIn<{ kind: string; at_s?: number; count?: number }[]>(doc, ["fraud", "campaigns"]) ?? []);
  const svc = getIn<LatencyDist>(doc, ["model_server", "service_time"]);
  const svcP50 = React.useMemo(() => preview(svc, 4000)?.p50 ?? null, [svc]);
  const cap = modelCapacity(getIn<number>(doc, ["model_server", "workers"]), getIn<number>(doc, ["model_server", "max_batch"]), svcP50, getIn<number>(doc, ["model_server", "per_item_us"]));
  const peakTps = Math.max(profile.tps ?? 0, profile.end_tps ?? 0);

  return (
    <>
      <PageHeader eyebrow="Scenarios" title="Scenario builder"
        subtitle="Every form field writes the scenario YAML and every YAML edit updates the form. Previews use the same maths as the engine."
        actions={<>
          <Button variant="outline" onClick={() => setDup(`${docName}-copy`)}><Copy />Duplicate</Button>
          <Button variant="secondary" onClick={() => validate.mutate()} disabled={validate.isPending || !!parseErr}>{validate.isPending ? <Loader2 className="animate-spin" /> : <CheckCircle2 />}Validate</Button>
          <Button variant="secondary" onClick={() => check.mutate()} disabled={check.isPending || !!parseErr}>{check.isPending ? <Loader2 className="animate-spin" /> : <Sparkles />}Budget check</Button>
          <Button variant="gradient" onClick={() => save.mutate({ name: docName, yaml: text })} disabled={!dirty || save.isPending || !!parseErr} data-testid="save-scenario">
            {save.isPending ? <Loader2 className="animate-spin" /> : <Save />}Save
          </Button>
        </>} />

      <div className="grid grid-cols-1 gap-5 xl:grid-cols-[260px_minmax(0,1fr)]">
        <Card className="self-start xl:sticky xl:top-20">
          <CardHeader title="Scenarios" subtitle={`${list.data?.length ?? "…"} files in scenarios/`} />
          <div className="max-h-[60vh] overflow-y-auto px-2 pb-3">
            {list.isLoading ? <div className="space-y-1.5 px-3">{Array.from({ length: 10 }, (_, i) => <Skeleton key={i} className="h-8" />)}</div> : (list.data ?? []).map((s) => (
              <button key={s.name} onClick={() => setParams({ name: s.name })}
                className={cn("flex w-full items-center gap-2 rounded-lg px-3 py-1.5 text-left text-[12.5px] cursor-pointer", s.name === name ? "bg-accent/12 text-fg" : "text-muted hover:bg-surface-2 hover:text-fg")}>
                <span className="truncate font-mono">{s.name}</span>
                {s.update_available ? <span className="size-1.5 shrink-0 rounded-full bg-info" title="the image has a newer version of this bundled scenario" />
                  : s.edited ? <span className="size-1.5 shrink-0 rounded-full bg-warn" title="edited (kept across upgrades)" /> : null}
                <span className="ml-auto shrink-0 text-[10.5px] text-subtle">{fmtCompact(s.tps)}</span>
              </button>
            ))}
          </div>
        </Card>

        <div className="min-w-0">
          {sc.error && <ErrorState error={sc.error} />}
          <div className="mb-4 flex flex-wrap items-center gap-2 rounded-xl border border-border bg-surface px-4 py-3">
            <FileCode2 className="size-4 text-subtle" />
            <span className="font-mono text-[14px] font-medium">{docName}.yaml</span>
            <TopologyBadge topology={topology} />
            {dirty ? <Badge tone="warn">unsaved changes</Badge> : <Badge>saved</Badge>}
            {current?.bundled && (current.update_available
              ? <Badge tone="info" title="you edited this bundled scenario; the image now ships a newer version">newer version available</Badge>
              : current.edited ? <Badge tone="neutral" title="kept as you edited it when the image is upgraded">edited</Badge> : <Badge tone="neutral">bundled</Badge>)}
            {current?.bundled && current.edited && (
              <Button size="sm" variant="ghost" onClick={() => reset.mutate()} disabled={reset.isPending}
                title="restore the version this image ships (your edit is kept in .backup/)"><RotateCcw />Reset to bundled</Button>
            )}
            {parseErr && <Badge tone="fail"><AlertTriangle />YAML error</Badge>}
            <span className="ml-auto text-[12px] text-subtle num">{fmtCompact(peakTps)} TPS peak · {fmtDuration(duration)} of traffic, warm-up {fmtDuration(warmup)} of it</span>
            <Button size="sm" variant="ghost" onClick={() => nav(`/launch?scenario=${encodeURIComponent(name)}`)} disabled={dirty}><Rocket />Launch</Button>
          </div>
          {errors.length > 0 && (
            <div className="mb-4 rounded-xl border border-fail/25 bg-fail/5 px-4 py-3 text-[12.5px]">
              <div className="mb-1 font-medium text-fail">The scenario does not validate</div>
              <ul className="list-disc space-y-0.5 pl-5 font-mono text-[11.5px] text-muted">{errors.map((e) => <li key={e}>{e}</li>)}</ul>
            </div>
          )}

          {sc.isLoading ? <Skeleton className="h-96 rounded-2xl" /> : (
            <Tabs value={tab} onValueChange={setTab}>
              <TabsList className="max-w-full overflow-x-auto">
                <TabsTrigger value="traffic"><Waves className="size-3.5" />Traffic</TabsTrigger>
                <TabsTrigger value="world"><Globe2 className="size-3.5" />World</TabsTrigger>
                <TabsTrigger value="topology"><Network className="size-3.5" />Topology</TabsTrigger>
                <TabsTrigger value="scoring"><Timer className="size-3.5" />Scoring</TabsTrigger>
                <TabsTrigger value="model"><Cpu className="size-3.5" />Model</TabsTrigger>
                <TabsTrigger value="fraud"><ShieldAlert className="size-3.5" />Fraud</TabsTrigger>
                <TabsTrigger value="budget"><Target className="size-3.5" />Budget</TabsTrigger>
                <TabsTrigger value="yaml"><FileCode2 className="size-3.5" />YAML</TabsTrigger>
              </TabsList>

              <TabsContent value="traffic" className="mt-4 grid grid-cols-1 gap-5 2xl:grid-cols-[minmax(0,1.4fr)_minmax(0,1fr)]">
                <Section title="Rate profile" subtitle="open-loop target rate over the measured window (RateProfile.rate_at)">
                  <RateProfileEditor value={profile} duration={duration} onChange={(p) => upd(["traffic", "profile"], p)} />
                </Section>
                <Section title="Run shape">
                  <div className="grid grid-cols-2 gap-3">
                    <Num doc={doc} upd={upd} path={["traffic", "duration_s"]} label="Duration" unit="s" placeholder="60" />
                    <Num doc={doc} upd={upd} path={["traffic", "warmup_s"]} label="Warm-up" unit="s" placeholder="10" />
                    <Label>Arrivals
                      <Select value={String(traffic.arrivals ?? "uniform")} onChange={(v) => upd(["traffic", "arrivals"], v === "uniform" ? undefined : v)}
                        options={[{ value: "uniform", label: "uniform (paced)" }, { value: "poisson", label: "Poisson" }]} />
                    </Label>
                    <Num doc={doc} upd={upd} path={["traffic", "generators"]} label="Generators" hint="emitter processes" placeholder="1" min={1} />
                    <Num doc={doc} upd={upd} path={["traffic", "backfill_days"]} label="Backfill" unit="days" step={0.5} hint="unpaced history first" placeholder="0" />
                    <Label hint="sim seconds per wall second, or auto">Time scale
                      <Input value={String(traffic.time_scale ?? "")} placeholder="auto" onChange={(e) => upd(["traffic", "time_scale"], e.target.value === "" ? undefined : e.target.value === "auto" ? "auto" : Number(e.target.value))} />
                    </Label>
                    <Label className="col-span-2">Simulated start
                      <Input value={String(traffic.sim_start ?? "")} placeholder="2026-11-02T00:00:00Z" onChange={(e) => upd(["traffic", "sim_start"], e.target.value || undefined)} />
                    </Label>
                  </div>
                </Section>
              </TabsContent>

              <TabsContent value="world" className="mt-4">
                <Section title="Synthetic world" subtitle="deterministic population; a world is cached per (customers, seed)">
                  <div className="grid grid-cols-2 gap-3 md:grid-cols-3">
                    <Num doc={doc} upd={upd} path={["seed"]} label="Seed" placeholder="42" />
                    <Num doc={doc} upd={upd} path={["world", "customers"]} label="Customers" placeholder="200000" />
                    <Num doc={doc} upd={upd} path={["world", "merchants"]} label="Merchants" placeholder="20000" />
                    <Num doc={doc} upd={upd} path={["world", "payees"]} label="Payees" placeholder="50000" />
                    <Num doc={doc} upd={upd} path={["world", "mule_fraction"]} label="Mule fraction" step={0.001} placeholder="0.002" />
                    <Num doc={doc} upd={upd} path={["feature_sample_permille"]} label="Feature sample" unit="‰" hint="decisions carrying the full vector" placeholder="1000" />
                  </div>
                  <Label className="mt-4">Description
                    <Textarea rows={3} value={String(doc.description ?? "")} onChange={(e) => upd(["description"], e.target.value || undefined)} />
                  </Label>
                </Section>
              </TabsContent>

              <TabsContent value="topology" className="mt-4 space-y-5">
                <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
                  {TOPOLOGIES.map((t) => <TopologyCard key={t} t={t} selected={topology === t} onClick={() => upd(["topology"], t)} />)}
                </div>
                <div className="grid grid-cols-1 gap-5 lg:grid-cols-2">
                  <Section title="Pipeline options">
                    <div className="grid grid-cols-2 gap-3">
                      <Label>Rule set<Input value={String(doc.rules ?? "")} placeholder="baseline" onChange={(e) => upd(["rules"], e.target.value || undefined)} /></Label>
                      <Label>Flink state
                        <Select value={String(doc.flink_state ?? "warm")} onChange={(v) => upd(["flink_state"], v === "warm" ? undefined : v)}
                          options={[{ value: "warm", label: "warm (keep state)" }, { value: "fresh", label: "fresh (exact verify)" }]} />
                      </Label>
                    </div>
                    <Switch className="mt-4" checked={Boolean(doc.exactly_once)} onCheckedChange={(v) => upd(["exactly_once"], v || undefined)} label="Exactly-once decisions sink (read_committed collector)" />
                  </Section>
                  <Section title="Sync rail" subtitle="used by gateway-grpc">
                    <div className="grid grid-cols-2 gap-3">
                      <Label>Mode<Select value={String(getIn(doc, ["sync", "mode"]) ?? "unary")} onChange={(v) => upd(["sync", "mode"], v === "unary" ? undefined : v)}
                        options={[{ value: "unary", label: "unary" }, { value: "stream", label: "bidi stream" }]} /></Label>
                      <Num doc={doc} upd={upd} path={["sync", "channels"]} label="Channels" placeholder="4" />
                      <Num doc={doc} upd={upd} path={["sync", "deadline_ms"]} label="Request deadline" unit="ms" hint="0 = gateway budget" placeholder="0" />
                    </div>
                  </Section>
                </div>
              </TabsContent>

              <TabsContent value="scoring" className="mt-4 grid grid-cols-1 gap-5 lg:grid-cols-2">
                <Section title="Deadlines" subtitle="deadline-aware orchestrator (ScoringConfig)">
                  <div className="grid grid-cols-2 gap-3">
                    <Num doc={doc} upd={upd} path={["scoring", "budget_ms"]} label="Hot-path budget" unit="ms" />
                    <Num doc={doc} upd={upd} path={["scoring", "feature_deadline_ms"]} label="Feature deadline" unit="ms" />
                    <Num doc={doc} upd={upd} path={["scoring", "model_deadline_ms"]} label="Model deadline" unit="ms" />
                    <Label>On deadline
                      <Select value={String(getIn(doc, ["scoring", "on_deadline"]) ?? "FALLBACK_RULES")} onChange={(v) => upd(["scoring", "on_deadline"], v === "FALLBACK_RULES" ? undefined : v)}
                        options={[{ value: "FALLBACK_RULES", label: "rules-only fallback" }, { value: "TIMEOUT", label: "time out" }, { value: "PROVISIONAL_THEN_ASYNC", label: "provisional, then final" }]} />
                    </Label>
                    <Num doc={doc} upd={upd} path={["scoring", "hedge_after_ms"]} label="Hedge after" unit="ms" hint="0 = off" />
                    <Label>Feature source
                      <Select value={String(getIn(doc, ["scoring", "feature_source"]) ?? "redis")} onChange={(v) => upd(["scoring", "feature_source"], v)}
                        options={[{ value: "redis", label: "Redis online store" }, { value: "mock", label: "mock" }]} />
                    </Label>
                    <Num doc={doc} upd={upd} path={["scoring", "microbatch_window_us"]} label="Micro-batch window" unit="µs" hint="0 = one RPC per event" />
                    <Num doc={doc} upd={upd} path={["scoring", "microbatch_max"]} label="Micro-batch max" />
                    <Num doc={doc} upd={upd} path={["scoring", "review_threshold"]} label="Review threshold" step={0.01} />
                    <Num doc={doc} upd={upd} path={["scoring", "decline_threshold"]} label="Decline threshold" step={0.01} />
                  </div>
                  <Switch className="mt-4" checked={Boolean(getIn(doc, ["scoring", "self_inclusion"]))} onCheckedChange={(v) => upd(["scoring", "self_inclusion"], v || undefined)} label="Self-inclusion (add the current event to online aggregates)" />
                </Section>
                <div className="space-y-5">
                  <Section title="Injected feature-lookup delay">
                    <DistributionEditor value={getIn(doc, ["scoring", "feature_lookup_delay"])} onChange={(d) => upd(["scoring", "feature_lookup_delay"], d ?? undefined)} compact />
                  </Section>
                  <Section title="Extra client-side model delay">
                    <DistributionEditor value={getIn(doc, ["scoring", "model_extra_delay"])} onChange={(d) => upd(["scoring", "model_extra_delay"], d ?? undefined)} compact />
                  </Section>
                </div>
              </TabsContent>

              <TabsContent value="model" className="mt-4 grid grid-cols-1 gap-5 lg:grid-cols-[minmax(0,1.3fr)_minmax(0,1fr)]">
                <Section title="Service time per batch" subtitle="mock KServe v2 server: a capacity model, not just a sleep">
                  <DistributionEditor value={svc} onChange={(d) => upd(["model_server", "service_time"], d ?? undefined)} allowNone />
                </Section>
                <Section title="Capacity">
                  <div className="grid grid-cols-2 gap-3">
                    <Num doc={doc} upd={upd} path={["model_server", "workers"]} label="Workers" hint="concurrent batches; 0 = ∞" />
                    <Num doc={doc} upd={upd} path={["model_server", "max_batch"]} label="Max batch" hint="1 = off" />
                    <Num doc={doc} upd={upd} path={["model_server", "max_queue_delay_us"]} label="Batch wait" unit="µs" />
                    <Num doc={doc} upd={upd} path={["model_server", "max_queue"]} label="Queue limit" hint="0 = unbounded" />
                    <Num doc={doc} upd={upd} path={["model_server", "per_item_us"]} label="Per-item compute" unit="µs" />
                    <Label>Model version<Input value={String(getIn(doc, ["model_server", "model_version"]) ?? "")} placeholder="default" onChange={(e) => upd(["model_server", "model_version"], e.target.value || undefined)} /></Label>
                  </div>
                  <div className={cn("mt-4 rounded-xl border p-3 text-[12.5px]", cap === null ? "border-border" : cap < peakTps ? "border-fail/30 bg-fail/5" : cap < peakTps * 1.5 ? "border-warn/30 bg-warn/5" : "border-pass/30 bg-pass/5")}>
                    <div className="text-[10.5px] uppercase tracking-wider text-subtle">estimated capacity</div>
                    <div className="mt-1 text-[18px] font-semibold num">{cap === null ? "unbounded" : `${fmtCompact(cap)} inferences/s`}</div>
                    <div className="text-muted">{cap === null ? "workers = 0: no queueing modelled" : `${fmtPct(peakTps / cap, 0)} utilisation at the ${fmtCompact(peakTps)} TPS peak (p50 service ${fmtMs(svcP50)})`}</div>
                  </div>
                </Section>
              </TabsContent>

              <TabsContent value="fraud" className="mt-4 grid grid-cols-1 gap-5 lg:grid-cols-2">
                <Section title="Background fraud mix" subtitle="relative weights of campaign typologies (normalised)">
                  <Num doc={doc} upd={upd} path={["fraud", "background_per_1k"]} label="Campaigns per 1,000 legit events" step={0.1} placeholder="0.5" />
                  <div className="mt-4 space-y-3">
                    {FRAUD_KINDS.map((k) => (
                      <div key={k} className="grid grid-cols-[130px_1fr_52px] items-center gap-3 text-[12.5px]">
                        <span className="truncate text-muted">{titleCase(k)}</span>
                        <Slider value={mix[k] ?? 0} min={0} max={0.5} step={0.01} onValueChange={(v) => upd(["fraud", "mix"], { ...mix, [k]: Math.round(v * 100) / 100 })} ariaLabel={k} />
                        <span className="text-right num">{fmtPct((mix[k] ?? 0) / mixTotal, 0)}</span>
                      </div>
                    ))}
                  </div>
                  <div className="mt-4 grid grid-cols-2 gap-3">
                    <Num doc={doc} upd={upd} path={["fraud", "unreported_fraction"]} label="Unreported fraction" step={0.01} placeholder="0.10" />
                    <Num doc={doc} upd={upd} path={["fraud", "friendly_fraud_per_1k"]} label="Friendly fraud / 1k" step={0.05} placeholder="0.2" />
                  </div>
                </Section>
                <Section title="Scheduled campaigns" subtitle="extra campaigns at fixed wall-clock offsets">
                  <div className="space-y-2">
                    {campaigns.map((c, i) => (
                      <div key={i} className="grid grid-cols-[1fr_90px_70px_32px] items-end gap-2">
                        <Label>Kind<Select value={c.kind} onChange={(v) => upd(["fraud", "campaigns", i, "kind"], v)} options={FRAUD_KINDS.map((k) => ({ value: k, label: titleCase(k) }))} /></Label>
                        <NumberField label="At" unit="s" value={c.at_s} onChange={(v) => upd(["fraud", "campaigns", i, "at_s"], v)} />
                        <NumberField label="Count" value={c.count} onChange={(v) => upd(["fraud", "campaigns", i, "count"], v)} />
                        <Button variant="ghost" size="icon-sm" className="mb-0.5" aria-label="Remove campaign"
                          onClick={() => upd(["fraud", "campaigns"], campaigns.filter((_, j) => j !== i).length ? campaigns.filter((_, j) => j !== i) : undefined)}><Trash2 /></Button>
                      </div>
                    ))}
                  </div>
                  <Button variant="outline" size="sm" className="mt-3" onClick={() => upd(["fraud", "campaigns"], [...campaigns, { kind: "card_testing", at_s: Math.round(warmup + duration / 2), count: 1 }])}><Plus />Add campaign</Button>
                  <CampaignTimeline campaigns={campaigns} total={duration} warmup={warmup} />
                </Section>
              </TabsContent>

              <TabsContent value="budget" className="mt-4 grid grid-cols-1 gap-5 lg:grid-cols-2">
                <Section title="Latency budget" subtitle="the verdict is PASS only if every budget holds">
                  <div className="grid grid-cols-2 gap-3">
                    <Num doc={doc} upd={upd} path={["budget", "e2e_p99_ms"]} label="e2e p99" unit="ms" placeholder="150" />
                    <Num doc={doc} upd={upd} path={["budget", "hot_path_p99_ms"]} label="Hot path p99" unit="ms" placeholder="100" />
                    <Num doc={doc} upd={upd} path={["budget", "min_compliance"]} label="Min compliance" step={0.001} hint="fraction within e2e budget" placeholder="0.99" />
                    <Label>Expected verdict
                      <Select value={String(getIn(doc, ["expect", "verdict"]) ?? "")} onChange={(v) => upd(["expect", "verdict"], v || undefined)}
                        options={[{ value: "", label: "none" }, { value: "PASS", label: "PASS" }, { value: "FAIL", label: "FAIL (negative test)" }]} />
                    </Label>
                  </div>
                </Section>
                <Section title="Guardrails" subtitle="abort early instead of burning the environment">
                  <div className="grid grid-cols-2 gap-3">
                    <Num doc={doc} upd={upd} path={["guardrails", "max_consumer_lag"]} label="Max consumer lag" placeholder="500000" />
                    <Num doc={doc} upd={upd} path={["guardrails", "max_error_rate"]} label="Max error rate" step={0.01} placeholder="0.05" />
                    <Num doc={doc} upd={upd} path={["guardrails", "max_schedule_lag_ms"]} label="Max schedule lag" unit="ms" placeholder="2000" />
                    <Num doc={doc} upd={upd} path={["guardrails", "max_missing_fraction"]} label="Max missing" step={0.001} placeholder="0.01" />
                  </div>
                </Section>
              </TabsContent>

              <TabsContent value="yaml" className="mt-4">
                <Card className="overflow-hidden">
                  <div className="flex items-center gap-2 border-b border-border px-4 py-2 text-[12px] text-subtle">
                    <FileCode2 className="size-3.5" />scenarios/{docName}.yaml
                    <span className="ml-auto">form edits re-emit the YAML (comments are not preserved)</span>
                  </div>
                  <Textarea value={text} onChange={(e) => onYaml(e.target.value)} spellCheck={false} data-testid="yaml-editor"
                    className="min-h-[520px] rounded-none border-0 bg-surface-2/40 font-mono text-[12.5px] leading-6 focus:ring-0" />
                  {parseErr && <div className="border-t border-fail/25 bg-fail/5 px-4 py-2 font-mono text-[12px] text-fail">{parseErr}</div>}
                </Card>
              </TabsContent>
            </Tabs>
          )}
        </div>
      </div>

      <Dialog open={!!budget} onOpenChange={(o) => !o && setBudget(null)}>
        <DialogContent title="Budget feasibility" description={`${docName} · ${topologyLabel(topology)}`} className="max-w-2xl" aria-describedby={undefined}>
          {budget && <BudgetPanel b={budget} />}
        </DialogContent>
      </Dialog>
      <Dialog open={dup !== null} onOpenChange={(o) => !o && setDup(null)}>
        <DialogContent title="Duplicate scenario" description="Saves a copy under a new name in scenarios/." aria-describedby={undefined}>
          <div className="space-y-4 p-5">
            <Label>New name<Input value={dup ?? ""} onChange={(e) => setDup(e.target.value.replace(/[^a-z0-9._-]/gi, "-"))} autoFocus /></Label>
            <div className="flex justify-end gap-2">
              <Button variant="ghost" onClick={() => setDup(null)}>Cancel</Button>
              <Button variant="gradient" disabled={!dup || save.isPending} onClick={() => {
                const next = setIn(doc, ["name"], dup!);
                save.mutate({ name: dup!, yaml: toYaml(next) });
                setDup(null);
              }}><Copy />Create copy</Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </>
  );
}
