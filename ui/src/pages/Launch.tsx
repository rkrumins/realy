import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { CalendarClock, Clock, FlaskConical, Gauge, Loader2, Rocket, Search, ShieldAlert, Sparkles, X } from "lucide-react";
import * as React from "react";
import { useNavigate, useSearchParams } from "react-router";
import { toast } from "sonner";
import { EmptyState, ErrorState, PageHeader, StatusBadge, TopologyBadge, topologyLabel } from "@/components/common";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardBody, CardHeader } from "@/components/ui/card";
import { Segmented, Skeleton, Switch } from "@/components/ui/controls";
import { Input, Label, NumberField } from "@/components/ui/input";
import { Select } from "@/components/ui/select";
import { api } from "@/lib/api";
import { cn } from "@/lib/cn";
import { fmtAgo, fmtCompact, fmtDuration, fmtMs, fmtPct } from "@/lib/format";
import { useStatus } from "@/lib/hooks";
import { TOPOLOGIES, type BudgetCheck, type ScenarioListItem } from "@/lib/types";

export const FRAUD_PACKS = new Set([
  "everyday-baseline", "mixed-realistic", "card-testing-wave", "atm-cashout-unlimited", "impossible-travel", "ato-campaign",
  "app-scam-mules", "merchant-breach", "black-friday",
]);

function ScenarioCard({ s, selected, onClick }: { s: ScenarioListItem; selected: boolean; onClick: () => void }) {
  return (
    <button onClick={onClick} data-testid={`scenario-${s.name}`}
      className={cn(
        "group flex h-full flex-col rounded-xl border p-3.5 text-left transition cursor-pointer",
        selected ? "border-accent/60 bg-accent/8 ring-1 ring-accent/30" : "border-border bg-surface hover:border-border-strong hover:bg-surface-2/50",
      )}>
      <div className="flex items-center gap-2">
        <span className="truncate font-mono text-[13px] font-medium text-fg">{s.name}</span>
        <span className="ml-auto shrink-0"><TopologyBadge topology={s.topology} /></span>
      </div>
      <p className="mt-1.5 line-clamp-2 text-[12.5px] leading-5 text-muted">{s.description || "—"}</p>
      <div className="mt-auto flex gap-3 pt-2.5 text-[11.5px] text-subtle num">
        <span className="inline-flex items-center gap-1"><Gauge className="size-3" />{fmtCompact(s.tps)} TPS</span>
        <span className="inline-flex items-center gap-1"><Clock className="size-3" />{fmtDuration(s.duration_s)}</span>
      </div>
    </button>
  );
}

function BudgetResult({ b }: { b: BudgetCheck }) {
  const tone = b.verdict === "FEASIBLE" ? "pass" : b.verdict === "MARGINAL" ? "warn" : "fail";
  return (
    <div className="mt-3 rounded-xl border border-border bg-surface-2/50 p-3 text-[12.5px]">
      <div className="flex items-center gap-2">
        <Badge tone={tone} className="font-semibold">{b.verdict}</Badge>
        <span className="text-muted">Monte Carlo, {fmtCompact(b.samples)} samples</span>
      </div>
      <div className="mt-2 grid grid-cols-3 gap-2 text-center">
        <div><div className="text-[10.5px] uppercase tracking-wider text-subtle">e2e p99</div><div className="font-semibold num">{fmtMs(b.e2e?.p99)}</div></div>
        <div><div className="text-[10.5px] uppercase tracking-wider text-subtle">within budget</div><div className="font-semibold num">{fmtPct(b.within_budget, 2)}</div></div>
        <div><div className="text-[10.5px] uppercase tracking-wider text-subtle">fallback</div><div className="font-semibold num">{fmtPct(b.fallback_rate, 1)}</div></div>
      </div>
      {b.advice?.length > 0 && <ul className="mt-2 list-disc space-y-0.5 pl-4 text-muted">{b.advice.slice(0, 3).map((a) => <li key={a}>{a}</li>)}</ul>}
    </div>
  );
}

export default function Launch() {
  const [params, setParams] = useSearchParams();
  const nav = useNavigate();
  const qc = useQueryClient();
  const status = useStatus();
  const scenarios = useQuery({ queryKey: ["scenarios"], queryFn: api.scenarios });
  const [q, setQ] = React.useState("");
  const selectedName = params.get("scenario") || "smoke";
  const selected = scenarios.data?.find((s) => s.name === selectedName) ?? scenarios.data?.[0];

  const [tps, setTps] = React.useState<number | undefined>();
  const [duration, setDuration] = React.useState<number | undefined>();
  const [warmup, setWarmup] = React.useState<number | undefined>();
  const [topology, setTopology] = React.useState("");
  const [verify, setVerify] = React.useState(false);
  const [when, setWhen] = React.useState<"now" | "at" | "every">("now");
  const [at, setAt] = React.useState("");
  const [every, setEvery] = React.useState<number | undefined>(60);
  const [count, setCount] = React.useState<number | undefined>();
  const [budget, setBudget] = React.useState<BudgetCheck | null>(null);

  React.useEffect(() => {
    setTps(undefined); setDuration(undefined); setWarmup(undefined); setTopology(""); setBudget(null);
  }, [selected?.name]);

  const overrides = React.useMemo(() => {
    const o: Record<string, unknown> = {};
    const traffic: Record<string, unknown> = {};
    if (tps !== undefined) traffic.profile = { tps };
    if (duration !== undefined) traffic.duration_s = duration;
    if (warmup !== undefined) traffic.warmup_s = warmup;
    if (Object.keys(traffic).length) o.traffic = traffic;
    if (topology) o.topology = topology;
    return o;
  }, [tps, duration, warmup, topology]);

  const launch = useMutation({
    mutationFn: async () => {
      if (!selected) throw new Error("pick a scenario");
      if (when === "now") return { kind: "job" as const, r: await api.startRun(selected.name, overrides, verify) };
      const body = {
        scenario: selected.name, overrides, verify,
        at: when === "at" && at ? new Date(at).toISOString() : undefined,
        every_minutes: when === "every" ? every : undefined, count: when === "every" ? count : undefined,
      };
      return { kind: "schedule" as const, r: await api.addSchedule(body) };
    },
    onSuccess: (res) => {
      qc.invalidateQueries({ queryKey: ["runs"] });
      qc.invalidateQueries({ queryKey: ["schedules"] });
      if (res.kind === "job") {
        toast.success(`Queued ${selected?.name}`, { description: status?.current ? "It starts when the current run finishes." : "Starting now — opening the live view." });
        nav("/live");
      } else {
        toast.success(`Scheduled ${selected?.name}`);
        nav("/runs?tab=schedules");
      }
    },
    onError: (e) => toast.error("Could not launch", { description: (e as Error).message }),
  });
  const check = useMutation({
    mutationFn: () => api.budget(selected!.name, { overrides, samples: 50_000 }),
    onSuccess: setBudget,
    onError: (e) => toast.error("Budget check failed", { description: (e as Error).message }),
  });

  const filtered = (scenarios.data ?? []).filter((s) => !q || `${s.name} ${s.description} ${s.topology}`.toLowerCase().includes(q.toLowerCase()));
  const groups = [
    { title: "Fraud packs", hint: "realistic payment mixes and fraud typologies", icon: <ShieldAlert className="size-4" />, items: filtered.filter((s) => FRAUD_PACKS.has(s.name)) },
    { title: "Performance packs", hint: "latency, capacity and topology experiments", icon: <FlaskConical className="size-4" />, items: filtered.filter((s) => !FRAUD_PACKS.has(s.name)) },
  ];
  const queue = status?.queue ?? [];

  return (
    <>
      <PageHeader eyebrow="Launch" title="Launch a benchmark run"
        subtitle="Pick a scenario, adjust the load, check the latency budget is feasible, then run it now or on a schedule." />
      <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_400px]">
        <div className="min-w-0">
          <div className="relative mb-4 max-w-md">
            <Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-subtle" />
            <Input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Filter scenarios…" className="pl-9" />
          </div>
          {scenarios.error && <ErrorState error={scenarios.error} />}
          {scenarios.isLoading && <div className="grid gap-3 sm:grid-cols-2 2xl:grid-cols-3">{Array.from({ length: 9 }, (_, i) => <Skeleton key={i} className="h-[108px] rounded-xl" />)}</div>}
          {groups.map((g) => g.items.length > 0 && (
            <section key={g.title} className="mb-7">
              <div className="mb-3 flex items-baseline gap-2">
                <span className="text-accent">{g.icon}</span>
                <h2 className="text-[14px] font-semibold text-fg">{g.title}</h2>
                <span className="text-[12.5px] text-subtle">{g.hint}</span>
              </div>
              <div className="grid gap-3 sm:grid-cols-2 2xl:grid-cols-3">
                {g.items.map((s) => (
                  <ScenarioCard key={s.name} s={s} selected={s.name === selected?.name}
                    onClick={() => setParams((p) => { p.set("scenario", s.name); return p; }, { replace: true })} />
                ))}
              </div>
            </section>
          ))}
          {scenarios.data && filtered.length === 0 && <EmptyState title="No scenario matches" icon={<Search />}>Try another filter.</EmptyState>}
        </div>

        <div className="xl:sticky xl:top-20 xl:self-start">
          <Card className="overflow-hidden">
            <div className="h-1 bg-[linear-gradient(90deg,#6366f1,#8b5cf6,#22d3ee)]" />
            <CardHeader title={selected ? <span className="font-mono">{selected.name}</span> : "Select a scenario"}
              subtitle={selected ? `${topologyLabel(selected.topology)} · ${fmtCompact(selected.tps)} TPS · ${fmtDuration(selected.duration_s)}` : undefined}
              icon={<Rocket />} />
            {selected && (
              <CardBody className="space-y-4">
                <div className="grid grid-cols-2 gap-3">
                  <NumberField label="Rate" unit="TPS" value={tps} onChange={setTps} placeholder={String(selected.tps ?? "")} min={1} />
                  <NumberField label="Duration" unit="s" value={duration} onChange={setDuration} placeholder={String(selected.duration_s ?? "")} min={5} />
                  <NumberField label="Warm-up" unit="s" value={warmup} onChange={setWarmup} placeholder="default" min={0} />
                  <Label>Topology
                    <Select value={topology} onChange={setTopology} ariaLabel="Topology override"
                      options={[{ value: "", label: `${topologyLabel(selected.topology)} (scenario)` }, ...TOPOLOGIES.map((t) => ({ value: t, label: topologyLabel(t), hint: t }))]} />
                  </Label>
                </div>
                <Switch checked={verify} onCheckedChange={setVerify} label="Verify correctness after the run" />
                <div>
                  <div className="mb-1.5 text-[12.5px] font-medium text-muted">When</div>
                  <Segmented value={when} onChange={setWhen} options={[
                    { value: "now", label: "Run now" }, { value: "at", label: "At a time" }, { value: "every", label: "Recurring" },
                  ]} />
                  {when === "at" && <Input type="datetime-local" className="mt-2" value={at} onChange={(e) => setAt(e.target.value)} />}
                  {when === "every" && (
                    <div className="mt-2 grid grid-cols-2 gap-3">
                      <NumberField label="Every" unit="min" value={every} onChange={setEvery} min={1} />
                      <NumberField label="Times" value={count} onChange={setCount} placeholder="forever" min={1} />
                    </div>
                  )}
                </div>
                {Object.keys(overrides).length > 0 && (
                  <div className="flex items-start gap-2 rounded-lg bg-surface-2/70 px-3 py-2 font-mono text-[11.5px] text-muted">
                    <span className="flex-1 break-all">overrides {JSON.stringify(overrides)}</span>
                    <button aria-label="Clear overrides" className="text-subtle hover:text-fg cursor-pointer" onClick={() => { setTps(undefined); setDuration(undefined); setWarmup(undefined); setTopology(""); }}><X className="size-3.5" /></button>
                  </div>
                )}
                <div className="flex gap-2">
                  <Button variant="secondary" onClick={() => check.mutate()} disabled={check.isPending} className="flex-1">
                    {check.isPending ? <Loader2 className="animate-spin" /> : <Sparkles />}Budget check
                  </Button>
                  <Button variant="gradient" onClick={() => launch.mutate()} disabled={launch.isPending} className="flex-[1.4]" data-testid="launch-button">
                    {launch.isPending ? <Loader2 className="animate-spin" /> : when === "now" ? <Rocket /> : <CalendarClock />}
                    {when === "now" ? (status?.current ? "Queue run" : "Launch run") : "Schedule"}
                  </Button>
                </div>
                {budget && <BudgetResult b={budget} />}
              </CardBody>
            )}
          </Card>
          <Card className="mt-4">
            <CardHeader title="Queue" subtitle={status?.current ? `running ${status.current.scenario}` : "nothing running"} />
            <CardBody className="pt-0">
              {queue.length === 0 ? <div className="text-[12.5px] text-subtle">The queue is empty.</div> : (
                <ul className="divide-y divide-border">
                  {queue.map((j) => (
                    <li key={j.id} className="flex items-center gap-2 py-2 text-[12.5px]">
                      <StatusBadge status={j.status} />
                      <span className="font-mono">{j.scenario}</span>
                      <span className="ml-auto text-subtle">{fmtAgo(j.created_at)}</span>
                    </li>
                  ))}
                </ul>
              )}
            </CardBody>
          </Card>
        </div>
      </div>
    </>
  );
}
