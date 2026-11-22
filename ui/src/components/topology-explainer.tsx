/**
 * What the run's topology is and whether the scenario fits it, on the Launch page: its tagline and runtime,
 * a warning when the scenario needs something the topology cannot do, and a one-click preflight of the
 * infrastructure the run would use (real, mock or bundled, with what would be simulated).
 */
import { useQuery } from "@tanstack/react-query";
import { AlertTriangle, ArrowRight, Loader2, ShieldCheck } from "lucide-react";
import * as React from "react";
import { Link } from "react-router";
import { Badge, Dot } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { api } from "@/lib/api";
import type { PreflightResult } from "@/lib/types";

const IMPL_TONE = { bundled: "neutral", mock: "warn", external: "info", unknown: "neutral" } as const;
const STATUS_TONE = { pass: "pass", warn: "warn", fail: "fail", skip: "neutral" } as const;

export function PreflightSummary({ r }: { r: PreflightResult }) {
  return (
    <div className="space-y-2" data-testid="preflight-summary">
      <ul className="divide-y divide-border rounded-lg border border-border">
        {r.components.map((c) => (
          <li key={c.component} className="flex flex-wrap items-center gap-2 px-3 py-1.5 text-[12.5px]">
            <Dot tone={STATUS_TONE[c.status]} />
            <span className="font-medium">{c.label}</span>
            <Badge tone={IMPL_TONE[c.implementation]}>{c.implementation}</Badge>
            <span className="truncate text-subtle">{c.version ?? ""}</span>
            {c.checks.filter((k) => k.status === "warn" || k.status === "fail").slice(0, 2).map((k) => (
              <span key={k.name} className={k.status === "fail" ? "w-full text-fail" : "w-full text-warn"}>{k.detail}{k.fix ? ` - ${k.fix}` : ""}</span>
            ))}
          </li>
        ))}
      </ul>
      {r.simulated.length > 0 && (
        <div className="rounded-lg bg-warn/8 px-3 py-2 text-[12px] text-muted">
          <div className="mb-0.5 font-medium text-warn">Simulated in this run</div>
          {r.simulated.map((s) => <div key={s.what}><b>{s.what}</b>: {s.detail}</div>)}
        </div>
      )}
    </div>
  );
}

export function TopologyExplainer({ topology, scenario, flows }: { topology: string; scenario: string; flows: string[] }) {
  const tops = useQuery({ queryKey: ["topologies"], queryFn: api.topologies, staleTime: 60_000 });
  const t = tops.data?.topologies.find((x) => x.id === topology);
  const [check, setCheck] = React.useState(false);
  const pre = useQuery({ queryKey: ["preflight", scenario, topology], queryFn: () => api.preflight({ scenario, topology }),
    enabled: check, staleTime: 15_000 });
  React.useEffect(() => setCheck(false), [scenario, topology]);
  if (!t) return null;
  const cannotFlows = flows.length > 0 && t.runtime.flows === "none";
  return (
    <div className="space-y-2 rounded-lg border border-border px-3 py-2.5" data-testid="topology-explainer">
      <div className="flex items-start gap-2">
        <p className="flex-1 text-[12.5px] leading-5 text-muted"><b className="text-fg">{t.title}.</b> {t.tagline}</p>
        <Link to={`/topologies?t=${encodeURIComponent(t.id)}`} className="shrink-0 text-[12px] text-accent hover:underline">
          how it works <ArrowRight className="inline size-3" /></Link>
      </div>
      {cannotFlows && (
        <div className="flex items-start gap-2 rounded-md bg-fail/10 px-2.5 py-1.5 text-[12px] text-fail" role="alert">
          <AlertTriangle className="mt-0.5 size-3.5 shrink-0" />
          <span>This scenario sends multi-message transactions ({flows.join(", ")}), which {t.title} cannot carry:
            {" "}{t.capabilities.multi_message?.note}. The run will be refused; pick a topology that assembles them.</span>
        </div>
      )}
      <div className="flex items-center gap-2">
        <Button size="sm" variant="secondary" onClick={() => { setCheck(true); pre.refetch(); }} disabled={pre.isFetching}>
          {pre.isFetching ? <Loader2 className="animate-spin" /> : <ShieldCheck />}Check infrastructure
        </Button>
        {pre.data && <Badge tone={STATUS_TONE[pre.data.status]}>{pre.data.status === "pass" ? "ready" : pre.data.status}</Badge>}
        {pre.isError && <span className="text-[12px] text-fail">{(pre.error as Error).message}</span>}
      </div>
      {pre.data && <PreflightSummary r={pre.data} />}
    </div>
  );
}
