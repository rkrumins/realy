/**
 * The preflight on the Environment page: pick a scenario (or a topology) and see every component its run
 * would use, as the run's first startup step sees it: bundled, mock or yours, each check with its fix, and
 * what in the run would be simulated.
 */
import { useQuery } from "@tanstack/react-query";
import { Loader2, ShieldCheck } from "lucide-react";
import * as React from "react";
import { Badge, Dot } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardBody, CardHeader } from "@/components/ui/card";
import { Select } from "@/components/ui/select";
import { ErrorState } from "@/components/common";
import { api } from "@/lib/api";
import { fmtMs } from "@/lib/format";

const IMPL_TONE = { bundled: "neutral", mock: "warn", external: "info", unknown: "neutral" } as const;
const STATUS_TONE = { pass: "pass", warn: "warn", fail: "fail", skip: "neutral" } as const;
const IMPL_HINT = {
  bundled: "the component this stack ships (Docker Compose / the local overlay)",
  mock: "a stand-in: its latency is simulated, not measured",
  external: "yours: the run measures it",
  unknown: "not reported",
} as const;

export function PreflightCard() {
  const scenarios = useQuery({ queryKey: ["scenarios"], queryFn: api.scenarios });
  const [scenario, setScenario] = React.useState("");
  const name = scenario || scenarios.data?.find((s) => s.name === "smoke")?.name || scenarios.data?.[0]?.name || "";
  const pre = useQuery({ queryKey: ["preflight", name], queryFn: () => api.preflight({ scenario: name }), enabled: !!name, staleTime: 15_000 });
  return (
    <Card className="mb-6" data-testid="preflight-card">
      <CardHeader title="Preflight" icon={<ShieldCheck />}
        subtitle="What a run of this scenario would use, as its first startup step checks it: bundled, a mock, or yours."
        actions={<>
          <div className="w-56"><Select value={name} onChange={setScenario} ariaLabel="Scenario"
            options={(scenarios.data ?? []).map((s) => ({ value: s.name, label: s.name, hint: s.topology ?? "" }))} /></div>
          <Button variant="secondary" size="sm" onClick={() => pre.refetch()} disabled={pre.isFetching}>
            {pre.isFetching ? <Loader2 className="animate-spin" /> : <ShieldCheck />}Check
          </Button>
        </>} />
      <CardBody className="pt-0">
        {pre.error ? <ErrorState error={pre.error} title="Preflight failed to run" /> : !pre.data ? (
          <div className="text-[12.5px] text-subtle">{pre.isFetching ? "Probing every component…" : "Pick a scenario."}</div>
        ) : (
          <>
            <div className="mb-3 flex flex-wrap items-center gap-2 text-[12.5px]">
              <Badge tone={STATUS_TONE[pre.data.status]}>{pre.data.status === "pass" ? "ready" : pre.data.status}</Badge>
              <span className="text-muted">topology <span className="font-mono text-fg">{pre.data.topology}</span></span>
              <span className="ml-auto flex flex-wrap gap-3 text-subtle">
                {(["bundled", "mock", "external"] as const).map((k) => (
                  <span key={k} title={IMPL_HINT[k]} className="flex items-center gap-1"><Badge tone={IMPL_TONE[k]}>{k}</Badge>{IMPL_HINT[k].split(":")[0]}</span>
                ))}
              </span>
            </div>
            <div className="grid grid-cols-1 gap-3 lg:grid-cols-2 2xl:grid-cols-3">
              {pre.data.components.map((c) => (
                <div key={c.component} className="rounded-lg border border-border">
                  <div className="flex flex-wrap items-center gap-2 border-b border-border px-3 py-2">
                    <Dot tone={STATUS_TONE[c.status]} pulse={c.status === "fail"} />
                    <span className="text-[13px] font-semibold">{c.label}</span>
                    <Badge tone={IMPL_TONE[c.implementation]} title={IMPL_HINT[c.implementation]}>{c.implementation}</Badge>
                    <span className="ml-auto truncate text-[11.5px] text-subtle">{c.version ?? ""}</span>
                  </div>
                  <div className="truncate px-3 pt-1.5 font-mono text-[11px] text-subtle">{c.endpoint || "—"}{c.probe_ms !== undefined ? ` · probe ${fmtMs(c.probe_ms)}` : ""}</div>
                  <ul className="space-y-1 px-3 py-2">
                    {c.checks.map((k) => (
                      <li key={k.name} className="text-[12px] leading-5">
                        <span className="inline-flex items-center gap-1.5"><Dot tone={STATUS_TONE[k.status]} /><span className="font-medium">{k.name}</span></span>
                        <span className="text-muted"> {k.detail}</span>
                        {k.fix && k.status !== "pass" && <div className="ml-4 text-[11.5px] text-accent">fix: {k.fix}</div>}
                      </li>
                    ))}
                  </ul>
                </div>
              ))}
            </div>
            {pre.data.simulated.length > 0 && (
              <div className="mt-3 rounded-lg bg-warn/8 px-3 py-2 text-[12.5px] text-muted">
                <div className="mb-0.5 font-medium text-warn">Simulated in a run of {name}</div>
                {pre.data.simulated.map((s) => <div key={s.what}><b>{s.what}</b>: {s.detail}</div>)}
              </div>
            )}
          </>
        )}
      </CardBody>
    </Card>
  );
}
