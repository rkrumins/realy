import { useQuery } from "@tanstack/react-query";
import { Boxes, Cpu, Database, ExternalLink, HardDrive, LayoutDashboard, LineChart, RefreshCw, Server, Terminal, Workflow } from "lucide-react";
import { ErrorState, MiniBar, PageHeader, Stat, Unavailable } from "@/components/common";
import { Badge, Dot } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardBody, CardHeader } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/controls";
import { api, isNotFound } from "@/lib/api";
import { cn } from "@/lib/cn";
import { fmtBytes, fmtCompact, fmtDuration, fmtMs, fmtNum, isNum } from "@/lib/format";
import { useEnv } from "@/lib/hooks";
import type { EnvComponent } from "@/lib/types";

const ICONS: Record<string, typeof Server> = {
  kafka: Workflow, redis: Database, clickhouse: HardDrive, flink: Boxes, gateway: Server, model: Cpu, console: Terminal,
};

function fmtDetail(v: unknown): string {
  if (v === null || v === undefined) return "—";
  if (typeof v === "boolean") return v ? "yes" : "no";
  if (typeof v === "number") return fmtNum(v, v % 1 ? 2 : 0);
  if (Array.isArray(v)) return v.length ? v.join(", ") : "none";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

function ComponentCard({ c }: { c: EnvComponent }) {
  const Icon = ICONS[c.id] ?? Server;
  const tone = c.status === "up" ? "pass" : c.status === "degraded" ? "warn" : "fail";
  return (
    <Card className="flex flex-col p-4" data-testid={`env-${c.id}`}>
      <div className="flex items-start gap-3">
        <div className={cn("flex size-9 items-center justify-center rounded-xl", c.status === "up" ? "bg-pass/12 text-pass" : c.status === "degraded" ? "bg-warn/12 text-warn" : "bg-fail/12 text-fail")}>
          <Icon className="size-[18px]" />
        </div>
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2"><span className="text-[14px] font-semibold">{c.name}</span></div>
          <div className="truncate text-[12px] text-subtle">{c.kind ?? c.id} · {c.version ?? "unknown version"}</div>
        </div>
        <Badge tone={tone}><Dot tone={tone} pulse={c.status !== "up"} />{c.status}</Badge>
      </div>
      <div className="mt-3 flex-1">
        {Object.entries(c.details ?? {}).filter(([k]) => k !== "service_time").slice(0, 5).map(([k, v]) => (
          <Stat key={k} label={k.replace(/_/g, " ")} value={<span className={cn(k === "error" && "text-fail", "break-all")}>{fmtDetail(v)}</span>} />
        ))}
      </div>
      <div className="mt-2 flex items-center justify-between border-t border-border pt-2 text-[11.5px] text-subtle">
        <span className="truncate font-mono">{c.url ?? ""}</span>
        <span className="num">{isNum(c.latency_ms) ? `probe ${fmtMs(c.latency_ms)}` : ""}</span>
      </div>
    </Card>
  );
}

function useDetail<T>(key: string, fn: () => Promise<T>) {
  return useQuery<T | null>({
    queryKey: ["env", key],
    queryFn: async () => {
      try { return await fn(); } catch (e) { if (isNotFound(e)) return null; throw e; }
    },
    refetchInterval: 10_000, retry: 0,
  });
}

export default function Environment() {
  const env = useEnv();
  const kafka = useDetail("kafka", api.envKafka);
  const flink = useDetail("flink", api.envFlink);
  const redis = useDetail("redis", api.envRedis);
  const ch = useDetail("clickhouse", api.envClickhouse);
  const links = env.data?.links ?? {};
  const maxLag = Math.max(1, ...(kafka.data?.groups ?? []).map((g) => g.lag ?? 0));
  const refresh = () => { env.refetch(); kafka.refetch(); flink.refetch(); redis.refetch(); ch.refetch(); };

  return (
    <>
      <PageHeader eyebrow="Environment" title="Environment & infrastructure"
        subtitle="Health of every component the benchmark drives, plus the Kafka, Flink, Redis and ClickHouse internals that explain its numbers."
        actions={<>
          {links.flink && <Button variant="outline" asChild><a href={links.flink} target="_blank" rel="noreferrer"><Boxes />Flink UI<ExternalLink className="size-3" /></a></Button>}
          {links.grafana && <Button variant="outline" asChild><a href={links.grafana} target="_blank" rel="noreferrer"><LayoutDashboard />Grafana<ExternalLink className="size-3" /></a></Button>}
          {links.prometheus && <Button variant="outline" asChild><a href={links.prometheus} target="_blank" rel="noreferrer"><LineChart />Prometheus<ExternalLink className="size-3" /></a></Button>}
          <Button variant="secondary" onClick={refresh}><RefreshCw className={cn(env.isFetching && "animate-spin")} />Refresh</Button>
        </>} />
      {env.error && <ErrorState error={env.error} />}
      {env.isLoading ? <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">{Array.from({ length: 7 }, (_, i) => <Skeleton key={i} className="h-48 rounded-2xl" />)}</div>
        : !env.data ? <Unavailable what="Environment status" /> : (
          <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">{env.data.components.map((c) => <ComponentCard key={c.id} c={c} />)}</div>
        )}

      <div className="mt-6 grid gap-5 xl:grid-cols-2">
        <Card>
          <CardHeader title="Kafka" icon={<Workflow />} subtitle={kafka.data ? `${kafka.data.brokers.length} broker(s): ${kafka.data.brokers.map((b) => `${b.host}:${b.port}`).join(", ")}` : "topics and consumer lag"} />
          <CardBody className="space-y-5">
            {kafka.isLoading ? <Skeleton className="h-40" /> : kafka.error ? <ErrorState error={kafka.error} title="Kafka unreachable" /> : !kafka.data ? <Unavailable what="Kafka detail" /> : <>
              <table className="w-full text-[12.5px]">
                <thead className="text-left text-[11px] uppercase tracking-wider text-subtle"><tr><th className="pb-1.5 font-medium">Topic</th><th className="pb-1.5 text-right font-medium">Partitions</th><th className="pb-1.5 text-right font-medium">Messages (end offsets)</th></tr></thead>
                <tbody>{kafka.data.topics.map((t) => (
                  <tr key={t.name} className="border-t border-border"><td className="py-1.5 font-mono">{t.name}</td><td className="text-right num">{t.partitions}</td><td className="text-right num">{fmtCompact(t.end_offset_total)}</td></tr>
                ))}</tbody>
              </table>
              <div>
                <div className="mb-2 text-[11px] font-semibold uppercase tracking-[0.12em] text-subtle">Consumer groups · lag</div>
                {kafka.data.groups.length === 0 ? <div className="text-[12.5px] text-subtle">No active consumer groups.</div> : (
                  <div className="space-y-1.5">{kafka.data.groups.map((g) => (
                    <div key={`${g.group}-${g.topic}`} className="grid grid-cols-[minmax(0,1.3fr)_minmax(0,1fr)_90px_70px] items-center gap-3 text-[12px]">
                      <span className="truncate font-mono text-muted" title={g.group}>{g.group}</span>
                      <span className="truncate font-mono text-subtle">{g.topic}</span>
                      <MiniBar value={g.lag ?? 0} max={maxLag} tone={(g.lag ?? 0) > 10_000 ? "warn" : "accent"} />
                      <span className="text-right num">{isNum(g.lag) ? fmtCompact(g.lag) : g.state ?? "—"}</span>
                    </div>
                  ))}</div>
                )}
              </div>
            </>}
          </CardBody>
        </Card>

        <Card>
          <CardHeader title="Flink" icon={<Boxes />} subtitle={flink.data ? `v${flink.data.version} · ${flink.data.taskmanagers} task manager(s) · ${flink.data.slots_available}/${flink.data.slots_total} slots free` : "jobs, checkpoints, backpressure"} />
          <CardBody>
            {flink.isLoading ? <Skeleton className="h-40" /> : flink.error ? <ErrorState error={flink.error} title="Flink unreachable" /> : !flink.data ? <Unavailable what="Flink detail" /> : flink.data.jobs.length === 0 ? <div className="text-[12.5px] text-subtle">No jobs.</div> : (
              <div className="space-y-4">{flink.data.jobs.map((j) => (
                <div key={j.id} className="rounded-xl border border-border p-3">
                  <div className="flex flex-wrap items-center gap-2">
                    <Badge tone={j.state === "RUNNING" ? "pass" : j.state === "FAILED" ? "fail" : "neutral"}>{j.state.toLowerCase()}</Badge>
                    <span className="truncate text-[13px] font-medium">{j.name}</span>
                    <span className="ml-auto text-[11.5px] text-subtle num">{fmtDuration((j.duration_ms ?? 0) / 1000)}</span>
                  </div>
                  {j.checkpoints && (
                    <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-[11.5px] text-muted num">
                      <span>checkpoints {fmtNum(j.checkpoints.completed)} ok · {fmtNum(j.checkpoints.failed)} failed</span>
                      <span>last {fmtMs(j.checkpoints.last_duration_ms)} · {fmtBytes(j.checkpoints.last_size_bytes)}</span>
                    </div>
                  )}
                  {j.vertices.length > 0 && (
                    <div className="mt-2 space-y-1">{j.vertices.map((v) => (
                      <div key={v.name} className="grid grid-cols-[minmax(0,1fr)_40px_80px_70px] items-center gap-2 text-[11.5px]">
                        <span className="truncate text-muted" title={v.name}>{v.name}</span>
                        <span className="text-right text-subtle num">×{v.parallelism}</span>
                        <Badge tone={v.backpressure === "high" ? "fail" : v.backpressure === "low" ? "warn" : "neutral"} className="justify-self-start">bp {v.backpressure ?? "?"}</Badge>
                        <span className="text-right text-subtle num">{fmtCompact(v.read_records)}</span>
                      </div>
                    ))}</div>
                  )}
                </div>
              ))}</div>
            )}
          </CardBody>
        </Card>

        <Card>
          <CardHeader title="Redis" icon={<Database />} subtitle="online feature store" />
          <CardBody className="grid gap-x-8 sm:grid-cols-2">
            {redis.isLoading ? <Skeleton className="h-28 sm:col-span-2" /> : redis.error ? <div className="sm:col-span-2"><ErrorState error={redis.error} title="Redis unreachable" /></div> : !redis.data ? <div className="sm:col-span-2"><Unavailable what="Redis detail" /></div> : <>
              <div><Stat label="Version" value={redis.data.version} /><Stat label="Keys" value={fmtNum(redis.data.keys)} /><Stat label="Memory" value={fmtBytes(redis.data.used_memory_bytes)} /></div>
              <div><Stat label="Ops / s" value={fmtNum(redis.data.ops_per_sec)} /><Stat label="Clients" value={fmtNum(redis.data.connected_clients)} /><Stat label="Uptime" value={fmtDuration(redis.data.uptime_s)} /></div>
            </>}
          </CardBody>
        </Card>

        <Card>
          <CardHeader title="ClickHouse" icon={<HardDrive />} subtitle={ch.data ? `v${ch.data.version} · database realy` : "audit warehouse tables"} />
          <CardBody>
            {ch.isLoading ? <Skeleton className="h-28" /> : ch.error ? <ErrorState error={ch.error} title="ClickHouse unreachable" /> : !ch.data ? <Unavailable what="ClickHouse detail" /> : (
              <table className="w-full text-[12.5px]">
                <thead className="text-left text-[11px] uppercase tracking-wider text-subtle"><tr><th className="pb-1.5 font-medium">Table</th><th className="pb-1.5 text-right font-medium">Rows</th><th className="pb-1.5 text-right font-medium">Parts</th><th className="pb-1.5 text-right font-medium">On disk</th></tr></thead>
                <tbody>{ch.data.tables.map((t) => (
                  <tr key={t.table} className="border-t border-border"><td className="py-1.5 font-mono">{t.table}</td><td className="text-right num">{fmtCompact(t.rows)}</td><td className="text-right num">{t.parts}</td><td className="text-right num">{fmtBytes(t.bytes)}</td></tr>
                ))}</tbody>
              </table>
            )}
          </CardBody>
        </Card>
      </div>
    </>
  );
}
