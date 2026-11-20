/**
 * Live pipeline topology map: nodes on a column/row grid, edges animated at a speed proportional to
 * events/s, nodes and edges coloured by their p99 share of the e2e budget.
 */
import { useQuery } from "@tanstack/react-query";
import { Boxes, Cpu, Database, HardDrive, Radio, Server, Workflow } from "lucide-react";
import * as React from "react";
import { api } from "@/lib/api";
import { cn } from "@/lib/cn";
import { fmtCompact, fmtMs, fmtPct, isNum } from "@/lib/format";
import { FALLBACK_TOPOLOGIES, heat, resolveP99, resolveRate, steadyHeat, type Heat } from "@/lib/topology";
import type { NodeState, NodeStateName, Snapshot, TopologyNode, TopologyResponse } from "@/lib/types";

const ICON: Record<TopologyNode["kind"], typeof Server> = {
  driver: Radio, kafka: Workflow, flink: Boxes, store: Database, service: Server, model: Cpu, sink: HardDrive,
};
const KIND_LABEL: Record<TopologyNode["kind"], string> = {
  driver: "bench driver", kafka: "Kafka", flink: "Flink", store: "store", service: "service", model: "model", sink: "sink",
};
const HEAT_COLOR: Record<Heat, string> = { idle: "var(--fg-subtle)", good: "var(--pass)", warn: "var(--warn)", hot: "var(--fail)" };
/** explicit states override latency colouring: a warming pipeline is not a slow one */
const STATE_COLOR: Record<Exclude<NodeStateName, "ready">, string> = {
  down: "var(--fail)", starting: "var(--info)", warming: "var(--accent)", degraded: "var(--warn)",
};

const NW = 176; // node width
const NH = 66;
const CW = 244; // column pitch
const RH = 96; // row pitch
const PAD = 18;

export function useTopology(topology: string | null | undefined) {
  const t = topology || "gateway-kafka";
  return useQuery<TopologyResponse>({
    queryKey: ["topology", t],
    queryFn: async () => {
      try {
        return await api.topology(t);
      } catch {
        return FALLBACK_TOPOLOGIES[t] ?? FALLBACK_TOPOLOGIES["gateway-kafka"];
      }
    },
    staleTime: Infinity,
    placeholderData: FALLBACK_TOPOLOGIES[t],
  });
}

function edgePath(a: { x: number; y: number }, b: { x: number; y: number }) {
  // a/b = node top-left corners
  const ax = a.x + NW, ay = a.y + NH / 2, bx = b.x, by = b.y + NH / 2;
  if (b.x > a.x) {
    const dx = Math.max(30, (bx - ax) / 2);
    return { d: `M${ax},${ay} C${ax + dx},${ay} ${bx - dx},${by} ${bx},${by}`, mx: (ax + bx) / 2, my: (ay + by) / 2 };
  }
  if (b.x === a.x) {
    // same column: vertical connector
    const down = b.y > a.y;
    const sx = a.x + NW / 2, sy = down ? a.y + NH : a.y, ex = b.x + NW / 2, ey = down ? b.y : b.y + NH;
    return { d: `M${sx},${sy} C${sx},${(sy + ey) / 2} ${ex},${(sy + ey) / 2} ${ex},${ey}`, mx: sx, my: (sy + ey) / 2 };
  }
  // backwards edge (lookup / response): leave from the left, enter from the right, bulging away
  const sx = a.x, sy = a.y + NH / 2, ex = b.x + NW, ey = b.y + NH / 2;
  const bulge = a.y === b.y ? 46 : 0;
  const c1y = sy + (a.y === b.y ? NH / 2 + bulge : 0);
  const c2y = ey + (a.y === b.y ? NH / 2 + bulge : 0);
  const sx2 = a.y === b.y ? a.x + NW / 2 : sx, sy2 = a.y === b.y ? a.y + NH : sy;
  const ex2 = a.y === b.y ? b.x + NW / 2 : ex, ey2 = a.y === b.y ? b.y + NH : ey;
  return {
    d: `M${sx2},${sy2} C${sx2 - 20},${c1y + 20} ${ex2 + 20},${c2y + 20} ${ex2},${ey2}`,
    mx: (sx2 + ex2) / 2, my: Math.max(sy2, ey2) + (a.y === b.y ? bulge * 0.75 : 0),
  };
}

function flowDuration(rate: number | null) {
  if (!isNum(rate) || rate <= 0) return null;
  return Math.max(0.35, Math.min(3.2, 3.4 - Math.log10(rate + 1) * 0.75));
}

export function TopologyMap({ graph, snapshots, budgetMs, active, className, onSelect, mini, states }: {
  graph: TopologyResponse | undefined; snapshots: Snapshot[]; budgetMs: number; active: boolean; className?: string;
  onSelect?: (n: TopologyNode) => void; mini?: boolean;
  /** per-node state from the console (status.pipeline.nodes) */
  states?: Record<string, NodeState> | null;
}) {
  const [hover, setHover] = React.useState<string | null>(null);
  const heatHistory = React.useRef(new Map<string, Heat[]>());
  // computed once per snapshot tick (not on hover re-renders): hysteresis needs one sample per second
  const nodeStats = React.useMemo(() => new Map((graph?.nodes ?? []).map((n) => {
    const rate = active ? resolveRate(n.rate, snapshots) : null;
    const p99 = active ? resolveP99(n.p99, snapshots) : null;
    const hist = heatHistory.current.get(n.id) ?? [];
    heatHistory.current.set(n.id, hist);
    return [n.id, { rate, p99, heat: steadyHeat(hist, heat(p99, budgetMs, n.p99?.stage, n.slo_ms)) }];
  })), [graph, snapshots, budgetMs, active]);
  if (!graph) return <div className={cn("skeleton w-full", mini ? "h-[120px]" : "h-[260px]")} />;
  const cols = Math.max(...graph.nodes.map((n) => n.column)) + 1;
  const rows = Math.max(...graph.nodes.map((n) => n.row)) + 1;
  const W = PAD * 2 + (cols - 1) * CW + NW;
  const H = PAD * 2 + (rows - 1) * RH + NH + 34;
  const pos = new Map(graph.nodes.map((n) => [n.id, { x: PAD + n.column * CW, y: PAD + n.row * RH }]));

  return (
    <div className={cn("relative w-full overflow-x-auto", className)}>
      <svg viewBox={`0 0 ${W} ${H}`} className="mx-auto block h-auto w-full" style={mini ? { maxHeight: 220 } : { minWidth: Math.min(W, 720), maxHeight: 440 }} role="img"
        aria-label={`${graph.topology} pipeline topology`}>
        <defs>
          <marker id="topo-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
            <path d="M0,1 L9,5 L0,9 z" fill="var(--border-strong)" />
          </marker>
          <filter id="topo-glow" x="-20%" y="-20%" width="140%" height="140%">
            <feGaussianBlur stdDeviation="6" result="b" />
            <feMerge><feMergeNode in="b" /><feMergeNode in="SourceGraphic" /></feMerge>
          </filter>
        </defs>

        {/* edges */}
        {graph.edges.map((e, i) => {
          const a = pos.get(e.from), b = pos.get(e.to);
          if (!a || !b) return null;
          const { d, mx, my } = edgePath(a, b);
          const warming = states?.[e.from]?.state === "warming" || states?.[e.to]?.state === "warming";
          const bulk = warming && active && e.bulk ? resolveRate(e.bulk, snapshots) : null;
          const liveRate = active ? resolveRate(e.rate, snapshots) : null;
          const rate = isNum(bulk) && bulk > 0 ? bulk : liveRate;
          const p99 = active && e.p99 && !(isNum(bulk) && bulk > 0) ? resolveP99(e.p99, snapshots) : null;
          const h = p99 !== null ? heat(p99, budgetMs, e.p99?.stage) : rate ? "good" : "idle";
          const dur = flowDuration(rate);
          const color = isNum(bulk) && bulk > 0 ? "var(--accent)" : h === "idle" ? "var(--border-strong)" : h === "good" ? "var(--accent)" : HEAT_COLOR[h];
          const lit = hover === e.from || hover === e.to;
          return (
            <g key={i} opacity={hover && !lit ? 0.35 : 1}>
              <path d={d} fill="none" stroke="var(--border)" strokeWidth={5} strokeLinecap="round" />
              <path d={d} fill="none" stroke={color} strokeOpacity={dur ? 0.35 : 0.6} strokeWidth={1.6} markerEnd="url(#topo-arrow)" />
              {dur && (
                <path d={d} fill="none" stroke={color} strokeWidth={2.4} strokeLinecap="round" strokeDasharray="3 9"
                  style={{ animation: `flow ${dur}s linear infinite` }} />
              )}
              <title>{`${e.from} → ${e.to}${e.label ? ` (${e.label})` : ""}${isNum(bulk) && bulk > 0 ? ` · history bulk load ${fmtCompact(bulk)}/s (not judged)` : isNum(rate) ? ` · ${fmtCompact(rate)} events/s` : ""}${isNum(p99) ? ` · p99 ${fmtMs(p99)}` : ""}`}</title>
              {(e.label || isNum(rate)) && (
                <g transform={`translate(${mx},${my - 8})`}>
                  {/* gaps between columns are narrow: show the live rate when there is one, else the verb */}
                  <text textAnchor="middle" className={isNum(rate) ? "fill-[var(--fg-muted)] text-[10px] font-medium" : "fill-[var(--fg-subtle)] text-[10px]"}
                    style={{ fontFamily: isNum(rate) ? "var(--font-mono)" : "var(--font-sans)" }}>
                    {isNum(rate) ? `${fmtCompact(rate)}/s` : e.label}
                  </text>
                </g>
              )}
            </g>
          );
        })}

        {/* nodes */}
        {graph.nodes.map((n) => {
          const p = pos.get(n.id)!;
          const st = nodeStats.get(n.id)!;
          const Icon = ICON[n.kind] ?? Server;
          const ns = states?.[n.id];
          const override = ns && ns.state !== "ready" ? ns : null;
          const stroke = override ? STATE_COLOR[override.state as Exclude<NodeStateName, "ready">]
            : st.heat === "idle" ? "var(--border-strong)" : HEAT_COLOR[st.heat];
          const share = override ? null : isNum(st.p99) ? st.p99 / (n.slo_ms ?? budgetMs) : null;
          const heatHot = !override && st.heat === "hot";
          return (
            <g key={n.id} transform={`translate(${p.x},${p.y})`} onMouseEnter={() => setHover(n.id)} onMouseLeave={() => setHover(null)}
              onClick={() => onSelect?.(n)} className={onSelect ? "cursor-pointer" : undefined}>
              <title>{`${n.label} (${KIND_LABEL[n.kind]})${override ? `\n${override.state}: ${override.reason}` : ""}${isNum(st.rate) ? `\n${fmtCompact(st.rate)} events/s` : ""}${isNum(st.p99) ? `\np99 ${fmtMs(st.p99)} = ${fmtPct(isNum(st.p99) ? st.p99 / (n.slo_ms ?? budgetMs) : null)} of ${n.slo_ms ? `its ${fmtMs(n.slo_ms)} SLO` : `the ${fmtMs(budgetMs)} budget`}` : ""}`}</title>
              {heatHot && <rect width={NW} height={NH} rx={14} fill="var(--fail)" opacity={0.18} filter="url(#topo-glow)" />}
              {override?.state === "down" && <rect width={NW} height={NH} rx={14} fill="var(--fail)" opacity={0.14} filter="url(#topo-glow)" />}
              <rect width={NW} height={NH} rx={14} fill="var(--card-2)" stroke={stroke} strokeOpacity={override ? 0.95 : st.heat === "idle" ? 1 : 0.7}
                strokeWidth={override ? 1.6 : 1.2} strokeDasharray={override?.state === "starting" ? "5 4" : undefined}>
                {override && (override.state === "starting" || override.state === "warming") && (
                  <animate attributeName="stroke-opacity" values="0.95;0.4;0.95" dur="1.6s" repeatCount="indefinite" />
                )}
              </rect>
              <rect x={10} y={10} width={26} height={26} rx={8} fill="var(--card-3)" />
              <Icon x={15} y={15} width={16} height={16} color={!override && st.heat === "idle" ? "var(--fg-muted)" : stroke} strokeWidth={2} />
              <text x={44} y={21} className="fill-[var(--fg)] text-[12.5px] font-semibold" style={{ fontFamily: "var(--font-sans)" }}>
                {n.label.length > 19 ? `${n.label.slice(0, 18)}…` : n.label}
              </text>
              <text x={44} y={34} className="fill-[var(--fg-subtle)] text-[10px]" style={{ fontFamily: "var(--font-sans)" }}>
                {(n.subtitle || KIND_LABEL[n.kind]).slice(0, 24)}
              </text>
              <text x={12} y={54} className="fill-[var(--fg-muted)] text-[10.5px]" style={{ fontFamily: "var(--font-mono)" }}>
                {isNum(st.rate) ? `${fmtCompact(st.rate)}/s` : active ? "—" : ""}
              </text>
              <text x={NW - 12} y={54} textAnchor="end" className="text-[10.5px] font-medium" fill={override ? stroke : isNum(st.p99) ? stroke : "var(--fg-subtle)"}
                style={{ fontFamily: override ? "var(--font-sans)" : "var(--font-mono)" }}>
                {override ? `${override.state}${isNum(override.progress) ? ` ${Math.round(override.progress * 100)}%` : ""}` : isNum(st.p99) ? `p99 ${fmtMs(st.p99)}` : ""}
              </text>
              {override && isNum(override.progress) ? (
                <>
                  <rect x={12} y={NH - 7} width={NW - 24} height={3} rx={1.5} fill="var(--card-3)" />
                  <rect x={12} y={NH - 7} width={Math.min(1, override.progress) * (NW - 24)} height={3} rx={1.5} fill={stroke} />
                </>
              ) : share !== null && (
                <>
                  <rect x={12} y={NH - 7} width={NW - 24} height={2} rx={1} fill="var(--card-3)" />
                  <rect x={12} y={NH - 7} width={Math.min(1, share) * (NW - 24)} height={2} rx={1} fill={stroke} />
                </>
              )}
            </g>
          );
        })}
      </svg>
      {!mini && <div className="mt-1 flex flex-wrap items-center justify-end gap-x-4 gap-y-1 px-1 text-[11px] text-subtle">
        <span className="inline-flex items-center gap-1.5"><span className="size-2 rounded-full bg-pass" />p99 well inside budget</span>
        <span className="inline-flex items-center gap-1.5"><span className="size-2 rounded-full bg-warn" />large budget share / degraded</span>
        <span className="inline-flex items-center gap-1.5"><span className="size-2 rounded-full bg-fail" />over its share (3 s) / down</span>
        <span className="inline-flex items-center gap-1.5"><span className="size-2 rounded-full bg-info" />starting</span>
        <span className="inline-flex items-center gap-1.5"><span className="size-2 rounded-full bg-accent" />warming (history, catch-up, prime)</span>
        <span>edge speed ∝ events/s</span>
      </div>}
    </div>
  );
}
