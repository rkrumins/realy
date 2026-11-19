import { useEffect, useRef } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";

// Colour palette chosen to stay legible on both light and dark panels.
export const PALETTE = ["#3b82f6", "#f59e0b", "#10b981", "#a855f7", "#ef4444", "#06b6d4", "#ec4899", "#84cc16"];

export type Series = { label: string; color?: string; dash?: number[]; width?: number };

function cssVar(name: string, fallback: string): string {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v || fallback;
}

type Props = {
  title: string;
  unit?: string;
  series: Series[];
  data: (number | null)[][]; // [xs, ...ys]
  height?: number;
  hline?: { value: number; label: string } | null;
  xTime?: boolean;            // x in unix seconds (default) or plain numbers
  right?: React.ReactNode;
};

export default function Chart({ title, unit, series, data, height = 180, hline, xTime = true, right }: Props) {
  const ref = useRef<HTMLDivElement>(null);
  const plot = useRef<uPlot | null>(null);
  const hl = useRef(hline);
  hl.current = hline;

  useEffect(() => {
    const el = ref.current!;
    const muted = cssVar("--muted", "#888");
    const grid = cssVar("--line", "#ddd");
    const bad = cssVar("--bad", "#c00");
    const opts: uPlot.Options = {
      width: el.clientWidth || 600,
      height,
      legend: { show: false },
      cursor: { drag: { x: false, y: false } },
      scales: { x: { time: xTime }, y: { range: (_u, _min, max) => [0, max > 0 ? max * 1.1 : 1] } },
      axes: [
        { stroke: muted, grid: { stroke: grid, width: 1 }, ticks: { stroke: grid }, size: 28, font: "11px system-ui" },
        { stroke: muted, grid: { stroke: grid, width: 1 }, ticks: { stroke: grid }, size: 46, font: "11px system-ui",
          values: (_u, vals) => vals.map((v) => (Math.abs(v) >= 1000 ? `${(v / 1000).toFixed(1)}k` : `${+v.toFixed(2)}`)) },
      ],
      series: [{}, ...series.map((s, i) => ({
        label: s.label, stroke: s.color ?? PALETTE[i % PALETTE.length], width: s.width ?? 1.6, dash: s.dash,
        points: { show: false }, spanGaps: true,
      }))],
      hooks: {
        draw: [(u) => {
          const h = hl.current;
          if (!h) return;
          const y = u.valToPos(h.value, "y", true);
          if (!isFinite(y) || y < u.bbox.top || y > u.bbox.top + u.bbox.height) return;
          const ctx = u.ctx;
          ctx.save();
          ctx.strokeStyle = bad;
          ctx.setLineDash([5, 4]);
          ctx.lineWidth = 1.2 * devicePixelRatio;
          ctx.beginPath();
          ctx.moveTo(u.bbox.left, y);
          ctx.lineTo(u.bbox.left + u.bbox.width, y);
          ctx.stroke();
          ctx.fillStyle = bad;
          ctx.font = `${10 * devicePixelRatio}px system-ui`;
          ctx.textAlign = "right";
          ctx.fillText(h.label, u.bbox.left + u.bbox.width - 4, y - 4 * devicePixelRatio);
          ctx.restore();
        }],
      },
    };
    plot.current = new uPlot(opts, data as uPlot.AlignedData, el);
    const ro = new ResizeObserver(() => plot.current?.setSize({ width: el.clientWidth, height }));
    ro.observe(el);
    return () => {
      ro.disconnect();
      plot.current?.destroy();
      plot.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [series.map((s) => s.label).join("|"), height, xTime]);

  useEffect(() => {
    const p = plot.current;
    if (!p) return;
    p.setData(data as uPlot.AlignedData);
  }, [data, hline?.value]);

  return (
    <div className="chart">
      <div className="title">
        <h2>{title}</h2>
        {unit && <span className="muted">{unit}</span>}
        <span className="sp" />
        <div className="legend">
          {series.map((s, i) => (
            <span key={s.label} style={{ ["--c" as any]: s.color ?? PALETTE[i % PALETTE.length] }}>{s.label}</span>
          ))}
        </div>
        {right}
      </div>
      <div ref={ref} />
    </div>
  );
}
