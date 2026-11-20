/** Formatting helpers (tabular, compact, unit-aware). */
export const isNum = (v: unknown): v is number => typeof v === "number" && Number.isFinite(v);

export function fmtMs(v: unknown, digits?: number): string {
  if (!isNum(v)) return "—";
  if (v >= 10_000) return `${(v / 1000).toFixed(1)} s`;
  if (v >= 1000) return `${(v / 1000).toFixed(2)} s`;
  const d = digits ?? (v >= 100 ? 0 : v >= 10 ? 1 : 2);
  return `${v.toFixed(d)} ms`;
}

export function fmtNum(v: unknown, digits = 0): string {
  if (!isNum(v)) return "—";
  return v.toLocaleString("en-US", { maximumFractionDigits: digits, minimumFractionDigits: digits });
}

export function fmtCompact(v: unknown, digits = 1): string {
  if (!isNum(v)) return "—";
  const a = Math.abs(v);
  if (a >= 1e9) return `${(v / 1e9).toFixed(digits)}B`;
  if (a >= 1e6) return `${(v / 1e6).toFixed(digits)}M`;
  if (a >= 1e4) return `${(v / 1e3).toFixed(digits)}k`;
  return fmtNum(v, a < 10 && a % 1 ? 2 : 0);
}

export function fmtPct(v: unknown, digits = 1): string {
  if (!isNum(v)) return "—";
  return `${(v * 100).toFixed(digits)}%`;
}

export function fmtUsd(v: unknown): string {
  if (!isNum(v)) return "—";
  return v.toLocaleString("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 });
}

export function fmtBytes(v: unknown): string {
  if (!isNum(v)) return "—";
  const u = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  let x = v;
  while (x >= 1024 && i < u.length - 1) {
    x /= 1024;
    i++;
  }
  return `${x.toFixed(i ? 1 : 0)} ${u[i]}`;
}

export function fmtDuration(seconds: unknown): string {
  if (!isNum(seconds)) return "—";
  const s = Math.max(0, Math.round(seconds));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  if (h < 48) return `${h}h ${m % 60}m`;
  return `${Math.floor(h / 24)}d ${h % 24}h`;
}

export function fmtTime(epochSeconds: unknown): string {
  if (!isNum(epochSeconds) || epochSeconds <= 0) return "—";
  return new Date(epochSeconds * 1000).toLocaleString("en-GB", {
    day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit", second: "2-digit",
  });
}

export function fmtAgo(epochSeconds: unknown): string {
  if (!isNum(epochSeconds) || epochSeconds <= 0) return "—";
  const d = Date.now() / 1000 - epochSeconds;
  if (d < 60) return "just now";
  return `${fmtDuration(d)} ago`;
}

export function fmtSimTime(us: unknown): string {
  if (!isNum(us) || us <= 0) return "—";
  return new Date(us / 1000).toISOString().replace("T", " ").slice(0, 19);
}

/** Run ids look like `<scenario>-YYYYMMDD-HHMMSS`; show the timestamp part compactly. */
export function runStamp(runId: string): string {
  const m = /-(\d{8})-(\d{6})$/.exec(runId);
  if (!m) return runId;
  const [, d, t] = m;
  return `${d.slice(6, 8)}/${d.slice(4, 6)} ${t.slice(0, 2)}:${t.slice(2, 4)}`;
}

export function titleCase(s: string): string {
  return s.replace(/[_-]+/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}
