/**
 * Latency distributions exactly as java/common LatencyModel samples them (proto3-JSON LatencyDist):
 * fixed | uniform | normal(clamped) | lognormal(median, p99, cap) | bimodal(lognormal body + uniform
 * tail) | empirical (piecewise-linear inverse CDF). Used for the builder's live percentile preview.
 */
export const Z99 = 2.3263478740408408;

export type DistKind = "none" | "fixed" | "uniform" | "normal" | "lognormal" | "bimodal" | "empirical";
export interface LatencyDist {
  fixed?: { ms?: number };
  uniform?: { min_ms?: number; max_ms?: number };
  normal?: { mean_ms?: number; sd_ms?: number; min_ms?: number; max_ms?: number };
  lognormal?: { median_ms?: number; p99_ms?: number; max_ms?: number };
  bimodal?: { median_ms?: number; p99_ms?: number; tail_prob?: number; tail_min_ms?: number; tail_max_ms?: number };
  empirical?: { quantiles?: number[]; ms?: number[] };
  error_rate?: number;
  timeout_rate?: number;
}

export function distKind(d: LatencyDist | null | undefined): DistKind {
  if (!d) return "none";
  for (const k of ["fixed", "uniform", "normal", "lognormal", "bimodal", "empirical"] as const) if (d[k]) return k;
  return "none";
}

/** Seeded PRNG (mulberry32) so previews are stable while editing. */
export function rng(seed = 42): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function gaussian(r: () => number): number {
  let u1 = r();
  const u2 = r();
  if (u1 < 1e-300) u1 = 1e-300;
  return Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
}

function lnParams(median = 1, p99 = 1): [number, number] {
  const m = Math.max(median, 1e-6);
  const p = Math.max(p99, m);
  return [Math.log(m), (Math.log(p) - Math.log(m)) / Z99];
}

export function empiricalAt(q: number[], ms: number[], u: number): number {
  const n = Math.min(q.length, ms.length);
  if (!n) return 0;
  if (u <= q[0]) return ms[0];
  for (let i = 1; i < n; i++) {
    if (u <= q[i]) {
      const t = q[i] === q[i - 1] ? 0 : (u - q[i - 1]) / (q[i] - q[i - 1]);
      return ms[i - 1] + t * (ms[i] - ms[i - 1]);
    }
  }
  return ms[n - 1];
}

/** One sample in ms (NaN for an injected error, +Infinity for an injected timeout). */
export function sampleOne(d: LatencyDist, r: () => number): number {
  const u = r();
  const er = d.error_rate ?? 0;
  const tr = d.timeout_rate ?? 0;
  let v: number;
  switch (distKind(d)) {
    case "fixed":
      v = d.fixed?.ms ?? 0;
      break;
    case "uniform": {
      const lo = d.uniform?.min_ms ?? 0;
      v = lo + r() * ((d.uniform?.max_ms ?? 0) - lo);
      break;
    }
    case "normal": {
      const n = d.normal!;
      const x = (n.mean_ms ?? 0) + (n.sd_ms ?? 0) * gaussian(r);
      v = Math.min(n.max_ms && n.max_ms > 0 ? n.max_ms : Infinity, Math.max(n.min_ms ?? 0, x));
      break;
    }
    case "lognormal": {
      const [mu, sigma] = lnParams(d.lognormal?.median_ms, d.lognormal?.p99_ms);
      const x = Math.exp(mu + sigma * gaussian(r));
      const cap = d.lognormal?.max_ms ?? 0;
      v = cap > 0 ? Math.min(x, cap) : x;
      break;
    }
    case "bimodal": {
      const b = d.bimodal!;
      if (r() < (b.tail_prob ?? 0)) {
        v = (b.tail_min_ms ?? 0) + r() * ((b.tail_max_ms ?? 0) - (b.tail_min_ms ?? 0));
      } else {
        const [mu, sigma] = lnParams(b.median_ms, b.p99_ms);
        v = Math.exp(mu + sigma * gaussian(r));
      }
      break;
    }
    case "empirical":
      v = empiricalAt(d.empirical?.quantiles ?? [], d.empirical?.ms ?? [], r());
      break;
    default:
      v = 0;
  }
  if (u < er) return NaN;
  if (u < er + tr) return Infinity;
  return Math.max(0, v);
}

export interface DistPreview {
  p50: number;
  p90: number;
  p99: number;
  p999: number;
  max: number;
  errorRate: number;
  timeoutRate: number;
  /** CDF curve points (ms, cumulative fraction) */
  cdf: { ms: number; q: number }[];
  /** histogram (bucket start ms, density) */
  hist: { ms: number; n: number }[];
}

export function quantile(sorted: number[], q: number): number {
  if (!sorted.length) return 0;
  const pos = (sorted.length - 1) * q;
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo);
}

export function preview(d: LatencyDist | null | undefined, n = 20000, seed = 7): DistPreview | null {
  if (!d || distKind(d) === "none") return null;
  const r = rng(seed);
  const vals: number[] = [];
  let errors = 0;
  let timeouts = 0;
  for (let i = 0; i < n; i++) {
    const v = sampleOne(d, r);
    if (Number.isNaN(v)) errors++;
    else if (!Number.isFinite(v)) timeouts++;
    else vals.push(v);
  }
  vals.sort((a, b) => a - b);
  const max = vals.length ? vals[vals.length - 1] : 0;
  const top = Math.max(quantile(vals, 0.999), 1e-9);
  const cdf = Array.from({ length: 41 }, (_, i) => {
    const q = i / 40;
    return { ms: quantile(vals, Math.min(q, 0.999)), q };
  });
  const buckets = 36;
  const hist = Array.from({ length: buckets }, (_, i) => ({ ms: (top * i) / buckets, n: 0 }));
  for (const v of vals) {
    const b = Math.min(buckets - 1, Math.floor((v / top) * buckets));
    hist[b].n++;
  }
  return {
    p50: quantile(vals, 0.5), p90: quantile(vals, 0.9), p99: quantile(vals, 0.99), p999: quantile(vals, 0.999),
    max, errorRate: errors / n, timeoutRate: timeouts / n, cdf, hist,
  };
}

/** Default parameters when the user switches a distribution's kind in the editor. */
export function defaultDist(kind: DistKind, prev?: LatencyDist | null): LatencyDist | null {
  const keep = { error_rate: prev?.error_rate, timeout_rate: prev?.timeout_rate };
  const clean = (d: LatencyDist) => {
    if (!keep.error_rate) delete d.error_rate;
    if (!keep.timeout_rate) delete d.timeout_rate;
    return d;
  };
  switch (kind) {
    case "fixed": return clean({ fixed: { ms: 10 }, ...keep });
    case "uniform": return clean({ uniform: { min_ms: 30, max_ms: 100 }, ...keep });
    case "normal": return clean({ normal: { mean_ms: 20, sd_ms: 5, min_ms: 0 }, ...keep });
    case "lognormal": return clean({ lognormal: { median_ms: 8, p99_ms: 25 }, ...keep });
    case "bimodal": return clean({ bimodal: { median_ms: 10, p99_ms: 30, tail_prob: 0.01, tail_min_ms: 150, tail_max_ms: 400 }, ...keep });
    case "empirical": return clean({ empirical: { quantiles: [0, 0.5, 0.9, 0.99, 1], ms: [1, 5, 12, 40, 120] }, ...keep });
    default: return null;
  }
}
