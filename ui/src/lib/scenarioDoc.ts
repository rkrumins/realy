/** Two-way form <-> YAML sync for the scenario builder: immutable path get/set + stable YAML dump. */
import { dump, load } from "js-yaml";

export type Doc = Record<string, unknown>;
export type Path = (string | number)[];

export function getIn<T = unknown>(obj: unknown, path: Path): T | undefined {
  let cur: unknown = obj;
  for (const k of path) {
    if (cur === null || cur === undefined || typeof cur !== "object") return undefined;
    cur = (cur as Record<string | number, unknown>)[k];
  }
  return cur as T | undefined;
}

const isEmptyObj = (v: unknown) => v !== null && typeof v === "object" && !Array.isArray(v) && Object.keys(v as object).length === 0;

/**
 * Returns a copy with `path` set to `value`. `undefined` deletes the key, and objects left empty by a
 * delete are pruned, so clearing a form field removes it from the YAML instead of writing `null`.
 */
export function setIn<T extends Doc>(obj: T, path: Path, value: unknown): T {
  if (!path.length) return value as T;
  const [k, ...rest] = path;
  const base: Record<string | number, unknown> = Array.isArray(obj) ? [...(obj as unknown[])] as unknown as Record<number, unknown> : { ...(obj ?? {}) };
  if (rest.length === 0) {
    if (value === undefined) delete base[k];
    else base[k] = value;
  } else {
    const child = base[k];
    const next = setIn((child && typeof child === "object" ? child : {}) as Doc, rest, value);
    if (value === undefined && isEmptyObj(next)) delete base[k];
    else base[k] = next;
  }
  return base as T;
}

/** Preferred top-level key order (matches the bundled scenarios). */
const ORDER = ["name", "description", "seed", "world", "traffic", "topology", "rules", "fraud", "scoring", "model_server",
  "budget", "guardrails", "sync", "feature_sample_permille", "flink_state", "exactly_once", "expect"];

export function toYaml(doc: Doc): string {
  const ordered: Doc = {};
  for (const k of ORDER) if (k in doc) ordered[k] = doc[k];
  for (const k of Object.keys(doc)) if (!(k in ordered)) ordered[k] = doc[k];
  return dump(ordered, { lineWidth: 120, noRefs: true, flowLevel: -1, sortKeys: false });
}

export function parseYaml(text: string): { doc: Doc | null; error: string | null } {
  try {
    const v = load(text);
    if (v === null || v === undefined) return { doc: {}, error: null };
    if (typeof v !== "object" || Array.isArray(v)) return { doc: null, error: "a scenario must be a YAML mapping" };
    return { doc: v as Doc, error: null };
  } catch (e) {
    return { doc: null, error: (e as Error).message.split("\n")[0] };
  }
}

/** Rough mock-model capacity: workers × batch / service p50 (items/s); null = unbounded. */
export function modelCapacity(workers: number | undefined, maxBatch: number | undefined, p50Ms: number | null, perItemUs = 0): number | null {
  if (!workers) return null;
  const batch = Math.max(1, maxBatch ?? 1);
  if (p50Ms === null || p50Ms <= 0) return null;
  const batchMs = p50Ms + (perItemUs * batch) / 1000;
  return (workers * batch * 1000) / batchMs;
}
