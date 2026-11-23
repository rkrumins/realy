import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";
import { api, isNotFound } from "./api";
import { buildSeries, live, LiveStore, useLiveVersion } from "./live";
import type { CurrentRun, EnvResponse, HealthStatus, Status } from "./types";

/** The run in progress on the pipeline, whoever started it (this console, the CLI, a Job, another console). */
export function activeRun(status: Status | null | undefined): CurrentRun | null {
  return status?.active ?? status?.current ?? null;
}

/** Live status: WS-driven, falls back to polling /api/status while the socket is down. */
export function useStatus(): Status | null {
  useLiveVersion();
  const q = useQuery({ queryKey: ["status"], queryFn: api.status, refetchInterval: live.connected ? false : 3000 });
  return live.status ?? q.data ?? null;
}

export function useLiveSeries(windowS = 300, runId?: string | null, store: LiveStore = live) {
  const v = useLiveVersion(store);
  return useMemo(() => buildSeries(windowS, runId === undefined ? undefined : runId, store), [v, windowS, runId, store]); // eslint-disable-line react-hooks/exhaustive-deps
}

export function useEnv() {
  return useQuery<EnvResponse | null>({
    queryKey: ["env"],
    queryFn: async () => {
      try {
        return await api.env();
      } catch (e) {
        if (isNotFound(e)) return null;
        throw e;
      }
    },
    refetchInterval: 15000,
    retry: 0,
  });
}

export function envHealth(env: EnvResponse | null | undefined): HealthStatus | "unknown" {
  if (!env) return "unknown";
  const st = env.components.map((c) => c.status);
  if (st.some((s) => s === "down")) return st.every((s) => s === "down") ? "down" : "degraded";
  if (st.some((s) => s === "degraded")) return "degraded";
  return "up";
}

/** Seconds elapsed since `startedAt` (epoch s), ticking once a second. */
export function useElapsed(startedAt: number | null | undefined): number | null {
  const [now, setNow] = useState(() => Date.now() / 1000);
  useEffect(() => {
    if (!startedAt) return;
    const t = setInterval(() => setNow(Date.now() / 1000), 1000);
    return () => clearInterval(t);
  }, [startedAt]);
  return startedAt ? Math.max(0, now - startedAt) : null;
}

export function useMediaQuery(q: string): boolean {
  const [m, setM] = useState(() => typeof window !== "undefined" && window.matchMedia(q).matches);
  useEffect(() => {
    const mq = window.matchMedia(q);
    const f = () => setM(mq.matches);
    mq.addEventListener("change", f);
    f();
    return () => mq.removeEventListener("change", f);
  }, [q]);
  return m;
}
