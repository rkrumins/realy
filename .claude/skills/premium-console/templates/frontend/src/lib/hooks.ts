import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";
import { api, isNotFound } from "./api";
import { buildSeries, live, useLiveVersion } from "./live";
import type { EnvResponse, HealthStatus, Status } from "./types";

/** Live status: WebSocket-driven, falls back to polling /api/status every 3 s while the socket is down. */
export function useStatus(): Status | null {
  useLiveVersion();
  const q = useQuery({ queryKey: ["status"], queryFn: api.status, refetchInterval: live.connected ? false : 3000 });
  return live.status ?? q.data ?? null;
}

export function useLiveSeries(windowS = 300, jobId?: string | null) {
  const v = useLiveVersion();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  return useMemo(() => buildSeries(windowS, jobId === undefined ? undefined : jobId), [v, windowS, jobId]);
}

/** Dependency health; null when this backend has no /api/env (render "not available", not an error). */
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
    refetchInterval: 15_000,
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

/** Epoch seconds, re-rendering every `everyMs` (for "x s ago" and elapsed timers). */
export function useNow(everyMs = 1000): number {
  const [now, setNow] = useState(() => Date.now() / 1000);
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now() / 1000), everyMs);
    return () => clearInterval(t);
  }, [everyMs]);
  return now;
}

/** Seconds elapsed since `startedAt` (epoch s), ticking once a second. */
export function useElapsed(startedAt: number | null | undefined): number | null {
  const now = useNow(1000);
  return startedAt ? Math.max(0, now - startedAt) : null;
}
