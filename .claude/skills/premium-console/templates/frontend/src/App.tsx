import { useQuery } from "@tanstack/react-query";
import { lazy, Suspense, useEffect, useState } from "react";
import { Route, Routes } from "react-router";
import { AppShell } from "./components/layout/AppShell";
import { Skeleton } from "./components/ui/controls";
import { api, isNotFound, onUnauthorized } from "./lib/api";
import { live } from "./lib/live";
import { SignIn } from "./pages/SignIn";

// one lazy chunk per page: the first paint only loads the shell and the page you asked for
const Overview = lazy(() => import("./pages/Overview"));
const Live = lazy(() => import("./pages/Live"));
const Jobs = lazy(() => import("./pages/Jobs"));
const JobDetail = lazy(() => import("./pages/JobDetail"));
const Environment = lazy(() => import("./pages/Environment"));
const Settings = lazy(() => import("./pages/Settings"));
const NotFound = lazy(() => import("./pages/NotFound"));

/** Shaped like a typical page (header, KPI row, big card) so the swap to real content does not jump. */
function PageFallback() {
  return (
    <div className="space-y-4">
      <Skeleton className="h-8 w-64" />
      <Skeleton className="h-4 w-96 max-w-full" />
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        {Array.from({ length: 4 }, (_, i) => <Skeleton key={i} className="h-24" />)}
      </div>
      <Skeleton className="h-72" />
    </div>
  );
}

/** Optional token auth: when the backend requires it and there is no session, show the sign-in screen. */
function useAuthGate() {
  const me = useQuery({
    queryKey: ["me"],
    queryFn: async () => {
      try {
        return await api.me();
      } catch (e) {
        if (isNotFound(e)) return { auth_required: false, authenticated: true };
        throw e;
      }
    },
    retry: 1,
    staleTime: 60_000,
  });
  const [kicked, setKicked] = useState(false);
  useEffect(() => {
    const a = onUnauthorized(() => setKicked(true));
    const b = live.onAuthRequired(() => setKicked(true));
    return () => {
      a();
      b();
    };
  }, []);
  const needs = kicked || (me.data ? me.data.auth_required && !me.data.authenticated : false);
  return { loading: me.isLoading, needs, onSignedIn: () => { setKicked(false); me.refetch(); live.reconnect(); } };
}

const ROUTES = [
  ["/", Overview], ["/live", Live], ["/jobs", Jobs], ["/jobs/:id", JobDetail], ["/environment", Environment],
  ["/settings", Settings], ["*", NotFound],
] as const;

export function App() {
  const auth = useAuthGate();
  if (auth.loading) return <div className="flex min-h-dvh items-center justify-center"><Skeleton className="h-10 w-40" /></div>;
  if (auth.needs) return <SignIn onSignedIn={auth.onSignedIn} />;
  return (
    <Routes>
      <Route element={<AppShell />}>
        {ROUTES.map(([path, C]) => (
          <Route key={path} path={path} element={<Suspense fallback={<PageFallback />}><C /></Suspense>} />
        ))}
      </Route>
    </Routes>
  );
}
