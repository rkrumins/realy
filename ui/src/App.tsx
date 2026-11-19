import { useQuery } from "@tanstack/react-query";
import { lazy, Suspense, useEffect, useState } from "react";
import { Route, Routes } from "react-router";
import { AppShell } from "./components/layout/AppShell";
import { Skeleton } from "./components/ui/controls";
import { api, isNotFound, onUnauthorized } from "./lib/api";
import { live } from "./lib/live";
import { SignIn } from "./pages/SignIn";

const Overview = lazy(() => import("./pages/Overview"));
const Launch = lazy(() => import("./pages/Launch"));
const Live = lazy(() => import("./pages/Live"));
const Runs = lazy(() => import("./pages/Runs"));
const RunReport = lazy(() => import("./pages/RunReport"));
const Compare = lazy(() => import("./pages/Compare"));
const Explorer = lazy(() => import("./pages/Explorer"));
const Scenarios = lazy(() => import("./pages/Scenarios"));
const Environment = lazy(() => import("./pages/Environment"));
const Docs = lazy(() => import("./pages/Docs"));
const Settings = lazy(() => import("./pages/Settings"));
const NotFound = lazy(() => import("./pages/NotFound"));

function PageFallback() {
  return (
    <div className="space-y-4">
      <Skeleton className="h-8 w-64" />
      <Skeleton className="h-4 w-96" />
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        {Array.from({ length: 4 }, (_, i) => <Skeleton key={i} className="h-24" />)}
      </div>
      <Skeleton className="h-72" />
    </div>
  );
}

/** Optional token auth: when the backend requires it and we have no session, show the sign-in screen. */
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

export function App() {
  const auth = useAuthGate();
  if (auth.loading) return <div className="flex min-h-dvh items-center justify-center"><Skeleton className="h-10 w-40" /></div>;
  if (auth.needs) return <SignIn onSignedIn={auth.onSignedIn} />;
  return (
    <Routes>
      <Route element={<AppShell />}>
        {([
          ["/", Overview], ["/launch", Launch], ["/live", Live], ["/runs", Runs], ["/runs/:id", RunReport],
          ["/compare", Compare], ["/explorer", Explorer], ["/scenarios", Scenarios], ["/environment", Environment],
          ["/docs", Docs], ["/settings", Settings], ["*", NotFound],
        ] as const).map(([path, C]) => (
          <Route key={path} path={path} element={<Suspense fallback={<PageFallback />}><C /></Suspense>} />
        ))}
      </Route>
    </Routes>
  );
}
