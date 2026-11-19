import { useEffect, useState } from "react";
import { useLive } from "./live";
import Runs from "./pages/Runs";
import Live from "./pages/Live";
import Scenarios from "./pages/Scenarios";
import Reports from "./pages/Reports";

export type Route = { page: string; arg?: string; query: URLSearchParams };

function parse(): Route {
  const h = location.hash.replace(/^#\/?/, "");
  const [path, qs] = h.split("?");
  const [page, ...rest] = path.split("/");
  return { page: page || "runs", arg: rest.join("/") || undefined, query: new URLSearchParams(qs || "") };
}

export const go = (to: string) => {
  location.hash = to.startsWith("#") ? to : `#/${to}`;
};

export default function App() {
  const [route, setRoute] = useState<Route>(parse());
  const live = useLive();
  useEffect(() => {
    const on = () => setRoute(parse());
    window.addEventListener("hashchange", on);
    return () => window.removeEventListener("hashchange", on);
  }, []);
  const cur = live.current;
  const tabs = [["runs", "Runs"], ["live", "Live"], ["scenarios", "Scenarios"], ["reports", "Reports"]];
  return (
    <>
      <header className="top">
        <div className="brand"><span className="dot" />realy console</div>
        <nav className="nav">
          {tabs.map(([k, l]) => (
            <a key={k} href={`#/${k}`} className={route.page === k || (k === "reports" && route.page === "compare") ? "on" : ""}>{l}</a>
          ))}
        </nav>
        <div className="statusbar">
          {cur ? (
            <a href="#/live">
              <span className={`badge ${cur.phase}`}>{cur.phase}</span> <span className="mono">{cur.run_id}</span>
            </a>
          ) : (
            <span>idle</span>
          )}
          {live.queueLen > 0 && <span>{live.queueLen} queued</span>}
          <span title="live telemetry">{live.connected ? (live.kafka ? "● live" : "● live (no Kafka)") : "○ offline"}</span>
        </div>
      </header>
      <main>
        {route.page === "runs" && <Runs />}
        {route.page === "live" && <Live />}
        {route.page === "scenarios" && <Scenarios name={route.arg} />}
        {(route.page === "reports" || route.page === "compare") && <Reports route={route} />}
      </main>
    </>
  );
}
