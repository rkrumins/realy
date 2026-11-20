import { Activity, History, LayoutDashboard, Server, Settings } from "lucide-react";

export interface NavItem {
  to: string;
  label: string;
  icon: typeof Activity;
  /** one line shown in the command palette */
  hint: string;
}

/** Sidebar groups (verbs: what the operator is doing), also the command palette's "Go to" list. */
export const NAV: { group: string; items: NavItem[] }[] = [
  {
    group: "Operate",
    items: [
      { to: "/", label: "Overview", icon: LayoutDashboard, hint: "health, recent jobs, totals" },
      { to: "/live", label: "Live", icon: Activity, hint: "the running job: steps, throughput, latency, log" },
      { to: "/jobs", label: "Jobs", icon: History, hint: "history and queue" },
    ],
  },
  {
    group: "Configure",
    items: [
      { to: "/environment", label: "Environment", icon: Server, hint: "dependency health and versions" },
      { to: "/settings", label: "Settings", icon: Settings, hint: "theme, session, endpoints" },
    ],
  },
];

export const FLAT_NAV = NAV.flatMap((g) => g.items);
