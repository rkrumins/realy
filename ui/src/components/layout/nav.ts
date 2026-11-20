import {
  Activity, BookOpen, GitCompareArrows, History, LayoutDashboard, Rocket, ScanSearch, Server, Settings, SlidersHorizontal,
} from "lucide-react";

export interface NavItem {
  to: string;
  label: string;
  icon: typeof Activity;
  hint: string;
}

export const NAV: { group: string; items: NavItem[] }[] = [
  {
    group: "Operate",
    items: [
      { to: "/", label: "Overview", icon: LayoutDashboard, hint: "health, recent runs, totals" },
      { to: "/launch", label: "Launch", icon: Rocket, hint: "start or schedule a scenario" },
      { to: "/live", label: "Live", icon: Activity, hint: "topology, throughput, latency, control" },
      { to: "/runs", label: "Runs", icon: History, hint: "history, queue and schedules" },
    ],
  },
  {
    group: "Analyse",
    items: [
      { to: "/explorer", label: "Explorer", icon: ScanSearch, hint: "decisions, customers, campaigns" },
      { to: "/compare", label: "Compare", icon: GitCompareArrows, hint: "two runs side by side" },
    ],
  },
  {
    group: "Configure",
    items: [
      { to: "/scenarios", label: "Scenarios", icon: SlidersHorizontal, hint: "visual scenario builder" },
      { to: "/environment", label: "Environment", icon: Server, hint: "Kafka, Flink, Redis, ClickHouse" },
    ],
  },
  {
    group: "Help",
    items: [
      { to: "/docs", label: "Docs", icon: BookOpen, hint: "guides and references" },
      { to: "/settings", label: "Settings", icon: Settings, hint: "theme, session, endpoints" },
    ],
  },
];

export const FLAT_NAV = NAV.flatMap((g) => g.items);
