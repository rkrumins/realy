import { useQuery } from "@tanstack/react-query";
import { ChevronsLeft, ChevronsRight, LogOut, Menu, Moon, Search, Sun, X } from "lucide-react";
import * as React from "react";
import { NavLink, Outlet, useLocation, useNavigate } from "react-router";
import { toast } from "sonner";
import { api } from "@/lib/api";
import { cn } from "@/lib/cn";
import { envHealth, useEnv, useStatus } from "@/lib/hooks";
import { pref, setPref, toggleTheme, useTheme } from "@/lib/theme";
import { Dot } from "../ui/badge";
import { Kbd } from "../ui/controls";
import { Tooltip } from "../ui/tooltip";
import { CommandPalette } from "./CommandPalette";
import { NAV } from "./nav";
import { RunStatusPill } from "./RunStatusPill";

function Logo({ collapsed }: { collapsed?: boolean }) {
  return (
    <div className="flex items-center gap-2.5">
      <div className="relative flex size-8 shrink-0 items-center justify-center rounded-[10px] bg-[linear-gradient(135deg,#6366f1_0%,#8b5cf6_50%,#22d3ee_100%)] shadow-[0_6px_20px_-6px_rgba(99,102,241,0.7)]">
        <svg viewBox="0 0 24 24" className="size-[18px] text-white" fill="none" stroke="currentColor" strokeWidth={2.4} strokeLinecap="round" strokeLinejoin="round">
          <path d="M3 12h4l3 -7l4 14l3 -7h4" />
        </svg>
      </div>
      {!collapsed && (
        <div className="leading-none">
          <div className="text-[15px] font-semibold tracking-tight text-fg">PulseBench</div>
          <div className="mt-1 text-[10.5px] font-medium uppercase tracking-[0.14em] text-subtle">fraud bench</div>
        </div>
      )}
    </div>
  );
}

function SidebarNav({ collapsed, onNavigate }: { collapsed: boolean; onNavigate?: () => void }) {
  const status = useStatus();
  const running = Boolean(status?.current);
  return (
    <nav className="flex flex-1 flex-col gap-5 overflow-y-auto px-3 py-4">
      {NAV.map((g) => (
        <div key={g.group}>
          {!collapsed && <div className="mb-1.5 px-2.5 text-[10.5px] font-semibold uppercase tracking-[0.14em] text-subtle">{g.group}</div>}
          {collapsed && <div className="mx-auto mb-2 h-px w-6 bg-border" />}
          <div className="flex flex-col gap-0.5">
            {g.items.map((it) => {
              const Icon = it.icon;
              const link = (
                <NavLink key={it.to} to={it.to} end={it.to === "/"} onClick={onNavigate}
                  className={({ isActive }) => cn(
                    "group relative flex h-9 items-center gap-3 rounded-lg px-2.5 text-[13.5px] font-medium transition-colors",
                    collapsed && "justify-center px-0",
                    isActive
                      ? "bg-accent/12 text-fg before:absolute before:inset-y-2 before:-left-3 before:w-[3px] before:rounded-r-full before:bg-accent"
                      : "text-muted hover:bg-surface-2 hover:text-fg",
                  )}>
                  {({ isActive }) => (
                    <>
                      <Icon className={cn("size-[17px] shrink-0", isActive ? "text-accent" : "text-subtle group-hover:text-muted")} />
                      {!collapsed && <span className="truncate">{it.label}</span>}
                      {it.to === "/live" && running && (
                        <span className={cn("ml-auto", collapsed && "absolute right-1.5 top-1.5")}><Dot tone="accent" pulse /></span>
                      )}
                    </>
                  )}
                </NavLink>
              );
              return collapsed ? <Tooltip key={it.to} content={it.label} side="right">{link}</Tooltip> : link;
            })}
          </div>
        </div>
      ))}
    </nav>
  );
}

function EnvDot() {
  const env = useEnv();
  const h = envHealth(env.data);
  const nav = useNavigate();
  const tone = h === "up" ? "pass" : h === "degraded" ? "warn" : h === "down" ? "fail" : "neutral";
  const down = env.data?.components.filter((c) => c.status !== "up").map((c) => c.name) ?? [];
  const label = h === "up" ? "All systems up" : h === "unknown" ? "Environment status unknown" : `Degraded: ${down.join(", ")}`;
  return (
    <Tooltip content={label} side="bottom">
      <button onClick={() => nav("/environment")} aria-label={label}
        className="inline-flex h-8 items-center gap-2 rounded-full border border-border bg-surface-2/60 px-2.5 text-[12.5px] text-muted hover:text-fg cursor-pointer">
        <Dot tone={tone} pulse={h === "degraded" || h === "down"} />
        <span className="hidden lg:inline">{h === "up" ? "Healthy" : h === "unknown" ? "Env" : h === "down" ? "Down" : "Degraded"}</span>
      </button>
    </Tooltip>
  );
}

function ThemeButton() {
  const t = useTheme();
  return (
    <Tooltip content={t === "dark" ? "Light theme" : "Dark theme"} side="bottom">
      <button onClick={toggleTheme} aria-label="Toggle theme"
        className="inline-flex size-8 items-center justify-center rounded-full border border-border bg-surface-2/60 text-muted hover:text-fg cursor-pointer">
        {t === "dark" ? <Sun className="size-4" /> : <Moon className="size-4" />}
      </button>
    </Tooltip>
  );
}

function SignOut() {
  const me = useQuery({ queryKey: ["me"], queryFn: api.me, retry: 0, staleTime: 60_000 });
  if (!me.data?.auth_required) return null;
  return (
    <Tooltip content="Sign out" side="bottom">
      <button aria-label="Sign out"
        onClick={async () => {
          await api.logout().catch(() => undefined);
          toast.success("Signed out");
          location.assign("/");
        }}
        className="inline-flex size-8 items-center justify-center rounded-full border border-border bg-surface-2/60 text-muted hover:text-fg cursor-pointer">
        <LogOut className="size-4" />
      </button>
    </Tooltip>
  );
}

export function AppShell() {
  const [collapsed, setCollapsed] = React.useState(() => pref("sidebar.collapsed", false));
  const [mobileOpen, setMobileOpen] = React.useState(false);
  const [paletteOpen, setPaletteOpen] = React.useState(false);
  const loc = useLocation();

  React.useEffect(() => setMobileOpen(false), [loc.pathname]);
  React.useEffect(() => {
    const f = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setPaletteOpen((o) => !o);
      }
    };
    window.addEventListener("keydown", f);
    return () => window.removeEventListener("keydown", f);
  }, []);
  const collapse = (v: boolean) => {
    setCollapsed(v);
    setPref("sidebar.collapsed", v);
  };

  return (
    <div className="flex min-h-dvh">
      {/* desktop sidebar */}
      <aside className={cn(
        "sticky top-0 hidden h-dvh shrink-0 flex-col border-r border-border bg-surface/70 backdrop-blur-xl transition-[width] duration-200 md:flex",
        collapsed ? "w-[68px]" : "w-[232px]",
      )}>
        <div className={cn("flex h-14 items-center border-b border-border", collapsed ? "justify-center" : "px-5")}>
          <Logo collapsed={collapsed} />
        </div>
        <SidebarNav collapsed={collapsed} />
        <div className="border-t border-border p-3">
          <button onClick={() => collapse(!collapsed)} aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
            className={cn("flex h-8 w-full items-center gap-2 rounded-lg px-2.5 text-[12.5px] text-subtle hover:bg-surface-2 hover:text-fg cursor-pointer", collapsed && "justify-center px-0")}>
            {collapsed ? <ChevronsRight className="size-4" /> : <><ChevronsLeft className="size-4" />Collapse</>}
          </button>
        </div>
      </aside>

      {/* mobile drawer */}
      {mobileOpen && (
        <div className="fixed inset-0 z-50 md:hidden">
          <div className="absolute inset-0 bg-black/50 backdrop-blur-[2px]" onClick={() => setMobileOpen(false)} />
          <aside className="absolute inset-y-0 left-0 flex w-[260px] flex-col border-r border-border bg-surface shadow-2xl">
            <div className="flex h-14 items-center justify-between border-b border-border px-4">
              <Logo />
              <button onClick={() => setMobileOpen(false)} aria-label="Close menu" className="rounded-lg p-1.5 text-subtle hover:bg-surface-2 cursor-pointer"><X className="size-4" /></button>
            </div>
            <SidebarNav collapsed={false} onNavigate={() => setMobileOpen(false)} />
          </aside>
        </div>
      )}

      <div className="flex min-w-0 flex-1 flex-col">
        <header className="sticky top-0 z-40 flex h-14 items-center gap-2 border-b border-border bg-bg/75 px-4 backdrop-blur-xl md:gap-3 md:px-6">
          <button onClick={() => setMobileOpen(true)} aria-label="Open menu" className="-ml-1 rounded-lg p-1.5 text-muted hover:bg-surface-2 md:hidden cursor-pointer">
            <Menu className="size-5" />
          </button>
          <div className="md:hidden"><Logo collapsed /></div>
          <button onClick={() => setPaletteOpen(true)}
            className="hidden h-8 w-full max-w-[340px] items-center gap-2 rounded-full border border-border bg-surface-2/60 px-3 text-[13px] text-subtle transition hover:border-border-strong hover:text-muted sm:flex cursor-pointer">
            <Search className="size-4" />
            <span className="flex-1 text-left">Search, launch, jump to…</span>
            <Kbd>⌘K</Kbd>
          </button>
          <div className="ml-auto flex items-center gap-2">
            <button onClick={() => setPaletteOpen(true)} aria-label="Search" className="inline-flex size-8 items-center justify-center rounded-full border border-border bg-surface-2/60 text-muted sm:hidden cursor-pointer">
              <Search className="size-4" />
            </button>
            <div className="hidden sm:block"><RunStatusPill /></div>
            <div className="sm:hidden"><RunStatusPill compact /></div>
            <EnvDot />
            <ThemeButton />
            <SignOut />
          </div>
        </header>
        <main className="mx-auto w-full max-w-[1480px] flex-1 px-4 py-6 md:px-8 md:py-8">
          <Outlet />
        </main>
      </div>
      <CommandPalette open={paletteOpen} onOpenChange={setPaletteOpen} />
    </div>
  );
}
