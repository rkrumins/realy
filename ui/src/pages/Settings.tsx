import { useQuery } from "@tanstack/react-query";
import { ExternalLink, LogOut, Moon, Sun } from "lucide-react";
import { toast } from "sonner";
import { PageHeader, Stat } from "@/components/common";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardBody, CardHeader } from "@/components/ui/card";
import { Segmented } from "@/components/ui/controls";
import { api } from "@/lib/api";
import { useEnv } from "@/lib/hooks";
import { live, useLiveVersion } from "@/lib/live";
import { setTheme, useTheme } from "@/lib/theme";

export default function Settings() {
  const theme = useTheme();
  const me = useQuery({ queryKey: ["me"], queryFn: api.me, retry: 0 });
  const env = useEnv();
  useLiveVersion();
  const links = env.data?.links ?? {};
  return (
    <>
      <PageHeader eyebrow="Settings" title="Console settings" subtitle="Appearance, session and where this console sends its requests." />
      <div className="grid grid-cols-1 gap-5 lg:grid-cols-2">
        <Card>
          <CardHeader title="Appearance" subtitle="Stored in this browser only." />
          <CardBody>
            <Segmented value={theme} onChange={(t) => setTheme(t)}
              options={[{ value: "dark", label: <span className="inline-flex items-center gap-1.5"><Moon className="size-3.5" />Dark</span> },
                { value: "light", label: <span className="inline-flex items-center gap-1.5"><Sun className="size-3.5" />Light</span> }]} />
          </CardBody>
        </Card>
        <Card>
          <CardHeader title="Session" subtitle="Optional token login (REALY_CONSOLE_TOKEN)." />
          <CardBody>
            <Stat label="Authentication" value={me.data?.auth_required ? <Badge tone="accent">token required</Badge> : <Badge>open (no token configured)</Badge>} />
            <Stat label="Signed in" value={me.data?.authenticated ? "yes" : "no"} />
            {me.data?.auth_required && (
              <Button variant="outline" size="sm" className="mt-3" onClick={async () => {
                await api.logout();
                toast.success("Signed out");
                location.assign("/");
              }}><LogOut />Sign out</Button>
            )}
          </CardBody>
        </Card>
        <Card className="lg:col-span-2">
          <CardHeader title="Endpoints" subtitle="The console API is served from this origin; the UI proxies nothing else." />
          <CardBody>
            <Stat label="Console API" value={<span className="font-mono text-[12px]">{location.origin}/api</span>} />
            <Stat label="Live telemetry" value={<span className="font-mono text-[12px]">{location.origin.replace(/^http/, "ws")}/api/live · {live.connected ? "connected" : "disconnected"}</span>} />
            <Stat label="OpenAPI" value={<a className="inline-flex items-center gap-1 text-accent hover:underline" href="/api/swagger" target="_blank" rel="noreferrer">/api/swagger<ExternalLink className="size-3" /></a>} />
            {Object.entries(links).filter(([, v]) => v).map(([k, v]) => (
              <Stat key={k} label={k[0].toUpperCase() + k.slice(1)} value={<a className="inline-flex items-center gap-1 text-accent hover:underline" href={v as string} target="_blank" rel="noreferrer">{v}<ExternalLink className="size-3" /></a>} />
            ))}
          </CardBody>
        </Card>
      </div>
    </>
  );
}
