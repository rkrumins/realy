import { useQuery } from "@tanstack/react-query";
import { ExternalLink, LogOut, Moon, Sun } from "lucide-react";
import { toast } from "sonner";
import { PageHeader, Stat } from "@/components/common";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardBody, CardHeader } from "@/components/ui/card";
import { Segmented } from "@/components/ui/controls";
import { api } from "@/lib/api";
import { live, useLiveVersion } from "@/lib/live";
import { setTheme, useTheme } from "@/lib/theme";

export default function Settings() {
  const theme = useTheme();
  const me = useQuery({ queryKey: ["me"], queryFn: api.me, retry: 0 });
  useLiveVersion();
  return (
    <>
      <PageHeader eyebrow="Settings" title="Console settings" subtitle="Appearance, session, and where this console sends its requests." />
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
          <CardHeader title="Session" subtitle="Optional token login (APP_TOKEN on the service)." />
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
          <CardHeader title="Endpoints" subtitle="The API is served from this origin; the UI talks to nothing else." />
          <CardBody>
            <Stat label="API" value={<span className="font-mono text-[12px]">{location.origin}/api</span>} />
            <Stat label="Live channel" value={<span className="font-mono text-[12px]">{location.origin.replace(/^http/, "ws")}/api/live · {live.connected ? "connected" : "disconnected (polling)"}</span>} />
            <Stat label="OpenAPI" value={<a className="inline-flex items-center gap-1 text-accent hover:underline" href="/api/docs" target="_blank" rel="noreferrer">/api/docs<ExternalLink className="size-3" /></a>} />
          </CardBody>
        </Card>
      </div>
    </>
  );
}
