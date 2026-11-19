import { KeyRound, Loader2 } from "lucide-react";
import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input, Label } from "@/components/ui/input";
import { api } from "@/lib/api";

export function SignIn({ onSignedIn }: { onSignedIn: () => void }) {
  const [token, setToken] = useState("");
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setErr(null);
    try {
      await api.login(token.trim());
      onSignedIn();
    } catch (x) {
      setErr((x as Error).message === "invalid token" ? "That token is not valid." : (x as Error).message);
    } finally {
      setBusy(false);
    }
  };
  return (
    <div className="grid-bg flex min-h-dvh items-center justify-center px-4">
      <form onSubmit={submit} className="w-full max-w-sm rounded-2xl border border-border bg-surface/90 p-7 shadow-2xl backdrop-blur">
        <div className="mb-6 flex items-center gap-3">
          <div className="flex size-10 items-center justify-center rounded-xl bg-[linear-gradient(135deg,#6366f1,#22d3ee)] text-white">
            <KeyRound className="size-5" />
          </div>
          <div>
            <h1 className="text-[17px] font-semibold tracking-tight">Sign in to realy</h1>
            <p className="text-[12.5px] text-muted">This console is protected by an access token.</p>
          </div>
        </div>
        <Label hint="Set by REALY_CONSOLE_TOKEN on the console deployment.">
          Access token
          <Input type="password" autoFocus value={token} onChange={(e) => setToken(e.target.value)} placeholder="••••••••••••" autoComplete="current-password" />
        </Label>
        {err && <div className="mt-3 rounded-lg border border-fail/25 bg-fail/10 px-3 py-2 text-[12.5px] text-fail">{err}</div>}
        <Button type="submit" variant="gradient" className="mt-5 w-full" disabled={!token.trim() || busy}>
          {busy && <Loader2 className="animate-spin" />}Sign in
        </Button>
        <p className="mt-4 text-center text-[11.5px] text-subtle">The session is an HTTP-only cookie valid for 12 hours.</p>
      </form>
    </div>
  );
}
