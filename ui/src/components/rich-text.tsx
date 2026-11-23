/** Contract text with `code` spans (contracts/topologies.yaml notes): backticks become inline code. */
import * as React from "react";

export function RichText({ text }: { text: string | null | undefined }) {
  if (!text) return null;
  const parts = text.split(/(`[^`]+`)/g);
  return <>{parts.map((p, i) => (p.startsWith("`") && p.endsWith("`") && p.length > 2
    ? <code key={i} className="rounded bg-surface-2 px-1 py-px font-mono text-[0.92em]">{p.slice(1, -1)}</code>
    : <React.Fragment key={i}>{p}</React.Fragment>))}</>;
}
