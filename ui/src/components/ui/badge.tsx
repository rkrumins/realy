import { cva, type VariantProps } from "class-variance-authority";
import * as React from "react";
import { cn } from "@/lib/cn";

export const badgeVariants = cva(
  "inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-[11.5px] font-medium leading-4 whitespace-nowrap [&_svg]:size-3",
  {
    variants: {
      tone: {
        neutral: "border-border bg-surface-2 text-muted",
        accent: "border-accent/25 bg-accent/10 text-accent",
        pass: "border-pass/25 bg-pass/10 text-pass",
        fail: "border-fail/25 bg-fail/10 text-fail",
        warn: "border-warn/25 bg-warn/10 text-warn",
        info: "border-info/25 bg-info/10 text-info",
        solidPass: "border-transparent bg-pass text-white dark:text-black",
        solidFail: "border-transparent bg-fail text-white dark:text-black",
      },
    },
    defaultVariants: { tone: "neutral" },
  },
);

export function Badge({ className, tone, ...p }: React.HTMLAttributes<HTMLSpanElement> & VariantProps<typeof badgeVariants>) {
  return <span className={cn(badgeVariants({ tone }), className)} {...p} />;
}

export function Dot({ tone = "neutral", pulse, className }: { tone?: "pass" | "fail" | "warn" | "info" | "neutral" | "accent"; pulse?: boolean; className?: string }) {
  const c = { pass: "bg-pass", fail: "bg-fail", warn: "bg-warn", info: "bg-info", neutral: "bg-subtle", accent: "bg-accent" }[tone];
  return (
    <span className={cn("relative inline-flex size-2 shrink-0", className)}>
      {pulse && <span className={cn("absolute inline-flex size-full animate-ping-slow rounded-full opacity-60", c)} />}
      <span className={cn("relative inline-flex size-2 rounded-full", c)} />
    </span>
  );
}
