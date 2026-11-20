import * as React from "react";
import { cn } from "@/lib/cn";

export function Card({ className, ...p }: React.HTMLAttributes<HTMLDivElement>) {
  return <div className={cn("rounded-2xl border border-border bg-surface shadow-card", className)} {...p} />;
}

export function CardHeader({ className, title, subtitle, actions, icon, children }: {
  className?: string; title?: React.ReactNode; subtitle?: React.ReactNode; actions?: React.ReactNode;
  icon?: React.ReactNode; children?: React.ReactNode;
}) {
  return (
    <div className={cn("flex flex-wrap items-start gap-3 px-5 pt-4 pb-3", className)}>
      {icon && <div className="mt-0.5 text-muted [&_svg]:size-4">{icon}</div>}
      <div className="min-w-0 flex-1">
        {title && <h3 className="text-[14px] font-semibold tracking-tight text-fg">{title}</h3>}
        {subtitle && <p className="mt-0.5 text-[12.5px] text-subtle">{subtitle}</p>}
        {children}
      </div>
      {actions && <div className="flex shrink-0 items-center gap-2">{actions}</div>}
    </div>
  );
}

export function CardBody({ className, ...p }: React.HTMLAttributes<HTMLDivElement>) {
  return <div className={cn("px-5 pb-5", className)} {...p} />;
}
