import * as DM from "@radix-ui/react-dropdown-menu";
import * as React from "react";
import { cn } from "@/lib/cn";

export const Dropdown = DM.Root;
export const DropdownTrigger = DM.Trigger;

export function DropdownContent({ className, ...p }: React.ComponentProps<typeof DM.Content>) {
  return (
    <DM.Portal>
      <DM.Content sideOffset={6} align="end" className={cn("z-50 min-w-48 rounded-xl border border-border bg-surface p-1 shadow-xl", className)} {...p} />
    </DM.Portal>
  );
}

export function DropdownItem({ className, ...p }: React.ComponentProps<typeof DM.Item>) {
  return <DM.Item className={cn("flex cursor-pointer select-none items-center gap-2 rounded-lg px-2.5 py-1.5 text-sm text-fg outline-none data-[highlighted]:bg-surface-2 [&_svg]:size-4 [&_svg]:text-muted", className)} {...p} />;
}

export function DropdownLabel({ className, ...p }: React.ComponentProps<typeof DM.Label>) {
  return <DM.Label className={cn("px-2.5 py-1.5 text-[11px] font-semibold uppercase tracking-wider text-subtle", className)} {...p} />;
}

export const DropdownSeparator = () => <DM.Separator className="my-1 h-px bg-border" />;
