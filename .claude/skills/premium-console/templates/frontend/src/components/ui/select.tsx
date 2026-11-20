import * as SelectPrimitive from "@radix-ui/react-select";
import { Check, ChevronDown } from "lucide-react";
import * as React from "react";
import { cn } from "@/lib/cn";

export interface Option {
  value: string;
  label: React.ReactNode;
  hint?: React.ReactNode;
}

/** Compact select built on Radix. Empty string is not a valid Radix value, so we map it. */
export function Select({ value, onChange, options, placeholder, className, size = "default", ariaLabel }: {
  value: string | undefined; onChange: (v: string) => void; options: Option[]; placeholder?: string;
  className?: string; size?: "sm" | "default"; ariaLabel?: string;
}) {
  const EMPTY = "__empty__";
  return (
    <SelectPrimitive.Root value={value === "" || value === undefined ? (options.some((o) => o.value === "") ? EMPTY : undefined) : value}
      onValueChange={(v) => onChange(v === EMPTY ? "" : v)}>
      <SelectPrimitive.Trigger aria-label={ariaLabel}
        className={cn(
          "inline-flex w-full items-center justify-between gap-2 rounded-lg border border-border bg-surface-2/60 px-3 text-sm text-fg",
          "focus:outline-none focus:ring-2 focus:ring-accent/20 data-[placeholder]:text-subtle cursor-pointer",
          size === "sm" ? "h-8 text-[13px]" : "h-9",
          className,
        )}>
        <span className="min-w-0 truncate text-left"><SelectPrimitive.Value placeholder={placeholder} /></span>
        <SelectPrimitive.Icon><ChevronDown className="size-4 text-subtle" /></SelectPrimitive.Icon>
      </SelectPrimitive.Trigger>
      <SelectPrimitive.Portal>
        <SelectPrimitive.Content position="popper" sideOffset={6}
          className="z-50 max-h-[min(380px,var(--radix-select-content-available-height))] min-w-[var(--radix-select-trigger-width)] overflow-hidden rounded-xl border border-border bg-surface p-1 shadow-xl">
          <SelectPrimitive.Viewport>
            {options.map((o) => (
              <SelectPrimitive.Item key={o.value || EMPTY} value={o.value || EMPTY}
                className="relative flex cursor-pointer select-none items-center gap-2 rounded-lg py-1.5 pr-8 pl-2.5 text-sm text-fg outline-none data-[highlighted]:bg-surface-2">
                <div className="flex min-w-0 flex-col">
                  <SelectPrimitive.ItemText>{o.label}</SelectPrimitive.ItemText>
                  {o.hint && <span className="text-[11.5px] text-subtle">{o.hint}</span>}
                </div>
                <SelectPrimitive.ItemIndicator className="absolute right-2.5"><Check className="size-4 text-accent" /></SelectPrimitive.ItemIndicator>
              </SelectPrimitive.Item>
            ))}
          </SelectPrimitive.Viewport>
        </SelectPrimitive.Content>
      </SelectPrimitive.Portal>
    </SelectPrimitive.Root>
  );
}
