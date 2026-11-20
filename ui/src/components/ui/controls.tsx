import * as CheckboxPrimitive from "@radix-ui/react-checkbox";
import * as SliderPrimitive from "@radix-ui/react-slider";
import * as SwitchPrimitive from "@radix-ui/react-switch";
import { Check } from "lucide-react";
import * as React from "react";
import { cn } from "@/lib/cn";

export function Switch({ checked, onCheckedChange, label, className, id }: {
  checked: boolean; onCheckedChange: (v: boolean) => void; label?: React.ReactNode; className?: string; id?: string;
}) {
  return (
    <label className={cn("inline-flex cursor-pointer items-center gap-2.5 text-[13px] text-muted", className)} htmlFor={id}>
      <SwitchPrimitive.Root id={id} checked={checked} onCheckedChange={onCheckedChange}
        className="relative h-5 w-9 shrink-0 rounded-full border border-border bg-surface-3 transition-colors data-[state=checked]:border-accent data-[state=checked]:bg-accent cursor-pointer">
        <SwitchPrimitive.Thumb className="block size-4 translate-x-0.5 rounded-full bg-white shadow transition-transform data-[state=checked]:translate-x-[17px]" />
      </SwitchPrimitive.Root>
      {label && <span className="text-fg">{label}</span>}
    </label>
  );
}

export function Slider({ value, onValueChange, onValueCommit, min, max, step, className, ariaLabel }: {
  value: number; onValueChange: (v: number) => void; onValueCommit?: (v: number) => void; min: number; max: number;
  step?: number; className?: string; ariaLabel?: string;
}) {
  return (
    <SliderPrimitive.Root value={[value]} min={min} max={max} step={step ?? 1}
      onValueChange={(v) => onValueChange(v[0])} onValueCommit={(v) => onValueCommit?.(v[0])}
      className={cn("relative flex h-5 w-full touch-none select-none items-center", className)}>
      <SliderPrimitive.Track className="relative h-1.5 grow overflow-hidden rounded-full bg-surface-3">
        <SliderPrimitive.Range className="absolute h-full rounded-full bg-[linear-gradient(90deg,#6366f1,#22d3ee)]" />
      </SliderPrimitive.Track>
      <SliderPrimitive.Thumb aria-label={ariaLabel}
        className="block size-4 rounded-full border-2 border-accent bg-white shadow-md focus:outline-none focus:ring-4 focus:ring-accent/20 cursor-grab" />
    </SliderPrimitive.Root>
  );
}

export function Checkbox({ checked, onCheckedChange, label, className }: {
  checked: boolean; onCheckedChange: (v: boolean) => void; label?: React.ReactNode; className?: string;
}) {
  return (
    <label className={cn("inline-flex cursor-pointer items-center gap-2 text-[13px] text-fg", className)}>
      <CheckboxPrimitive.Root checked={checked} onCheckedChange={(v) => onCheckedChange(v === true)}
        className="flex size-4 shrink-0 items-center justify-center rounded-[5px] border border-border-strong bg-surface-2 data-[state=checked]:border-accent data-[state=checked]:bg-accent cursor-pointer">
        <CheckboxPrimitive.Indicator><Check className="size-3 text-accent-fg" strokeWidth={3} /></CheckboxPrimitive.Indicator>
      </CheckboxPrimitive.Root>
      {label}
    </label>
  );
}

export function Skeleton({ className }: { className?: string }) {
  return <div className={cn("skeleton h-4", className)} />;
}

export function Kbd({ children, className }: { children: React.ReactNode; className?: string }) {
  return <kbd className={cn("inline-flex h-5 min-w-5 items-center justify-center rounded-md border border-border bg-surface-2 px-1.5 font-mono text-[10.5px] text-muted", className)}>{children}</kbd>;
}

export function Separator({ className, vertical }: { className?: string; vertical?: boolean }) {
  return <div className={cn(vertical ? "h-full w-px" : "h-px w-full", "bg-border", className)} />;
}

/** Segmented control for a handful of mutually exclusive options. */
export function Segmented<T extends string>({ value, onChange, options, className, size = "default" }: {
  value: T; onChange: (v: T) => void; options: { value: T; label: React.ReactNode }[]; className?: string; size?: "sm" | "default";
}) {
  return (
    <div className={cn("inline-flex rounded-lg border border-border bg-surface-2/60 p-0.5", className)} role="radiogroup">
      {options.map((o) => (
        <button key={o.value} type="button" role="radio" aria-checked={value === o.value} onClick={() => onChange(o.value)}
          className={cn(
            "rounded-md px-2.5 font-medium transition-colors cursor-pointer",
            size === "sm" ? "h-6 text-[12px]" : "h-7 text-[13px]",
            value === o.value ? "bg-surface text-fg shadow-sm" : "text-muted hover:text-fg",
          )}>
          {o.label}
        </button>
      ))}
    </div>
  );
}
