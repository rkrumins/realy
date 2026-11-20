import * as React from "react";
import { cn } from "@/lib/cn";

export const Input = React.forwardRef<HTMLInputElement, React.InputHTMLAttributes<HTMLInputElement>>(({ className, ...p }, ref) => (
  <input
    ref={ref}
    className={cn(
      "h-9 w-full min-w-0 rounded-lg border border-border bg-surface-2/60 px-3 text-sm text-fg placeholder:text-subtle",
      "transition-colors focus:border-accent/60 focus:bg-surface focus:outline-none focus:ring-2 focus:ring-accent/20 num",
      className,
    )}
    {...p}
  />
));
Input.displayName = "Input";

export const Textarea = React.forwardRef<HTMLTextAreaElement, React.TextareaHTMLAttributes<HTMLTextAreaElement>>(({ className, ...p }, ref) => (
  <textarea
    ref={ref}
    className={cn(
      "w-full rounded-lg border border-border bg-surface-2/60 px-3 py-2 text-sm text-fg placeholder:text-subtle",
      "focus:border-accent/60 focus:outline-none focus:ring-2 focus:ring-accent/20",
      className,
    )}
    {...p}
  />
));
Textarea.displayName = "Textarea";

export function Label({ className, children, hint, ...p }: React.LabelHTMLAttributes<HTMLLabelElement> & { hint?: React.ReactNode }) {
  return (
    <label className={cn("flex flex-col gap-1.5 text-[12.5px] font-medium text-muted", className)} {...p}>
      {children}
      {hint && <span className="-mt-0.5 text-[11px] font-normal leading-4 text-subtle">{hint}</span>}
    </label>
  );
}

/** Labelled numeric field (empty string -> undefined). */
export function NumberField({ label, value, onChange, unit, step, min, max, hint, className, placeholder }: {
  label: React.ReactNode; value: number | undefined | null; onChange: (v: number | undefined) => void; unit?: string;
  step?: number; min?: number; max?: number; hint?: React.ReactNode; className?: string; placeholder?: string;
}) {
  return (
    <Label className={className} hint={hint}>
      {label}
      <div className="relative">
        <Input
          type="number"
          value={value ?? ""}
          step={step}
          min={min}
          max={max}
          placeholder={placeholder}
          onChange={(e) => onChange(e.target.value === "" ? undefined : Number(e.target.value))}
          className={unit ? "pr-12" : undefined}
        />
        {unit && <span className="pointer-events-none absolute inset-y-0 right-3 flex items-center text-xs text-subtle">{unit}</span>}
      </div>
    </Label>
  );
}
