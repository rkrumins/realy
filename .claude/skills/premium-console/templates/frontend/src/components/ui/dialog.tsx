import * as DialogPrimitive from "@radix-ui/react-dialog";
import { X } from "lucide-react";
import * as React from "react";
import { cn } from "@/lib/cn";

export const Dialog = DialogPrimitive.Root;
export const DialogTrigger = DialogPrimitive.Trigger;
export const DialogClose = DialogPrimitive.Close;

export function DialogContent({ className, children, title, description, side, ...p }: React.ComponentProps<typeof DialogPrimitive.Content> & {
  title?: React.ReactNode; description?: React.ReactNode; side?: "center" | "right";
}) {
  return (
    <DialogPrimitive.Portal>
      <DialogPrimitive.Overlay className="fixed inset-0 z-50 bg-black/50 backdrop-blur-[2px]" />
      <DialogPrimitive.Content
        className={cn(
          "fixed z-50 border border-border bg-surface shadow-2xl focus:outline-none",
          side === "right"
            ? "inset-y-0 right-0 flex h-full w-full max-w-xl flex-col rounded-none border-y-0 border-r-0"
            : "left-1/2 top-[10vh] w-[calc(100vw-2rem)] max-w-lg -translate-x-1/2 rounded-2xl",
          className,
        )}
        {...p}
      >
        {(title || description) && (
          <div className="border-b border-border px-5 py-4 pr-12">
            {title && <DialogPrimitive.Title className="text-[15px] font-semibold">{title}</DialogPrimitive.Title>}
            {description && <DialogPrimitive.Description className="mt-1 text-[13px] text-muted">{description}</DialogPrimitive.Description>}
          </div>
        )}
        {!title && <DialogPrimitive.Title className="sr-only">Dialog</DialogPrimitive.Title>}
        {children}
        <DialogPrimitive.Close className="absolute right-3 top-3 rounded-lg p-1.5 text-subtle hover:bg-surface-2 hover:text-fg cursor-pointer" aria-label="Close">
          <X className="size-4" />
        </DialogPrimitive.Close>
      </DialogPrimitive.Content>
    </DialogPrimitive.Portal>
  );
}
