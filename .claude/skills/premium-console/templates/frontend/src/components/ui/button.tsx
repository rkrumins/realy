import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";
import * as React from "react";
import { cn } from "@/lib/cn";

export const buttonVariants = cva(
  "inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-lg text-sm font-medium transition-all " +
    "disabled:pointer-events-none disabled:opacity-50 [&_svg]:size-4 [&_svg]:shrink-0 cursor-pointer select-none",
  {
    variants: {
      variant: {
        default: "bg-accent text-accent-fg shadow-sm hover:brightness-110 active:brightness-95",
        gradient: "text-white shadow-md bg-[image:var(--gradient-brand)] hover:brightness-110 active:brightness-95",
        secondary: "bg-surface-2 text-fg border border-border hover:bg-surface-3",
        outline: "border border-border-strong text-fg hover:bg-surface-2",
        ghost: "text-muted hover:text-fg hover:bg-surface-2",
        danger: "bg-fail/12 text-fail border border-fail/30 hover:bg-fail/20",
        link: "text-accent underline-offset-4 hover:underline px-0",
      },
      size: {
        sm: "h-8 px-3 text-[13px]",
        default: "h-9 px-4",
        lg: "h-11 px-6 text-[15px]",
        icon: "size-9",
        "icon-sm": "size-8",
      },
    },
    defaultVariants: { variant: "default", size: "default" },
  },
);

export interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement>, VariantProps<typeof buttonVariants> {
  asChild?: boolean;
}

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(({ className, variant, size, asChild, ...props }, ref) => {
  const Comp = asChild ? Slot : "button";
  return <Comp ref={ref} className={cn(buttonVariants({ variant, size }), className)} {...props} />;
});
Button.displayName = "Button";
