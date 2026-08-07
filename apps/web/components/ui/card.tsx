import * as React from "react";
import { cn } from "@/lib/utils";

export function Card({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      className={cn("rounded-lg border border-neutral-800 bg-neutral-950/60 p-4", className)}
      {...props}
    />
  );
}

export function CardLabel({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      className={cn("text-xs font-medium tracking-wide text-neutral-400 uppercase", className)}
      {...props}
    />
  );
}

export function CardValue({ className, ...props }: React.ComponentProps<"div">) {
  return <div className={cn("mt-1 text-2xl font-semibold tabular-nums", className)} {...props} />;
}
