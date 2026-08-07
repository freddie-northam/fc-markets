import * as React from "react";
import { cn } from "@/lib/utils";

type Tone = "neutral" | "cheap" | "rich" | "floor";

const TONES: Record<Tone, string> = {
  neutral: "border-neutral-700 text-neutral-300",
  cheap: "border-emerald-800 bg-emerald-950/60 text-emerald-300",
  rich: "border-amber-800 bg-amber-950/60 text-amber-300",
  floor: "border-neutral-700 bg-neutral-900 text-neutral-400",
};

export function Badge({
  tone = "neutral",
  className,
  ...props
}: React.ComponentProps<"span"> & { tone?: Tone }) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded border px-1.5 py-0.5 text-xs font-medium",
        TONES[tone],
        className,
      )}
      {...props}
    />
  );
}
