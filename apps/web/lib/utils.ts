import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

/** The shadcn class helper: merge conditional classes, last Tailwind rule wins. */
export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}
