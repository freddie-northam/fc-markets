import Link from "next/link";
import type { Market } from "@/lib/api";

/**
 * The market switcher, shared by both routes.
 *
 * One widget rendered in two places. Kept as one component because a styling
 * change applied to a copy would leave the other page quietly different, and
 * nothing would catch it.
 */
export function MarketNav({
  markets,
  currentId,
  hrefFor,
}: {
  markets: Market[];
  currentId: string;
  hrefFor: (marketId: string) => string;
}) {
  return (
    <nav className="flex gap-1.5">
      {markets.map((m) => (
        <Link
          key={m.id}
          href={hrefFor(m.id)}
          className={
            m.id === currentId
              ? "rounded border border-neutral-600 bg-neutral-800 px-2.5 py-1 text-xs"
              : "rounded border border-neutral-800 px-2.5 py-1 text-xs text-neutral-400 hover:text-neutral-200"
          }
        >
          {m.platform}
        </Link>
      ))}
    </nav>
  );
}
