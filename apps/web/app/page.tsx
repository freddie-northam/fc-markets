import Link from "next/link";
import { Badge } from "@/components/ui/badge";
import { MarketNav } from "@/components/market-nav";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { formatCoins, getMarkets, getValuations } from "@/lib/api";

/** A Server Component calling the Rust API directly. There is no state library. */
export default async function MarketPage({
  searchParams,
}: {
  searchParams: Promise<{ market?: string }>;
}) {
  const markets = await getMarkets();
  if (markets.length === 0) {
    return <Empty>No market is configured. Run the migrations.</Empty>;
  }

  const requested = (await searchParams).market;
  const market = markets.find((m) => m.id === requested) ?? markets[0];
  const rows = await getValuations(market.id);

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-baseline justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">Market</h1>
          <p className="mt-1 text-sm text-neutral-400">
            Each card measured against the median of its own class: same rarity, same
            version, same rating. A ratio below 1 is cheap for its class.
          </p>
        </div>
        <MarketNav
          markets={markets}
          currentId={market.id}
          hrefFor={(id) => `/?market=${id}`}
        />
      </div>

      {rows.length === 0 ? (
        <Empty>
          No asset has a fresh price in this market. Run <code>fc-market ingest</code>.
        </Empty>
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Card</TableHead>
              <TableHead className="text-right">Price</TableHead>
              <TableHead className="text-right">Class median</TableHead>
              <TableHead className="text-right">Class size</TableHead>
              <TableHead className="text-right">Ratio</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((row) => (
              <TableRow key={row.asset_id}>
                <TableCell>
                  <Link href={`/assets/${row.asset_id}`} className="hover:underline">
                    {row.name}
                  </Link>
                  {row.at_floor && (
                    <Badge tone="floor" className="ml-2">
                      at floor
                    </Badge>
                  )}
                </TableCell>
                <TableCell className="text-right tabular-nums">
                  {formatCoins(row.price)}
                </TableCell>
                <TableCell className="text-right tabular-nums text-neutral-400">
                  {row.median_price === null ? "--" : formatCoins(Math.round(row.median_price))}
                </TableCell>
                <TableCell className="text-right tabular-nums text-neutral-400">
                  {row.class_size ?? "--"}
                </TableCell>
                <TableCell className="text-right">
                  <Ratio value={row.value_ratio} size={row.class_size} />
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}

      <p className="text-xs text-neutral-500">
        A card resting on EA&apos;s minimum listing price is not cheap, it is floored, so it
        is kept out of the median and sorted last. A card whose polls are failing is
        excluded entirely, because a stale price is biased high and would manufacture a
        false buy signal.
      </p>
    </div>
  );
}

/**
 * A thin class says nothing, so it shows nothing. Printing 1.0 for a class of one
 * would read as a signal, and it is only an artefact of the card being measured
 * against itself.
 */
function Ratio({ value, size }: { value: number | null; size: number | null }) {
  if (value === null) {
    return (
      <span className="text-neutral-500" title={`Class of ${size ?? 0}: too thin to judge`}>
        --
      </span>
    );
  }
  const tone = value < 0.9 ? "cheap" : value > 1.1 ? "rich" : "neutral";
  return <Badge tone={tone}>{value.toFixed(2)}</Badge>;
}

function Empty({ children }: { children: React.ReactNode }) {
  return (
    <div className="rounded-lg border border-dashed border-neutral-800 p-8 text-center text-sm text-neutral-400">
      {children}
    </div>
  );
}
