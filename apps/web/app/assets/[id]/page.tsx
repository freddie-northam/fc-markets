import Link from "next/link";
import { MarketNav } from "@/components/market-nav";
import { notFound } from "next/navigation";
import type { UTCTimestamp } from "lightweight-charts";
import { PriceChart, type ChartPoint } from "@/components/price-chart";
import { Card, CardLabel, CardValue } from "@/components/ui/card";
import {
  ApiError,
  formatCoins,
  getAsset,
  getMarkets,
  getPrices,
  type PricePoint,
} from "@/lib/api";

/**
 * Null for "the API says this does not exist", rethrow for everything else.
 *
 * A 400 is here because a malformed id never reaches the handler: axum rejects
 * the path before it runs, which is the same answer as not found.
 */
function missingOrRethrow(error: unknown): null {
  if (error instanceof ApiError && (error.status === 404 || error.status === 400)) {
    return null;
  }
  throw error;
}

export default async function AssetPage({
  params,
  searchParams,
}: {
  params: Promise<{ id: string }>;
  searchParams: Promise<{ market?: string }>;
}) {
  const { id } = await params;
  const markets = await getMarkets();
  if (markets.length === 0) notFound();

  const requested = (await searchParams).market;
  const market = markets.find((m) => m.id === requested) ?? markets[0];

  // Only a 404 or a rejected id means the card is missing. Catching everything
  // mapped a database outage to "this asset does not exist", which is the one
  // answer that is certainly wrong, and it did it on every asset page at once.
  // Anything else is rethrown into the error boundary, which says the API is
  // unreachable rather than inventing a verdict.
  const [asset, prices] = await Promise.all([
    getAsset(id).catch(missingOrRethrow),
    getPrices(id, market.id).catch(missingOrRethrow),
  ]);
  if (!asset) notFound();

  const series = prices ?? [];
  const latest = series.at(-1) ?? null;

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-baseline justify-between gap-3">
        <div>
          <Link href="/" className="text-xs text-neutral-500 hover:text-neutral-300">
            &larr; Market
          </Link>
          <h1 className="mt-1 text-lg font-semibold tracking-tight">{asset.name}</h1>
          <p className="mt-1 text-sm text-neutral-400">
            {asset.version} &middot; {asset.rarity} &middot; rating {asset.rating ?? "--"}
            {asset.position ? ` · ${asset.position}` : ""}
          </p>
        </div>
        <MarketNav
          markets={markets}
          currentId={market.id}
          hrefFor={(m) => `/assets/${id}?market=${m}`}
        />
      </div>

      <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
        <Card>
          <CardLabel>Latest price</CardLabel>
          <CardValue>{latest ? formatCoins(latest.price) : "--"}</CardValue>
        </Card>
        <Card>
          <CardLabel>Observed at</CardLabel>
          <CardValue className="text-base font-normal text-neutral-300">
            {latest ? new Date(latest.observed_at).toUTCString() : "--"}
          </CardValue>
        </Card>
        <Card>
          <CardLabel>Recorded changes</CardLabel>
          <CardValue>{series.length}</CardValue>
        </Card>
      </div>

      <PriceChart points={toSeries(series)} />

      <p className="text-xs text-neutral-500">
        The ledger records changes only, so a flat stretch means the price held, not that
        the card went unread. Coverage is recorded separately, which is what lets the two
        be told apart.
      </p>
    </div>
  );
}

/**
 * The chart needs seconds, ascending, with one point per timestamp.
 *
 * Two sources may report the same instant, and the API returns both rows in a
 * deterministic order. The later one wins here so the series stays strictly
 * increasing, which the chart requires.
 */
function toSeries(prices: PricePoint[]): ChartPoint[] {
  const byTime = new Map<number, number>();
  for (const point of prices) {
    byTime.set(Math.floor(new Date(point.observed_at).getTime() / 1000), point.price);
  }
  return [...byTime.entries()]
    .sort(([a], [b]) => a - b)
    .map(([time, value]) => ({ time: time as UTCTimestamp, value }));
}
