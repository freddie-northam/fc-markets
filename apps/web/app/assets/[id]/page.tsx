import Link from "next/link";
import { notFound } from "next/navigation";
import type { UTCTimestamp } from "lightweight-charts";
import { PriceChart, type ChartPoint } from "@/components/price-chart";
import { Card, CardLabel, CardValue } from "@/components/ui/card";
import { formatCoins, getAsset, getMarkets, getPrices, type PricePoint } from "@/lib/api";

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

  const [asset, prices] = await Promise.all([
    getAsset(id).catch(() => null),
    getPrices(id, market.id),
  ]);
  if (!asset) notFound();

  const latest = prices.at(-1) ?? null;

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
        <nav className="flex gap-1.5">
          {markets.map((m) => (
            <Link
              key={m.id}
              href={`/assets/${id}?market=${m.id}`}
              className={
                m.id === market.id
                  ? "rounded border border-neutral-600 bg-neutral-800 px-2.5 py-1 text-xs"
                  : "rounded border border-neutral-800 px-2.5 py-1 text-xs text-neutral-400 hover:text-neutral-200"
              }
            >
              {m.platform}
            </Link>
          ))}
        </nav>
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
          <CardValue>{prices.length}</CardValue>
        </Card>
      </div>

      <PriceChart points={toSeries(prices)} />

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
