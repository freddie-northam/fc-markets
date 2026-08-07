/**
 * The Rust API, typed by hand.
 *
 * Section 6 of the design: no OpenAPI document and no generated client. Six
 * endpoints and four types do not pay for the tooling. That trade reverses at
 * about fifteen endpoints.
 */

const BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8090";

export type Market = {
  id: string;
  platform: "playstation" | "pc";
  game_code: string;
};

export type Asset = {
  id: string;
  name: string;
  rating: number | null;
  rarity: string;
  version: string;
  position: string | null;
};

export type PricePoint = {
  observed_at: string;
  price: number;
  source: string;
  market_id: string;
};

/**
 * One row of the class median. `value_ratio` is null when the class holds too
 * few cards to say anything, and null is the honest answer: a class of one
 * returns the card's own price as the median and a ratio of exactly 1.0, which
 * reads as signal and is not.
 */
export type ClassValue = {
  asset_id: string;
  name: string;
  price: number;
  median_price: number | null;
  class_size: number | null;
  value_ratio: number | null;
  at_floor: boolean;
};

/** Thrown when the API answers, but not with success. */
export class ApiError extends Error {
  constructor(readonly status: number, path: string) {
    super(`${path} returned ${status}`);
  }
}

async function get<T>(path: string): Promise<T> {
  const response = await fetch(`${BASE}${path}`, {
    // Live ledger data. Nothing here may be served from a build-time cache.
    cache: "no-store",
  });
  if (!response.ok) {
    throw new ApiError(response.status, path);
  }
  return (await response.json()) as T;
}

export function getMarkets(): Promise<Market[]> {
  return get<Market[]>("/markets");
}

export function getAsset(id: string): Promise<Asset> {
  return get<Asset>(`/assets/${encodeURIComponent(id)}`);
}

export function getValuations(marketId: string): Promise<ClassValue[]> {
  return get<ClassValue[]>(`/markets/${encodeURIComponent(marketId)}/valuations`);
}

export function getPrices(assetId: string, marketId: string): Promise<PricePoint[]> {
  // The market is always supplied. A card trades independently on each platform,
  // so a series that mixes two markets is not a price history.
  return get<PricePoint[]>(
    `/assets/${encodeURIComponent(assetId)}/prices?market=${encodeURIComponent(marketId)}`,
  );
}

/** Coins, grouped for readability. Prices reach eight figures. */
export function formatCoins(value: number): string {
  return new Intl.NumberFormat("en-GB").format(value);
}
