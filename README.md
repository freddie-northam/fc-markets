# FC Market

A canonical, replayable ledger of EA Sports FC Ultimate Team market observations.
A quantitative market terminal sits on top of that ledger.

## Status

This project is at the foundation stage. The design exists. Adversarial review is
in progress. No code exists yet.

Read the [design document](docs/superpowers/specs/2026-08-07-fc-market-foundation-design.md)
first. It holds the schema, the ingestion rules and the accepted risks.

## Purpose

Most Ultimate Team sites answer one question. They tell you what a card costs now.
This project aims at the questions that a market terminal answers:

- What happens to the market, and why?
- What usually happens next?
- Which assets are unusually cheap or expensive?
- How does this period compare with the same period in earlier games?

None of that works without the layer below it. The first goal is not a dashboard.
The first goal is to own a growing history of Ultimate Team market prices that does
not depend on any one data provider.

## Core ideas

**A player, an asset, a market and an observation are four different things.** Jude
Bellingham is a player. His FC26 TOTS card is an asset. The FC26 PlayStation market
is a market. A price of 1,420,000 coins at 21:05 is an observation. If you merge
these ideas, the data loses its value later.

**The ledger only adds rows.** The current price is a calculation over history. It
is not a field that we overwrite. We never update a historical row to show today's
value.

**We record two timestamps.** One timestamp says when the market showed the price.
The other says when we imported it. A price from January 2024 that we import in
August 2026 must keep both. Without this split, every reconstructed dataset carries
the pollution of its import date.

**No external identifier becomes our primary key.** Each provider maps into
`asset_source_ids`. The history stays coherent when a provider stops.

**We archive each raw payload before we parse it.** Canonical data is valuable. Raw
data is insurance. A better parser can rebuild the canonical tables from it.

**Valuations use arithmetic, not a trained model.** Fair value comes from hedonic
pricing over observable card attributes. The ledger reproduces every valuation
exactly. No training data is needed, and the method works on day one.

## Stack

PostgreSQL with TimescaleDB. Rust with Axum, SQLx and Tokio. Next.js. S3 compatible
object storage. We add nothing else until something proves that we need it.

## Abbreviations

- **API**: application programming interface
- **EA**: Electronic Arts
- **FUT**: FIFA Ultimate Team
- **SQL**: structured query language
- **STE**: Simplified Technical English
- **TOTS**: Team of the Season

## Licence

See [LICENSE](LICENSE).
