# FC Market

A canonical, replayable ledger of EA Sports FC Ultimate Team market observations.
A quantitative market terminal sits on top of that ledger.

## Status

The foundation is built and runs end to end. The ledger cannot start, because no
data source is available yet. A `fixture` source stands in: it serves saved
payloads from disk, so every link of the pipeline is genuine except the network
call.

Read the [design document](docs/superpowers/specs/2026-08-07-fc-market-foundation-design.md)
first. It holds the schema, the ingestion rules and the accepted risks.

## Run it

Rust 1.90 or later, Node with pnpm, and Docker.

```sh
docker compose up -d          # TimescaleDB on 5434, MinIO on 9002
cp .env.example .env
cargo run -- migrate          # schema, then the game and its two markets
cargo run -- ingest           # one pass: discover, learn, cover, price
cargo run -- serve            # API on 8090, plus ingestion on an interval

cd apps/web && pnpm install && pnpm dev
```

Ports avoid the usual local collisions: 5434 for PostgreSQL, 9002 and 9003 for
the object store, 8090 for the API.

`cargo test` needs the database running. Each test builds and drops its own.

### Commands

| Command | Purpose |
|---|---|
| `migrate` | Apply pending migrations and stop |
| `ingest` | One ingestion pass and stop |
| `serve` | The API, the interval ingest and the nightly dump |
| `backup` | One `pg_dump -Fc` to the archive bucket |
| `replay <run-id>` | Re-parse one run's archived payloads under the current parser |

`replay` is the one deliberate exception to the append-only rule. It never runs
automatically.

## Operating notes

**`PG_DUMP_PATH` must be at least as new as the server.** The database image
ships a matching `pg_dump`; a host binary is often a different version. Newer is
fine, older is not.

**Restoring is `scripts/restore.sh <dump-file> [target-database]`.** Do not do it
by hand. `timescaledb_pre_restore()` must run before `pg_restore` and
`timescaledb_post_restore()` after it, and skipping the first fails part way
through with a misleading message:

```
ERROR: table "market_observations" is not a hypertable
```

by which point the database holds a partial restore.

The restore path is verified, not assumed. A dump taken by the `backup` command
and pulled back out of the bucket restores with identical row counts, keeps its
hypertables, its compression settings, its already compressed chunks and its
compression policies, and still rejects a duplicate while accepting a
restatement. Pointing the server at the restored copy and running one ingest
reports every known price as `unchanged`, which is what proves identity
resolution and the idempotency index both survived.

**Set the dead man's switch grace period above the largest poll interval**, which
is four hours by default. A run that closes `degraded` or `failed` sends no
heartbeat on purpose, so the drift checks raise their alarm through the same
channel as a dead host.

**The drift thresholds are guesses.** No source exists, so nothing has been
checked against real data. The same applies to the class size of five in the
valuation and to the tier sizes. Check all three before fixing them.

**The fixture ages.** Its saved timestamps are fixed, so after a day the frozen
feed check correctly reports it as stale. That is the check working, not a fault.

## Not built

Named in the design but deliberately absent:

- **`backfill`**. Section 4.1 lists the command, and no other section defines what
  it should do. It waits for a definition rather than a guess.
- **An HTTP source.** `SOURCE_API_KEY` and `HTTP_TIMEOUT_SECONDS` are read into
  the config and nothing consumes them yet. They stay because section 4.5
  requires the source client to set an explicit request timeout, and a provider
  added without one would retry silently and spend the quota twice.
- The event model, indices, signals, delivery, predictions and accounts, which
  section 8 puts outside this milestone.

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
