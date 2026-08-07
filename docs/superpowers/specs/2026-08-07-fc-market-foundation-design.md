# FC Market Terminal: Foundation Design

Date: 2026-08-07
Status: Revision 5. Five adversarial review passes are complete.
Scope: Foundation milestone

## 1. Mission

Build the smallest correct foundation for a quantitative EA Sports FC Ultimate Team
market terminal.

One measure decides success. We must permanently own a growing, canonical,
replayable history of Ultimate Team market observations. Every other feature sits
on top of that ledger.

A second principle applies. The product computes valuations by arithmetic. It does
not use a trained model.

## 2. Data source

The design does not depend on any one data source. This is deliberate.

We reviewed the available market data sources before we wrote this design. No
source that suits a commercial product is available at this date. Therefore the
ledger cannot start yet.

The architecture answers this problem directly:

- Each observation records the source that supplied it.
- Each asset maps to external identifiers through `asset_source_ids`.
- Each raw payload goes to an immutable archive before we parse it.
- Each source module converts provider codes into canonical values that we own.

A new source therefore needs one new module. It does not need a schema change.

Source selection notes are kept outside this repository.

## 3. Decisions

| Decision | Choice | Reason |
|---|---|---|
| Runtime | One small host that runs always | A ledger with gaps is not a ledger |
| Raw archive | S3 compatible object storage | One code path, no storage abstraction |
| Database backup | Nightly `pg_dump` to the same bucket | The database holds identity, so the archive alone cannot rebuild it |
| Polling | Tiered by version and rating | Sources refresh cheap cards slowly |
| Rollups | None | No query reads one, and the market query cannot be one |
| Alerting | One outward heartbeat | A pull check cannot report that its own host is gone |
| Crates | One Rust crate | A second crate would have one consumer |
| API client generation | None yet | Three endpoints do not justify the tools |
| Valuation | Plain queries over the ledger | Continuous aggregates cannot express them |
| Asset coverage | 600 | Tiered polling makes 600 cheaper than 100 |

## 4. Architecture

### 4.1 Repository

```
fc-market/
├── apps/
│   ├── server/
│   │   └── src/
│   │       ├── main.rs         CLI dispatch: ingest, serve, backfill, replay
│   │       ├── ids.rs          AssetId, PlayerId, MarketId
│   │       ├── db.rs           pool and query functions
│   │       ├── archive.rs      raw payload to object storage
│   │       ├── ingest/
│   │       │   ├── mod.rs      run orchestration and run records
│   │       │   └── <source>.rs provider structs stay private here
│   │       ├── valuation.rs    deterministic valuation queries
│   │       └── api.rs          axum router and handlers
│   └── web/                    Next.js application
├── migrations/
├── fixtures/
├── docs/
├── docker-compose.yml
└── Cargo.toml
```

One crate is enough. A separate domain crate and a separate core crate would each
have one consumer. We split the crate when a second consumer exists.

### 4.2 Schema

```sql
games              (id, code, name, released_at NOT NULL)

players            (id, name, ea_base_id UNIQUE)

assets             (id, game_id, player_id, name,
                    -- canonical values that we own, mapped at parse time
                    rating, rarity, version, position, league_id, nation_id,
                    club_id, skill_moves, weak_foot, pace, shooting, passing,
                    dribbling, defending, physicality, playstyle_count,
                    metadata JSONB,
                    first_seen_at, last_seen_at)

asset_poll_state   (asset_id, market_id, poll_interval_seconds,
                    last_polled_at, consecutive_failures,
                    PRIMARY KEY (asset_id, market_id))

asset_source_ids   (asset_id, game_id, source, external_id,
                    UNIQUE (game_id, source, external_id))

markets            (id, game_id, platform, UNIQUE (game_id, platform))

ingest_runs        (id, source, parser_version, started_at, heartbeat_at,
                    finished_at, status, records_seen, records_written,
                    records_rejected, error, metadata JSONB)

market_observations(asset_id      NOT NULL,
                    market_id     NOT NULL,
                    source        NOT NULL,
                    observed_at   NOT NULL,   -- time dimension
                    price         NOT NULL,
                    ingested_at, min_price, max_price,
                    source_ref, ingest_run_id)

ingest_polls       (asset_id      NOT NULL,
                    market_id     NOT NULL,
                    polled_at     NOT NULL,   -- time dimension
                    source_observed_at,       -- nullable by design
                    outcome       NOT NULL,   -- written, unchanged, rejected, no_price
                    run_id,
                    PRIMARY KEY (asset_id, market_id, polled_at))
```

`market_observations` partitions on `observed_at`. `ingest_polls` partitions on
`polled_at`. TimescaleDB forces `NOT NULL` on a partitioning column, so this choice
matters. `source_observed_at` must stay nullable, because it is null in exactly the
case that proves we looked and found nothing.

**Idempotency.** A database constraint gives it. Application code does not:

```sql
UNIQUE (asset_id, market_id, source, observed_at, price)
```

`price` belongs in the key. Without it, a corrected price at an already recorded
timestamp disappears in silence. With it, an identical replay still conflicts, so
replay stays idempotent, and a genuine restatement lands as a second truthful row.

Inserts use `ON CONFLICT DO NOTHING`. TimescaleDB enforces unique constraints on
compressed chunks and supports `ON CONFLICT` from version 2.11. Section 4.8 pins
the version.

`records_written` comes from the affected row count of the insert.

**Timestamps.** Every timestamp column is `TIMESTAMPTZ` and holds UTC. The Rust type
is `chrono::DateTime<Utc>`. Each source module converts. A source timestamp with no
unambiguous offset is a rejected record.

**Canonical dimension values.** `rating`, `rarity`, `version` and `position` hold
values that we own. `league_id`, `nation_id` and `club_id` hold canonical
identifiers that we own. Each source module maps provider codes to these values
when it parses. We add no dimension tables until a second source proves that we
need them. Without this rule a source change would silently move assets between
valuation classes.

**Domains.** `source`, `platform`, `position`, `rarity` and `version` are Rust enums
that serialise to `TEXT`. There is no PostgreSQL enum type, because adding a value
to one is a migration and a rename is worse. A `CHECK` constraint holds the closed
list. Free text lets a provider rename fork an asset's history, so the list stays
closed and each source module maps into it.

- `source`: one value for each provider module.
- `platform`: `playstation`, `pc`. Add a value when a source separates more.
- `rarity`: the base tier. `common`, `rare`.
- `version`: the card variant, including promotional variants. `base`, `totw`,
  `tots`, `toty`, `icon`, `hero`, `winter_wildcards`, and so on.

**`rarity` and `version` are not the same thing, and `version` matters more.** An
89-rated Team of the Season card and an 89-rated base rare gold card share a rating
and a rarity. Late in a game cycle they trade orders of magnitude apart, because
pack supply accumulates for the base card and does not for the promotional one. Any
calculation that groups these two together describes neither.

`bid` and `listings` do not appear. No available source supplies them.

**Indexes.** One index serves the read path:

```sql
CREATE INDEX ON market_observations (market_id, asset_id, observed_at DESC);
```

### 4.3 Why `ingest_polls` exists

`market_observations` records changes only. The insert writes nothing when a price
does not move. Without a second record, the ledger cannot separate a stable price
from an outage. It also cannot measure missing data or prove coverage.

`ingest_polls` records that we looked and what happened. The `outcome` column
matters as much as the row itself. Without it, an asset that fails validation on
every run looks exactly like an asset with a stable price, and its stale value
keeps entering the valuation classes in section 5.

### 4.4 Identity resolution

| Our record | Purpose |
|---|---|
| `players.ea_base_id` | The real footballer |
| `assets` row | One tradeable card |
| `asset_source_ids` | One row for each provider identifier |

Resolution is deterministic. One indexed lookup of `(game_id, source, external_id)`
returns the asset. A map caches the result for the run. There is no fuzzy match.

**The lookup includes `game_id`.** Provider identifiers are not always unique across
titles. An identifier that a provider reuses for the next game would otherwise
resolve onto the previous game's asset and contaminate its history forever.

**Source precedence.** Two sources may hold a row for the same asset, market and
time. Every read query that picks one row must order by `source` after
`observed_at`, so the result stays deterministic.

### 4.5 Ingestion

```
acquire an advisory lock; log and stop if another run holds it
select due assets from asset_poll_state
group the identifiers into batches
for each batch:
  request prices, with an explicit timeout
  ARCHIVE the request envelope and response; stop the batch if this fails
  parse into provider structs that stay inside their module
  validate, then map to canonical observations
  BEGIN
    insert observations with ON CONFLICT DO NOTHING
    insert ingest_polls rows with the outcome for every asset asked about
    update asset_poll_state
  COMMIT
close the run with counts and a status
```

`observed_at` comes from the source. It never comes from our clock. `ingested_at`
comes from our clock.

The three writes form one transaction. A poll row never commits for an asset whose
observation did not commit.

An advisory lock serialises runs. The `serve` interval task and the `ingest`
command share one entry point, so without a lock a slow run and a manual run spend
the quota twice and race on poll state.

The HTTP client sets an explicit request timeout and connect timeout. It has no
retry policy. The next scheduled poll is the retry, and
`asset_poll_state.consecutive_failures` supplies the backoff.

**Asset discovery.** On a slow cadence the run enumerates the source's full asset
list and inserts unseen `(game_id, source, external_id)` pairs. It does not track a
highest-identifier watermark, because a provider can renumber or backfill records.

**Asset metadata.** On the same slow cadence as discovery, the run fetches card
attributes for assets that are new, and for assets whose `last_seen_at` is older
than the metadata cadence. It archives the envelope, maps provider codes to the
canonical domains in section 4.2, and updates the `assets` row.

This step is not optional. Every valuation input in section 5 reads these columns,
and Rule 1 compares them against each payload. Without this step they stay null and
the terminal has nothing to compute.

**Quota.** Each run counts the requests that it issues into `ingest_runs.metadata`
and stops when it reaches the configured daily budget. Discovery and metadata share
the same budget as prices. An HTTP 429 stops the run and marks it `degraded`. It is
not a per-record rejection.

### 4.6 Ingestion rules

**Rule 1. Never map a price by position.** A price response can omit a player,
reorder players or remove duplicates. If the response carries no player identifier,
every later price attaches to the wrong asset. Nothing raises an error. Charts
still draw. We would find the fault years later, and every valuation and signal
above it would be void.

Therefore: request one player for each call when a response carries no identifier.
Pay the quota cost. We can recover lost coverage. We cannot recover a silent wrong
attribution.

When a payload carries a player name, compare it against the resolved asset. A
mismatch is a rejected record, never a write. This catches a provider that
re-points an existing identifier at a different card.

**Do not compare the rating.** Some cards raise their rating during the season by
design, such as Ones to Watch, Path to Glory and Road to the Knockouts, and EA also
refreshes ratings mid season. A rating check would reject every such card on every
poll until the metadata step catches up, and those are high value cards. The name
is stable where the rating is not.

**Rule 2. Validate `observed_at` before it reaches the hypertable.** Reject any
value outside `[game.released_at, ingest_runs.started_at + 5 minutes]`. One bad
timestamp far in the future makes TimescaleDB create a chunk at that date, and that
chunk then sits between us and every range query for the life of the database.

The upper bound uses the run's own `started_at`, not `now()`. A wall clock bound
would accept on replay what it rejected at ingest time, so the same archive would
produce two different tables.

**Rule 3. A missing or ambiguous source timestamp makes a rejected record.** We
count it. We never substitute our own clock. A substitute clock turns an unknown
observation time into a false one.

**Rule 4. One bad record never fails a run.** Each record parses into its own
`Result`. Failures increase `records_rejected`. The log holds the raw fragment for
at most the first ten failures in a run. A provider schema change rejects every
record at once, and unbounded fragment logging would fill the disk and stop the
database. The archive holds the full payload for anything the sample omits.

### 4.7 Polling tiers

The source quota sets asset coverage. Code does not set it.

| Tier | Assets | Interval | Daily requests |
|---|---|---|---|
| A | 50 | 15 min | 4,800 |
| B | 150 | 1 h | 3,600 |
| C | 400 | 4 h | 2,400 |
| Total | 600 | | 10,800 |

"Daily requests" counts provider requests, not observation rows. A response that
carries both platforms costs one request and can write two rows.

**Tier assignment.** A tier is a computable predicate on columns that we already
hold. It is not an editorial judgement:

```sql
poll_interval_seconds = CASE
  WHEN version <> 'base' OR rating >= 88 THEN 900     -- A
  WHEN rating >= 83                      THEN 3600    -- B
  ELSE                                        14400   -- C
END
```

Asset discovery inserts one `asset_poll_state` row for each `(asset, market)` pair
of a covered asset, with the interval that this expression gives. Nothing else
creates rows in that table.

The tier expression and the tier sizes must agree. Check the counts against real
data before the numbers are fixed, because the table above is an estimate made
with no source available.

This is one column, one expression and one `WHERE` clause. It is not an
abstraction.

The same column becomes self tuning later. No new structure is needed:

```
poll_interval = clamp(median observed change gap / 2, 15 min, 6 h)
```

A cache cannot reduce price requests, because the source publishes no validators.
The source change timestamp gives the same saving.

We cache metadata hard. Clubs, leagues, nations and rarities almost never change.

A game leaves the rotation when we delete the `asset_poll_state` rows of its
assets. The history and the coverage record stay. The quota frees immediately. No
flag column and no schema change is needed.

### 4.8 TimescaleDB operations

These are longevity requirements. They are not optimisations.

At 10,800 polls each day across two markets, the ceiling is 21,600 rows each day.
That gives about 8 million observation rows each year. The real figure is lower,
because the table records changes only.

**Version.** Pin the exact image tag in `docker-compose.yml`. The minimum is
TimescaleDB 2.18 on PostgreSQL 17. The unique constraint and `ON CONFLICT`
behaviour in section 4.2 depends on 2.11 or later. Declare the unique constraint in
the first migration, before any compression policy exists.

**Compression.** Compress chunks older than 30 days:

```sql
ALTER TABLE market_observations SET (
  timescaledb.compress_segmentby = 'asset_id, market_id',
  timescaledb.compress_orderby   = 'observed_at DESC'
);
```

`segmentby` is the setting that decides whether a per-asset query reads one segment
or decompresses the whole chunk. Without it, rows for all assets interleave inside
the same compressed batches and no batch can be excluded.

**Retention.** Never add a retention policy to `market_observations`. The ledger is
the asset.

**Rollups.** None. We add no continuous aggregate in this milestone.

No query in this document reads one. The market table query cannot be one, for the
three reasons in section 5.4. The chart endpoint reads one asset over a bounded
range, which the read index already serves. At about 8 million rows each year, a
rollup would optimise a problem that we do not have.

Adding one later also carries a cost that we avoid by waiting. A refresh policy
only materialises a bounded recent window, so any backfill or replay outside that
window leaves the rollup silently wrong until somebody calls
`refresh_continuous_aggregate` by hand. We take that on when a query needs it.

A rollup could not fill gaps in any case. TimescaleDB does not permit
`time_bucket_gapfill` or `locf` inside a continuous aggregate definition. The chart
applies both at read time.

**Backup.** A nightly `pg_dump -Fc` writes to the same bucket as the archive, and
we keep the last 30. The archive alone cannot rebuild the ledger, because identity
lives in the database. A restore calls `timescaledb_pre_restore()` before the load
and `timescaledb_post_restore()` after it.

### 4.9 Data quality and drift

Ingestion degrades quietly over long runs. A source that returns nulls, frozen
values or a changed schema looks the same as a healthy source.

Each run computes checks. A failed check sets `status` to `degraded`:

- the share of records with no price
- the count of distinct prices, which finds a frozen source
- the share of assets whose source timestamp did not advance
- a comparison of the price distribution against the last good run

A run that dies leaves `status` at `running` forever. The next start finds runs
with an old `heartbeat_at` and marks them `abandoned`.

**Health.** `GET /health` returns 503 only when the ledger has stopped:

- no `ingest_polls` row of any outcome is newer than the largest poll interval
- free space on the data volume is below a fixed threshold

Read `ingest_polls`, not `market_observations`. The observation table records
changes only, so a quiet market writes nothing and would look like an outage.

For the same reason, never make a missing `written` outcome a failure condition.
`written` means the price moved. A card that holds its price produces `unchanged`
for months, which is correct behaviour, and a check that treats it as a fault sits
red on a healthy system until somebody stops reading it.

Per-asset staleness belongs in the drift checks above, where it sets `degraded`. It
does not belong in a signal that pages a human.

The disk check matters because `compress_chunk` needs room for a compressed copy
before it drops the original, and a full volume stops PostgreSQL.

**Heartbeat.** A pull check on a failing host cannot report that the host is gone.
This is the only signal that points outward.

When a run closes with status `ok`, the run sends one request to the URL in
`HEARTBEAT_URL`, which is an external dead man's switch, with a short timeout. A
failed or slow heartbeat writes a warning. It never changes the run status. When
`HEARTBEAT_URL` is unset the call is skipped, so tests and local development need
no external service.

A run that closes `degraded` or `failed` sends no heartbeat. The drift checks
therefore raise an alarm through the same channel as a dead host.

Set the grace period of the switch above the largest poll interval in section 4.7.

Every service in `docker-compose.yml` sets `restart: unless-stopped`. A host reboot
must not end the ledger.

**Supervision.** The interval task awaits the join handle of each run. A panic
becomes a failed run in the log. It never leaves axum serving 200 while ingestion
is dead.

### 4.10 Raw archive

```
raw/<source>/YYYY/MM/DD/<run-id>-<batch>.json.zst
raw/<source>/metadata/YYYY/MM/DD/<run-id>.json.zst
```

The archive stores an envelope, not a bare body:

```json
{ "fetched_at": ..., "url": ..., "requested_ids": [...],
  "http_status": ..., "sha256": ..., "body": ... }
```

The envelope matters. Rule 1 sends one request for each player exactly when the
response carries no identifier, so the body alone cannot be attributed to an asset
on replay. `requested_ids` supplies the missing link.

The archive wraps every provider response, including metadata fetches. Card
attributes feed every valuation input in section 5. Without them in the archive
they exist in one place only.

The archive write is a precondition. If it fails, we do not parse that batch, and
the run status becomes `degraded`. The object key and the checksum go into
`ingest_runs.metadata`.

**Replay.** Replay is the one deliberate exception to the append-only rule. It is a
manual command. It never runs automatically:

```sql
DELETE FROM market_observations WHERE ingest_run_id = ANY($1);
```

Then re-insert under the new `parser_version`. Without this, `ON CONFLICT DO
NOTHING` makes the first parse permanent, and a better parser could never correct a
wrong row.

Replay re-parses into the existing database and resolves identity through
`asset_source_ids`. The database is the record of identity, which is why section
4.8 requires a backup.

### 4.11 Configuration

All configuration comes from environment variables. `main.rs` reads them once into
a plain `Config` struct and passes it down. There is no configuration crate and no
configuration file format. A checked-in `.env.example` documents every variable, and
`docker-compose.yml` supplies the development values.

| Variable | Purpose |
|---|---|
| `DATABASE_URL` | PostgreSQL connection |
| `OBJECT_STORE_ENDPOINT` | S3 compatible endpoint |
| `OBJECT_STORE_BUCKET` | Archive and backup bucket |
| `OBJECT_STORE_ACCESS_KEY` | Object store credential |
| `OBJECT_STORE_SECRET_KEY` | Object store credential |
| `SOURCE_API_KEY` | Provider credential, when a provider needs one |
| `DAILY_REQUEST_BUDGET` | Quota stop in section 4.5 |
| `HTTP_TIMEOUT_SECONDS` | Request timeout in section 4.5 |
| `MIN_FREE_DISK_BYTES` | Disk threshold in section 4.9 |
| `HEARTBEAT_URL` | Dead man's switch in section 4.9. Optional |
| `API_ROW_CAP` | Server side row cap in section 6 |
| `NEXT_PUBLIC_API_URL` | The Rust API base URL, read by the web application |

Secrets never enter the repository, the archive, a log line or an `ingest_runs`
row. The `.env` file stays in `.gitignore`.

## 5. Valuation engine

Valuation uses hedonic pricing. An asset's fair value is a function of its
observable attributes. Mispricing is the residual. Property valuation uses the same
arithmetic. It needs no training data, no labels and no model.

### 5.1 Class median

```sql
-- $1 = market_id
WITH live AS (
  SELECT asset_id
  FROM asset_poll_state
  WHERE market_id = $1
    AND consecutive_failures = 0
    AND last_polled_at > now() - interval '3 days'
),
latest AS (
  SELECT v.asset_id, o.price, o.observed_at, o.min_price, o.max_price
  FROM live v
  CROSS JOIN LATERAL (
    SELECT price, observed_at, min_price, max_price
    FROM market_observations
    WHERE market_id = $1 AND asset_id = v.asset_id
    ORDER BY observed_at DESC, source
    LIMIT 1
  ) o
),
buckets AS (
  SELECT a.rarity, a.version, a.rating,
         percentile_cont(0.5) WITHIN GROUP (ORDER BY l.price) AS median_price,
         count(*) AS n
  FROM latest l
  JOIN assets a ON a.id = l.asset_id
  WHERE l.min_price IS NULL OR l.price > l.min_price
  GROUP BY a.rarity, a.version, a.rating
)
SELECT a.name, l.price, b.median_price, b.n,
       CASE WHEN b.n >= 5 THEN l.price / b.median_price END AS value_ratio,
       (l.min_price IS NOT NULL AND l.price <= l.min_price) AS at_floor
FROM latest l
JOIN assets  a ON a.id = l.asset_id
LEFT JOIN buckets b
       ON b.rarity = a.rarity AND b.version = a.version AND b.rating = a.rating
ORDER BY at_floor, value_ratio NULLS LAST;
```

A `value_ratio` below 1 means the card is cheap for its class.

Six decisions in that query each answer a specific failure.

**The class key includes `version`.** Without it a promotional card and a base card
of the same rating share a bucket, and the median describes neither. The ratio then
measures which promotion a card belongs to, not whether it is cheap. This is the
single most important line in the query.

**There is no time window on the observations.** The table records changes only, so
a card that has held one price for months has no recent row. A window would drop
the illiquid majority, which is 400 of the 600 covered assets, and every remaining
median would be biased upward. The `LATERAL` lookup takes each asset's newest row
whenever it was written. It costs one index seek for each covered asset.

**Freshness comes from `asset_poll_state`, not from the observation age.** An old
`observed_at` is ambiguous: the price held, or we have failed to read that card for
weeks. Section 4.3 exists to resolve exactly this. Without the `live` gate, a dead
feed keeps voting, and because prices deflate across a cycle its stale value is
biased high and manufactures false buy signals.

**The join to `buckets` is a `LEFT JOIN` and there is no `HAVING`.** An inner join
with a count threshold deletes the asset rather than its ratio, and thin buckets are
exactly the Icons and top promotional cards that carry the volume. The asset now
always appears. Only the ratio goes null, and `n` is returned so a reader can see
why.

**Bound-pinned cards are excluded from the fit and sorted last.** EA sets a minimum
listing price. A card resting on it holds the lowest ratio in its class by
construction, so an ascending sort would fill the head of the product's main table
with cards that are not cheap, only floored.

**The `source` tiebreak is required.** Without it PostgreSQL returns an arbitrary
row once two sources exist, and the same query gives different answers.

Two honest limits.

The threshold of five is a guess made with no data. Check the real distribution of
assets for each `(rarity, version, rating)` cell before fixing it, and consider a
rating band if too many cells stay thin. Do not choose a band width now, because
price is strongly convex in rating and a width chosen without data would mix
populations again.

The output ranks a level, not a move. A card that has sat at 0.7 of its class for
three months and a card that fell to 0.7 last night produce the same number, and
only the second is a signal. The fix is a second anchor at a lookback date. We do
not add it now, because with no history a lookback returns nothing. It is the first
enhancement once the ledger has depth.

### 5.2 Later stages. Not in this milestone.

**Hedonic regression.** Ordinary least squares on `log(price)`, with rating,
rarity, version, position, league, the six face statistics and the playstyle count
as inputs. A QR decomposition solves it in about thirty lines. This replaces the
class median when cells stay thin, because a regression pools across cells and is
well powered exactly where a per-cell median is not. It also separates a genuinely
underpriced card from a card that is correctly cheap because it is unusable.

**Fodder floor.** Coins for each rating point that a card contributes, computed
from EA's published squad rating rules. This gives an objective floor that no
opinion moves.

Neither is in the build order. Both need real data to be worth writing, and the
regression adds a dependency that the foundation does not need.

### 5.3 Valuations are queries, not stored facts

A valuation is a function of the ledger and the asset attributes at the time we
compute it. We run it on demand. There is no results table to maintain.

Two honest limits apply.

A valuation is **not** reproducible for an arbitrary past date. The `assets`
feature columns are mutable, and EA changes ratings and statistics in season
through upgrades and Evolutions. If exact historical reproduction becomes a
requirement, `assets` needs validity dating. We do not add that now.

A valuation **cannot** be a continuous aggregate. TimescaleDB rejects `DISTINCT ON`
and rejects ordered-set aggregates such as `percentile_cont` in an aggregate
definition, and it requires a `time_bucket` group on the partitioning column. The
query above fails all three tests. This is why section 4.8 builds no rollups.

## 6. API

```
GET /health
GET /assets
GET /assets/:id
GET /assets/:id/prices?from=&to=
```

`from` and `to` default to the last 30 days. A hard server side row cap applies.
Without a bound, one request for an old tier A asset serialises hundreds of
thousands of rows and can exhaust memory on the host that also runs ingestion.

`GET /assets` stays unpaginated until coverage passes a few thousand assets.

Three handlers and three hand written TypeScript types. We generate no OpenAPI
document and no client. That trade reverses at about fifteen endpoints.

## 7. Frontend

Next.js App Router. Server Components call the Rust API directly. shadcn supplies
the primitives. TradingView Lightweight Charts draws the price chart.

Two routes only. `/` shows the market table. `/assets/[id]` shows one asset with
its latest price and its history. There is no state library.

The `serve` command runs axum and a Tokio interval task. That task calls the same
`ingest::run()` function that the `ingest` command calls. One function has two
entry points, and the advisory lock in section 4.5 keeps them apart. There is no
cron job, no queue and no worker.

## 8. Build order

No step below depends on a data source.

1. Create the Rust workspace and the Next.js application. Verify `cargo check` and
   the development server.
2. Create the Docker Compose file with a pinned PostgreSQL and TimescaleDB image.
   Set `restart: unless-stopped` on every service. Verify that the extension loads.
3. Write the `Config` struct and `.env.example` from section 4.11.
4. Write the first migration: the tables, the `NOT NULL` columns, the `CHECK`
   domains, the unique constraints and the read index from section 4.2.
5. Convert the two fact tables to hypertables. Set the compression settings. Verify
   inserts and range queries.
6. Write the canonical Rust types and the identifier newtypes.
7. Write the SQLx connection and the smallest set of query functions.
8. Write the ingest skeleton, the run records, the advisory lock, and the archive
   write and read.
9. Write the source parser against fixtures.
10. Add asset discovery, the tier expression from section 4.7 and the metadata step
    from section 4.5. Without this step no asset is ever polled and no valuation
    input is ever populated.
11. Add the validation rules from section 4.6.
12. Add the health checks and the heartbeat from section 4.9, and the nightly dump
    from section 4.8.
13. Add the class median query from section 5.1.
14. Add the API handlers.
15. Build the frontend market page, asset page and chart.

We do not build the event model, indices, signals, delivery, predictions or
accounts.

## 9. Tests

| Test | Assertion |
|---|---|
| Identity | One external identifier always maps to one internal asset |
| Identity | Two sources map to one internal asset |
| Identity | The same external identifier in two games maps to two assets |
| Identity | A payload whose name disagrees with the resolved asset is rejected |
| Identity | A payload whose rating disagrees is accepted, because ratings change in season |
| Tiers | The tier expression puts every covered asset in exactly one tier |
| Tiers | Discovery creates one poll state row for each asset and market pair |
| Idempotency | One payload ingested twice does not double the observation count |
| Idempotency | The unique constraint still rejects a duplicate after chunk compression |
| Restatement | A different price at the same timestamp lands as a second row |
| Historical | An old `observed_at` and a current `ingested_at` stay separate |
| Multi market | One asset holds separate observations and poll state for each market |
| Time validation | A far future or pre release `observed_at` is rejected |
| Replay determinism | Replaying one archive twice gives the same table |
| Missing time | A missing source timestamp is rejected and counted |
| Coverage | An unchanged price writes a poll row with outcome `unchanged` |
| Coverage | A rejected record writes a poll row with outcome `rejected` |
| Rejection | One bad record does not fail the run |
| Concurrency | A second run stops when the advisory lock is held |
| Atomicity | A failure between the writes leaves no poll row without its observation |
| Archive | A failed archive write stops the batch and marks the run degraded |
| Valuation | The class median ratio is stable and reproducible on fixed input |
| Valuation | Two sources at one timestamp give a deterministic result |
| Valuation | A promotional card and a base card of one rating fall in separate classes |
| Valuation | An asset in a class below the threshold appears with a null ratio, never 1.0 |
| Valuation | An asset whose last price is months old still appears, because the ledger records changes only |
| Valuation | An asset with failing polls is excluded, and its stale price leaves the median |
| Valuation | A card at its minimum price does not lead the cheap list |
| API | Observations return in chronological order and respect the row cap |
| Health | A stopped ledger makes `/health` return 503 |
| Health | A market where no price moves keeps `/health` at 200 |
| Health | A degraded run sends no heartbeat |
| Raw replay | A saved fixture parses again with no network |

The compiler enforces provider isolation, because the provider structs stay private
to their module. A CI grep is the backstop.

Parser tests read fixtures from disk. Database tests use real PostgreSQL and
TimescaleDB in Docker.

## 10. Risks

1. **No data source.** The ledger cannot start. No step in section 8 depends on a
   source, so the delay costs little.
2. **Source terms.** A supplier may restrict storage of its data or publication of
   derived charts. Get a written answer before you build a parser for that
   supplier.
3. **Unverified payload shape.** A published schema can be wrong. Rule 1 protects
   the ledger when the response shape is unclear.
4. **No accuracy warranty.** Suppliers disclaim accuracy. Section 4.9 gives our own
   checks.
5. **One supplier.** `asset_source_ids` limits this risk. It does not remove it.
6. **Valuations are not reproducible for a past date.** Section 5.4 states the
   limit. Validity dating on `assets` is the fix when something needs it.
