-- Foundation schema. See docs/superpowers/specs for the reasoning behind each
-- constraint. The comments here record why a line exists, not what it does.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------------
-- Identity
-- ---------------------------------------------------------------------------

CREATE TABLE games (
    id          UUID PRIMARY KEY,
    code        TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    -- NOT NULL because rule 2 uses this as the lower validation bound. A null
    -- would make the range test yield null and reject every record.
    released_at DATE NOT NULL
);

CREATE TABLE players (
    id          UUID PRIMARY KEY,
    name        TEXT NOT NULL,
    ea_base_id  BIGINT UNIQUE
);

CREATE TABLE assets (
    id               UUID PRIMARY KEY,
    game_id          UUID NOT NULL REFERENCES games (id),
    player_id        UUID NOT NULL REFERENCES players (id),
    name             TEXT NOT NULL,

    -- Valuation inputs. These are columns rather than JSON because section 5
    -- groups and filters on them on every read.
    rating           SMALLINT,
    rarity           TEXT NOT NULL DEFAULT 'rare'
                     CHECK (rarity IN ('common', 'rare')),
    -- version carries the promotional variant and drives price far more than
    -- rarity does. Section 5.1 groups on it.
    version          TEXT NOT NULL DEFAULT 'base',
    position         TEXT,
    league_id        INTEGER,
    nation_id        INTEGER,
    club_id          INTEGER,
    skill_moves      SMALLINT,
    weak_foot        SMALLINT,
    pace             SMALLINT,
    shooting         SMALLINT,
    passing          SMALLINT,
    dribbling        SMALLINT,
    defending        SMALLINT,
    physicality      SMALLINT,
    playstyle_count  SMALLINT,

    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at     TIMESTAMPTZ
);

CREATE INDEX assets_game_idx ON assets (game_id);
CREATE INDEX assets_class_idx ON assets (rarity, version, rating);

-- Provider identifiers never become our primary key. The game belongs in the
-- key because provider identifiers are not always unique across titles: without
-- it, next season's card resolves onto this season's asset and contaminates its
-- history permanently.
CREATE TABLE asset_source_ids (
    asset_id    UUID NOT NULL REFERENCES assets (id) ON DELETE CASCADE,
    game_id     UUID NOT NULL REFERENCES games (id),
    source      TEXT NOT NULL,
    external_id TEXT NOT NULL,
    PRIMARY KEY (game_id, source, external_id)
);

CREATE INDEX asset_source_ids_asset_idx ON asset_source_ids (asset_id);

CREATE TABLE markets (
    id       UUID PRIMARY KEY,
    game_id  UUID NOT NULL REFERENCES games (id),
    platform TEXT NOT NULL CHECK (platform IN ('playstation', 'pc')),
    UNIQUE (game_id, platform)
);

-- ---------------------------------------------------------------------------
-- Scheduling
-- ---------------------------------------------------------------------------

-- Keyed on both columns because a card trades independently on each platform
-- and a source may price them in separate calls.
CREATE TABLE asset_poll_state (
    asset_id             UUID NOT NULL REFERENCES assets (id) ON DELETE CASCADE,
    market_id            UUID NOT NULL REFERENCES markets (id),
    poll_interval_seconds INTEGER NOT NULL,
    last_polled_at       TIMESTAMPTZ,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (asset_id, market_id)
);

-- Serves the "which assets are due" scan.
CREATE INDEX asset_poll_state_due_idx
    ON asset_poll_state (market_id, last_polled_at NULLS FIRST);

-- ---------------------------------------------------------------------------
-- Operational records
-- ---------------------------------------------------------------------------

CREATE TABLE ingest_runs (
    id               UUID PRIMARY KEY,
    source           TEXT NOT NULL,
    -- Replay cannot be reasoned about without knowing which parser wrote a row.
    parser_version   TEXT NOT NULL,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    heartbeat_at     TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ,
    status           TEXT NOT NULL DEFAULT 'running'
                     CHECK (status IN ('running', 'ok', 'degraded', 'failed', 'abandoned')),
    records_seen     INTEGER NOT NULL DEFAULT 0,
    records_written  INTEGER NOT NULL DEFAULT 0,
    records_rejected INTEGER NOT NULL DEFAULT 0,
    error            TEXT,
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX ingest_runs_open_idx ON ingest_runs (status, heartbeat_at)
    WHERE status = 'running';

-- ---------------------------------------------------------------------------
-- Facts
-- ---------------------------------------------------------------------------

CREATE TABLE market_observations (
    asset_id      UUID NOT NULL REFERENCES assets (id),
    market_id     UUID NOT NULL REFERENCES markets (id),
    source        TEXT NOT NULL,
    -- The time the market showed this price, taken from the source. Never our
    -- clock. This is the hypertable dimension.
    observed_at   TIMESTAMPTZ NOT NULL,
    price         BIGINT NOT NULL CHECK (price > 0),
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    min_price     BIGINT CHECK (min_price IS NULL OR min_price > 0),
    max_price     BIGINT CHECK (max_price IS NULL OR max_price > 0),
    source_ref    TEXT,
    ingest_run_id UUID REFERENCES ingest_runs (id)
);

SELECT create_hypertable('market_observations', by_range('observed_at'));

-- price is in the key on purpose. Without it an upstream correction at an
-- already recorded timestamp is discarded in silence by ON CONFLICT DO NOTHING.
-- With it, an identical replay still conflicts so replay stays idempotent, and a
-- genuine restatement lands as a second truthful row.
CREATE UNIQUE INDEX market_observations_idem_idx
    ON market_observations (asset_id, market_id, source, observed_at, price);

-- The read path: latest price for one asset in one market.
CREATE INDEX market_observations_read_idx
    ON market_observations (market_id, asset_id, observed_at DESC);

-- Lets replay delete exactly the rows one run wrote.
CREATE INDEX market_observations_run_idx ON market_observations (ingest_run_id);

-- segmentby decides whether a per-asset read touches one segment or decompresses
-- the whole chunk. Without it every asset interleaves into the same batches.
--
-- Every column of the unique index above must appear in segmentby or orderby.
-- TimescaleDB warns when one does not, because it cannot enforce uniqueness
-- against a compressed chunk on a column it did not segment or order by. That
-- would silently retire our only idempotency guarantee 30 days after each write.
ALTER TABLE market_observations SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'asset_id, market_id, source',
    timescaledb.compress_orderby   = 'observed_at DESC, price'
);

SELECT add_compression_policy('market_observations', INTERVAL '30 days');

-- market_observations records changes only, so it cannot distinguish a stable
-- price from an outage. This table records that we looked and what happened.
CREATE TABLE ingest_polls (
    asset_id           UUID NOT NULL REFERENCES assets (id),
    market_id          UUID NOT NULL REFERENCES markets (id),
    polled_at          TIMESTAMPTZ NOT NULL,
    -- Null exactly when the provider returned no timestamp, which is the case
    -- that proves we looked and got nothing. It therefore cannot be the
    -- hypertable dimension, because that column is forced NOT NULL.
    source_observed_at TIMESTAMPTZ,
    outcome            TEXT NOT NULL
                       CHECK (outcome IN ('written', 'unchanged', 'rejected', 'no_price')),
    run_id             UUID REFERENCES ingest_runs (id)
);

SELECT create_hypertable('ingest_polls', by_range('polled_at'));

CREATE UNIQUE INDEX ingest_polls_idem_idx
    ON ingest_polls (asset_id, market_id, polled_at);

ALTER TABLE ingest_polls SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'asset_id, market_id',
    timescaledb.compress_orderby   = 'polled_at DESC'
);

SELECT add_compression_policy('ingest_polls', INTERVAL '30 days');

-- No retention policy on either fact table. The ledger is the asset.
