-- When the game did something, so a price move can be attributed to it.
--
-- Journey C of the product notes is blocked entirely on this table, and it is
-- the one thing here that cannot be backfilled. Prices we can accumulate from
-- today. Release dates we can only record as they happen, because a promotion
-- that ran last Thursday leaves no trace in a price series that says WHY it
-- moved. Without this, "85+ golds rise before a drop" is untestable for ever.
--
-- The ledger's own rule applies unchanged: rows are added, never edited. An
-- event whose time we learn more precisely later is a NEW row that supersedes an
-- earlier one, so a study run last week can be reproduced exactly.
CREATE TABLE market_events (
    id           UUID PRIMARY KEY,
    game_id      UUID NOT NULL REFERENCES games (id),
    -- What kind of thing happened. Deliberately not a closed list: EA invents
    -- promotions faster than we can migrate, and the same shape reasoning as
    -- `version` applies. Constrain the shape, not the values.
    kind         TEXT NOT NULL
                 CHECK (kind = lower(kind) AND kind <> '' AND kind !~ '\s'),
    -- The promotion or release this belongs to, as EA named it.
    name         TEXT NOT NULL CHECK (name <> ''),
    -- When it happened, from the game, never our clock. The same split the
    -- observation table makes and for the same reason: an event recorded three
    -- days late is still an event that happened on the day it happened.
    occurred_at  TIMESTAMPTZ NOT NULL,
    -- Null while it is still running, or when the kind is instantaneous.
    ended_at     TIMESTAMPTZ,
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- The row this replaces, when we learn a time more precisely. Null on a
    -- first record. Following the chain is what makes a past study reproducible.
    supersedes   UUID REFERENCES market_events (id),
    note         TEXT,
    CHECK (ended_at IS NULL OR ended_at >= occurred_at)
);

-- Studies ask "what happened in the days around this", so the range scan by game
-- and time is the only access pattern worth an index today.
CREATE INDEX market_events_game_time_idx
    ON market_events (game_id, occurred_at DESC);

-- One live row per event: a superseded row is history, not a duplicate.
CREATE UNIQUE INDEX market_events_current_idx
    ON market_events (game_id, kind, name, occurred_at)
    WHERE supersedes IS NULL;

COMMENT ON TABLE market_events IS
    'When the game did something. Append only: a correction is a new row that '
    'supersedes an older one, so a study is reproducible.';
