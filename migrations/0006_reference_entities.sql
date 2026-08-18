-- The names behind the identifiers.
--
-- assets stores league_id, nation_id and club_id as bare integers from the
-- provider. Nothing resolves them, so a card knows it belongs to league 13 and
-- cannot say Premier League. That blocks two things: labelling anything a human
-- reads, and cohorts cut by league or nation rather than by rating.
--
-- Stored rather than fetched on demand for the reason the archive exists: these
-- lists are the provider's, and if the provider goes away the ledger must still
-- be able to say what a row meant.
--
-- One table rather than three. Leagues, nations and clubs differ only in what
-- they belong to, so three tables would mean three fetch paths, three parsers
-- and three upserts to keep in step. `kind` carries the difference, and a new
-- reference list needs no migration.
CREATE TABLE reference_entities (
    source       TEXT NOT NULL,
    game_id      UUID NOT NULL REFERENCES games (id),
    kind         TEXT NOT NULL CHECK (kind IN ('league', 'nation', 'club')),
    external_id  INTEGER NOT NULL,
    name         TEXT NOT NULL CHECK (name <> ''),
    -- What it belongs to: a club's league, a league's nation. Null for a nation,
    -- which belongs to nothing, and null when the provider omits it.
    parent_id    INTEGER,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Keyed by provider AND game. The provider renumbers between titles, exactly
    -- as it does for assets, so one key per game is what stops next season's
    -- league 13 overwriting this season's.
    PRIMARY KEY (source, game_id, kind, external_id)
);

COMMENT ON TABLE reference_entities IS
    'Provider reference lists: leagues, nations and clubs, by name. Small, '
    'static, and refreshed on a slow cadence.';
