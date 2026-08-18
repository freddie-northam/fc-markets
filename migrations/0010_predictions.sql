-- What the system said, when it said it, and never revised.
--
-- Collection alone proves nothing. In six weeks we will have a price history and
-- still no evidence the signal works, unless the claims were written down BEFORE
-- their outcomes were knowable. Writing them down afterwards is choosing the
-- winners and calling it a result.
--
-- Three properties make a score believable, and each is a column here.
--
-- The claim is immutable. Rows are added, never edited, so a pick cannot be
-- quietly dropped once it goes wrong.
--
-- The horizon is stated in advance. Without it the horizon becomes a free
-- parameter chosen after the fact, which is the ordinary way an honest looking
-- backtest lies.
--
-- The evidence is recorded beside the claim. `made_at` is the as_of the
-- derivation used, so the ranking that produced this pick recomputes exactly,
-- and the price and ratio are kept so the claim reads without recomputing.
--
-- The OUTCOME is deliberately not a column. It is derived from the ledger when
-- asked, so it cannot be written down wrong and cannot be edited later.
CREATE TABLE predictions (
    id             UUID PRIMARY KEY,
    game_id        UUID NOT NULL REFERENCES games (id),
    market_id      UUID NOT NULL REFERENCES markets (id),
    asset_id       UUID NOT NULL REFERENCES assets (id),
    claim          TEXT NOT NULL
                   CHECK (claim = lower(claim) AND claim <> '' AND claim !~ '\s'),
    -- The instant the derivation was asked about, not our clock. This is what
    -- makes the ranking behind the pick reproducible.
    made_at        TIMESTAMPTZ NOT NULL,
    recorded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- When this becomes scoreable, stated now so it cannot be chosen later.
    horizon        INTERVAL NOT NULL CHECK (horizon > INTERVAL '0'),
    -- The evidence as it stood, kept so the claim is legible years later.
    price          BIGINT NOT NULL CHECK (price > 0),
    class_median   DOUBLE PRECISION,
    value_ratio    DOUBLE PRECISION,
    cohort_band    TEXT NOT NULL,
    cohort_version TEXT NOT NULL
);

-- One claim per asset per run. A second identical pick would double count in
-- any score computed over these rows.
CREATE UNIQUE INDEX predictions_once_idx
    ON predictions (market_id, asset_id, claim, made_at);

CREATE INDEX predictions_due_idx ON predictions (game_id, made_at);

COMMENT ON TABLE predictions IS
    'What the system claimed and when. Append only, horizon stated in advance, '
    'outcome derived rather than stored.';
