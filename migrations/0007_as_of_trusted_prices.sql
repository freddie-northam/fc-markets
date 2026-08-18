-- Every derivation must answer AS OF a stated time, never as of now.
--
-- Without this the class median cannot be replayed, so a claim it made last
-- Tuesday cannot be scored: the answer moves every time it is asked, and marking
-- our own predictions becomes marking our own homework against a moved goalpost.
--
-- The view this replaces could not simply take a parameter. It read freshness
-- from asset_poll_state, whose last_polled_at and consecutive_failures are
-- OVERWRITTEN on every poll. Passing it a past date would have applied today's
-- poll state to that date and returned a confident, false reconstruction, which
-- is worse than refusing to answer.
--
-- Freshness therefore comes from ingest_polls, which is append only and holds
-- one row per poll for ever. That table exists precisely so the ledger can
-- separate "the price held" from "we stopped looking", and this is the second
-- use of it.
DROP VIEW trusted_latest_prices;

CREATE FUNCTION trusted_latest_prices(as_of TIMESTAMPTZ)
RETURNS TABLE (
    asset_id    UUID,
    market_id   UUID,
    price       BIGINT,
    observed_at TIMESTAMPTZ,
    min_price   BIGINT,
    at_floor    BOOLEAN
)
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    WITH live AS (
        -- The newest poll at or before the stated time, per asset and market.
        -- DISTINCT ON is what makes this the state THEN rather than the state
        -- now, and the three day window is the same one the view used.
        SELECT DISTINCT ON (p.asset_id, p.market_id)
               p.asset_id, p.market_id, p.outcome
        FROM ingest_polls p
        WHERE p.polled_at <= as_of
          AND p.polled_at > as_of - INTERVAL '3 days'
        ORDER BY p.asset_id, p.market_id, p.polled_at DESC
    )
    SELECT l.asset_id,
           l.market_id,
           o.price,
           o.observed_at,
           o.min_price,
           -- A card resting on EA's minimum is not cheap, it is floored.
           -- Flagged rather than filtered, because the class median drops it
           -- from the median while still ranking it.
           (o.min_price IS NOT NULL AND o.price <= o.min_price) AS at_floor
    FROM live l
    CROSS JOIN LATERAL (
        -- No time window on the observations themselves. The table records
        -- changes only, so a card that has held one price for months has no
        -- recent row, and a window would drop the illiquid majority and bias
        -- every median upward.
        --
        -- Bounded by ingested_at, NOT by observed_at. The question is what we
        -- KNEW at that moment, not what the market had shown by then. A price
        -- observed last week but imported yesterday was not available for a
        -- decision made the day before, and counting it would be hindsight.
        SELECT price, observed_at, min_price
        FROM market_observations
        WHERE market_id = l.market_id
          AND asset_id = l.asset_id
          AND ingested_at <= as_of
        -- source breaks the tie. Without it two providers at one instant make
        -- the same query return different answers on different days.
        ORDER BY observed_at DESC, source
        LIMIT 1
    ) o
    -- A read that failed is not evidence of a price. 'rejected' and 'no_price'
    -- are the failures; this is the reproducible form of consecutive_failures.
    WHERE l.outcome IN ('written', 'unchanged')
$$;

COMMENT ON FUNCTION trusted_latest_prices IS
    'The newest price per asset and market that we were willing to reason from '
    'AS OF the given time. Reads only append-only tables, so the same argument '
    'always returns the same rows.';
