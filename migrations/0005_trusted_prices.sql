-- One definition of "a price we are willing to reason from".
--
-- The class median query already encoded this and the cohort index needs the
-- same rules. Copying them would mean two definitions of trust that drift apart
-- in silence, and each rule below exists because of a specific way a price lies.

CREATE VIEW trusted_latest_prices AS
WITH live AS (
    -- Freshness comes from the poll state, not from how old the price is. An old
    -- observed_at is ambiguous: either the price held, or we have failed to read
    -- the card for weeks. Prices deflate, so a stale read is biased high and
    -- would manufacture false buy signals.
    SELECT asset_id, market_id
    FROM asset_poll_state
    WHERE consecutive_failures = 0
      AND last_polled_at > now() - INTERVAL '3 days'
)
SELECT l.asset_id,
       l.market_id,
       o.price,
       o.observed_at,
       o.min_price,
       -- A card resting on EA's minimum listing price is not cheap, it is
       -- floored. Carried rather than filtered, because the class median drops
       -- floored cards from the median it computes but still ranks them, and the
       -- cohort index needs the same distinction rather than a different one.
       (o.min_price IS NOT NULL AND o.price <= o.min_price) AS at_floor
FROM live l
CROSS JOIN LATERAL (
    -- No time window on the observations. The table records changes only, so a
    -- card that has held one price for months has no recent row, and a window
    -- would drop the illiquid majority and bias every median upward.
    SELECT price, observed_at, min_price
    FROM market_observations
    WHERE market_id = l.market_id AND asset_id = l.asset_id
    -- source breaks the tie. Without it two providers at one instant make the
    -- same query return different answers on different days.
    ORDER BY observed_at DESC, source
    LIMIT 1
) o;

COMMENT ON VIEW trusted_latest_prices IS
    'The newest price per asset and market that we are willing to reason from: '
    'polled recently, not failing, with floored prices flagged rather than '
    'hidden. One definition, shared by the class median and the cohort index.';

-- One definition of the rating bands the cohort index groups on.
--
-- In SQL because grouping happens here. The polling tier lives in Rust and keys
-- off a different boundary (75) for a different reason (what the budget can
-- afford daily), so the two are deliberately separate rather than one constant
-- pressed into two jobs.
--
-- The boundaries come from the measured catalogue: promotional cards are all
-- rated 80 or above and base cards 82 or below, so the bands below 83 mix the
-- two and the bands above it do not.
CREATE FUNCTION rating_band(rating SMALLINT) RETURNS TEXT
    LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE
        WHEN rating IS NULL THEN 'unrated'
        WHEN rating >= 90 THEN '90+'
        WHEN rating >= 86 THEN '86-89'
        WHEN rating >= 83 THEN '83-85'
        WHEN rating >= 80 THEN '80-82'
        WHEN rating >= 75 THEN '75-79'
        ELSE 'under-75'
    END
$$;

COMMENT ON FUNCTION rating_band IS
    'The cohort rating bands. Immutable so it can be indexed and grouped on.';
