-- The trust window becomes an argument.
--
-- It was INTERVAL '3 days', hardcoded, while the slowest polling band was also
-- 72 hours. The two are different answers to "how stale is too stale" and they
-- had met exactly: an asset became DUE for polling at the same moment it stopped
-- being trusted. 19,798 assets sit in that band, and any delay dropped them from
-- the class median and every cohort while they waited for a poll that was only
-- just overdue. Overdue is not unknown.
--
-- Passing it in leaves one definition, in domain.rs, next to the bands it has to
-- exceed, where a test asserts the relationship rather than a comment asking for
-- it.
DROP FUNCTION trusted_latest_prices(TIMESTAMPTZ);

CREATE FUNCTION trusted_latest_prices(as_of TIMESTAMPTZ, max_age INTERVAL)
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
        SELECT DISTINCT ON (p.asset_id, p.market_id)
               p.asset_id, p.market_id, p.outcome
        FROM ingest_polls p
        WHERE p.polled_at <= as_of
          AND p.polled_at > as_of - max_age
        ORDER BY p.asset_id, p.market_id, p.polled_at DESC
    )
    SELECT l.asset_id,
           l.market_id,
           o.price,
           o.observed_at,
           o.min_price,
           (o.min_price IS NOT NULL AND o.price <= o.min_price) AS at_floor
    FROM live l
    CROSS JOIN LATERAL (
        -- Bounded by ingested_at, NOT observed_at. The question is what we KNEW
        -- at that moment. A price observed last week but imported yesterday was
        -- not available for a decision made the day before.
        SELECT price, observed_at, min_price
        FROM market_observations
        WHERE market_id = l.market_id
          AND asset_id = l.asset_id
          AND ingested_at <= as_of
        ORDER BY observed_at DESC, source
        LIMIT 1
    ) o
    WHERE l.outcome IN ('written', 'unchanged')
$$;

COMMENT ON FUNCTION trusted_latest_prices IS
    'The newest price per asset and market we were willing to reason from AS OF '
    'the given time, where max_age must exceed the slowest polling band.';
