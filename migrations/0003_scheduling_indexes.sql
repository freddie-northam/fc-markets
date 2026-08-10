-- Two aggregates run on every ingest start and had no index behind them. Both
-- tables grow for ever, so each was a sequential scan whose cost rises without
-- bound: at 96 runs a day the cost compounds long before anyone notices.

-- frozen_feed() takes max(source_observed_at) across the whole poll table. That
-- table gains ~1,200 rows per run, roughly 42 million a year, and is never
-- pruned, because it is the coverage record.
CREATE INDEX ingest_polls_source_observed_idx
    ON ingest_polls (source_observed_at DESC)
    WHERE source_observed_at IS NOT NULL;

-- requests_spent_today() and seconds_since_step() both filter ingest_runs by
-- source and start time, the latter twice per run through the cadence gates.
CREATE INDEX ingest_runs_source_started_idx
    ON ingest_runs (source, started_at DESC);
