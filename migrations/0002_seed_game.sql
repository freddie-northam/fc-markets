-- Rule 2 compares released_at against a source timestamp, so it must carry a
-- time zone. A DATE compares at local midnight, which moves the lower validation
-- bound by the server's offset and would reject or accept a release-day
-- observation depending on where the host stands.
ALTER TABLE games
    ALTER COLUMN released_at TYPE TIMESTAMPTZ
    USING released_at::timestamptz;

-- The game and its markets are reference data, not user data. Seeding them here
-- keeps the identifiers fixed, so a test, a development database and production
-- all resolve the same market for the same platform.
--
-- The identifiers are literals rather than generated values on purpose. Ingestion
-- resolves a market by (game, platform), and a generated identifier would differ
-- between environments for no gain.

INSERT INTO games (id, code, name, released_at) VALUES
    ('0000fc26-0000-4000-8000-000000000001', 'FC26', 'EA SPORTS FC 26',
     '2025-09-26T00:00:00Z')
ON CONFLICT (code) DO NOTHING;

-- Two platforms, because those are the two a source separates today. Section 4.2
-- adds a value here when a source separates more.
INSERT INTO markets (id, game_id, platform) VALUES
    ('0000fc26-0000-4000-8000-000000000010',
     '0000fc26-0000-4000-8000-000000000001', 'playstation'),
    ('0000fc26-0000-4000-8000-000000000011',
     '0000fc26-0000-4000-8000-000000000001', 'pc')
ON CONFLICT (game_id, platform) DO NOTHING;
