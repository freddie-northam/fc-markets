-- Section 4.2 makes `version` a closed domain that we own, for the same reason
-- as `rarity`: free text lets a provider rename fork an asset's history. Rarity
-- got a CHECK in the first migration and version did not.
--
-- Version is the more dangerous of the two. `version <> 'base'` decides the
-- polling tier AND the coverage ranking, and version is a key of the valuation
-- class. A provider that started sending "Base" would promote every base card to
-- the fastest tier, spend the request budget on cards that do not move, and fork
-- every class in the valuation. Nothing would raise an error.
--
-- The promotional list is open ended, so a fixed value list would need a
-- migration per promotion. Constraining the SHAPE gets the safety without that:
-- source modules fold to this form at parse time, so the equality comparisons
-- that decide tiering and classes are sound.
ALTER TABLE assets
    ADD CONSTRAINT assets_version_canonical
    CHECK (version = lower(version) AND version <> '' AND version !~ '\s');
