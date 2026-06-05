-- climbs now accumulates across regions; tag each row with its source region so the
-- importer can replace a single region's rows (DELETE WHERE region = ...) instead of
-- truncating the whole table.
ALTER TABLE climbs ADD COLUMN region TEXT;

-- Backfill existing rows (the only region loaded so far is serbia).
UPDATE climbs SET region = 'serbia' WHERE region IS NULL;

ALTER TABLE climbs ALTER COLUMN region SET NOT NULL;

CREATE INDEX climbs_region_idx ON climbs (region);
