DROP TABLE IF EXISTS users;
ALTER TABLE segments DROP COLUMN IF EXISTS strava_id;
ALTER TABLE segments DROP COLUMN IF EXISTS star_count;
ALTER TABLE segments
    ADD COLUMN surface TEXT NOT NULL DEFAULT 'asphalt'
        CHECK (surface IN ('asphalt', 'non_asphalt'));
ALTER TABLE segments ALTER COLUMN surface DROP DEFAULT;
