TRUNCATE TABLE segments;

ALTER TABLE segments RENAME TO climbs;

ALTER TABLE climbs DROP COLUMN surface;

ALTER TABLE climbs
    ADD COLUMN surfaces TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN elevation_profile REAL[] NOT NULL DEFAULT '{}',
    ADD COLUMN osm_way_ids BIGINT[] NOT NULL DEFAULT '{}',
    ADD COLUMN bidirectional BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN score DOUBLE PRECISION;

ALTER TABLE climbs ALTER COLUMN surfaces DROP DEFAULT;
ALTER TABLE climbs ALTER COLUMN elevation_profile DROP DEFAULT;
ALTER TABLE climbs ALTER COLUMN osm_way_ids DROP DEFAULT;
ALTER TABLE climbs ALTER COLUMN bidirectional DROP DEFAULT;

ALTER TABLE climbs ADD CONSTRAINT climbs_osm_way_ids_key UNIQUE (osm_way_ids);
