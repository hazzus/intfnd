TRUNCATE TABLE climbs;

ALTER TABLE climbs 
  ADD COLUMN end_lat DOUBLE PRECISION NOT NULL,
  ADD COLUMN end_lng DOUBLE PRECISION NOT NULL;

ALTER TABLE climbs DROP CONSTRAINT climbs_lat_lng_osm_way_ids_key;

ALTER TABLE climbs ADD CONSTRAINT climbs_natural_key
  UNIQUE (start_lat, start_lng, end_lat, end_lng, osm_way_ids);