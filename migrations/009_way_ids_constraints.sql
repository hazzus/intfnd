ALTER TABLE climbs DROP CONSTRAINT climbs_osm_way_ids_key;

ALTER TABLE climbs ADD CONSTRAINT climbs_lat_lng_osm_way_ids_key UNIQUE (start_lat, start_lng, osm_way_ids);