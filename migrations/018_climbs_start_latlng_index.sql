-- Explore queries filter climbs by viewport bounding box
-- (start_lat BETWEEN ... AND start_lng BETWEEN ...), which the GIST geography
-- index in 003 cannot serve. A plain btree on the coordinates supports it.
CREATE INDEX climbs_start_latlng_idx ON climbs (start_lat, start_lng);
