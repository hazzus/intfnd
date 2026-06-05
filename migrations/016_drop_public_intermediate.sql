-- Intermediate importer tables are now created per-region in their own schema by the
-- OSM importer (osm/schema.sql), not in public. The app only reads climbs, so drop the
-- now-unused public copies created by migrations 011/013-015.
DROP TABLE IF EXISTS proto_climbs CASCADE;
DROP TABLE IF EXISTS ways CASCADE;
DROP TABLE IF EXISTS nodes CASCADE;
