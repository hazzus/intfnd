-- Importer-owned intermediate schema, bootstrapped per region by pipeline.py.
-- search_path points at <region>,public when this runs, so unqualified tables land in
-- the region schema while PostGIS / gen_random_uuid resolve via public.
-- Mirrors the final shape of migrations 011 (ways/nodes) and 013-015 (proto_climbs).

CREATE TABLE IF NOT EXISTS ways (
    id BIGINT PRIMARY KEY,
    name TEXT,
    ref TEXT,
    highway TEXT NOT NULL,
    surface TEXT,
    nodes BIGINT[] NOT NULL,
    bidirectional BOOL NOT NULL,
    tags JSONB NOT NULL DEFAULT '{}',
    chain_id BIGINT
);

CREATE INDEX IF NOT EXISTS ways_first_node_idx ON ways ((nodes[1]));
CREATE INDEX IF NOT EXISTS ways_last_node_idx ON ways ((nodes[array_length(nodes,1)]));

CREATE TABLE IF NOT EXISTS nodes (
    id BIGINT PRIMARY KEY,
    lat DOUBLE PRECISION NOT NULL,
    lng DOUBLE PRECISION NOT NULL,
    is_signal BOOLEAN NOT NULL DEFAULT FALSE,
    tags JSONB NOT NULL DEFAULT '{}',
    elevation REAL,
    degree INT
);

CREATE TABLE IF NOT EXISTS proto_climbs (
    id          UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    nodes       BIGINT[] NOT NULL,
    osm_way_ids BIGINT[] NOT NULL,
    start_lat   DOUBLE PRECISION NOT NULL,
    start_lng   DOUBLE PRECISION NOT NULL,
    distance    DOUBLE PRECISION NOT NULL,
    nodes_hash  TEXT NOT NULL DEFAULT '',
    from_climbs UUID[]
);

CREATE INDEX IF NOT EXISTS proto_climbs_start_geo ON proto_climbs
    USING GIST (CAST(ST_MakePoint(start_lng, start_lat) AS geography));

CREATE INDEX IF NOT EXISTS idx_proto_climbs_nodes ON proto_climbs USING GIN (nodes);

CREATE UNIQUE INDEX IF NOT EXISTS proto_climbs_nodes_hash_key ON proto_climbs (nodes_hash);
