CREATE TABLE proto_climbs (
    id          UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    nodes       BIGINT[] NOT NULL UNIQUE,
    osm_way_ids BIGINT[] NOT NULL,
    start_lat   DOUBLE PRECISION NOT NULL,
    start_lng   DOUBLE PRECISION NOT NULL,
    from_climbs UUID[]
);

CREATE INDEX proto_climbs_start_geo ON proto_climbs
    USING GIST (CAST(ST_MakePoint(start_lng, start_lat) AS geography));

CREATE INDEX idx_proto_climbs_nodes ON proto_climbs USING GIN (nodes);