TRUNCATE proto_climbs;

ALTER TABLE proto_climbs DROP CONSTRAINT IF EXISTS proto_climbs_nodes_key;

ALTER TABLE proto_climbs ADD COLUMN nodes_hash TEXT NOT NULL DEFAULT '';

CREATE UNIQUE INDEX proto_climbs_nodes_hash_key ON proto_climbs (nodes_hash);
