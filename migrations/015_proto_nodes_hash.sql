ALTER TABLE proto_climbs DROP CONSTRAINT proto_climbs_nodes_key;

CREATE UNIQUE INDEX proto_climbs_nodes_hash_key ON proto_climbs (md5(nodes::text));
