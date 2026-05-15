CREATE TABLE ways (
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

CREATE INDEX ON ways ((nodes[1]));
CREATE INDEX ON ways ((nodes[array_length(nodes,1)]));

CREATE TABLE nodes (
    id BIGINT PRIMARY KEY,
    lat DOUBLE PRECISION NOT NULL,
    lng DOUBLE PRECISION NOT NULL,
    is_signal BOOLEAN NOT NULL DEFAULT FALSE,
    tags JSONB NOT NULL DEFAULT '{}',
    elevation REAL,
    degree INT
);
