CREATE TABLE users (
    id                    BIGINT PRIMARY KEY,
    access_token          TEXT NOT NULL,
    refresh_token         TEXT NOT NULL,
    token_expires_at      TIMESTAMPTZ NOT NULL,
    sync_activities_total INTEGER NOT NULL DEFAULT 0,
    sync_activities_done  INTEGER NOT NULL DEFAULT 0,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE segments (
    strava_id      BIGINT PRIMARY KEY,
    name           TEXT NOT NULL,
    distance       DOUBLE PRECISION NOT NULL,
    average_grade  DOUBLE PRECISION NOT NULL,
    start_lat      DOUBLE PRECISION NOT NULL,
    start_lng      DOUBLE PRECISION NOT NULL,
    elevation_gain DOUBLE PRECISION NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

