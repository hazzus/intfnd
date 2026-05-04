# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
cargo build          # compile
cargo run            # build and run
cargo test <name>    # run a single test by name
cargo clippy         # lint
cargo fmt            # format

docker compose up --build   # build image and start app + postgres
docker compose up           # start without rebuilding
docker compose stop         # stop containers, keep volumes (preserve DB)
docker compose logs -f app  # tail app logs
docker compose exec db psql -U postgres intfnd  # connect to DB
```

## Architecture

Axum 0.8 + Tokio web server, PostgreSQL + PostGIS for storage, SQLx 0.8 (runtime queries, no compile-time macros). Single binary, read-only API — climbs are populated out-of-band by `scripts/import_osm_climbs.py`.

**AppState** (`src/main.rs`): holds `PgPool` and `Arc<Config>`.

**Routes:**
- `GET /` — serves `templates/index.html`
- `GET /icon.png` — site icon
- `POST /api/search` — PostGIS radius query + physics filter, returns ranked climbs

**Physics** (`src/physics.rs`): three-force cycling model — gravity (`m·g·grade`), rolling resistance (`m·g·Crr`), and aerodynamic drag (`½·ρ·CdA·v²`). Solves for velocity using Newton's method on the cubic power equation (`P = F_total · v`), then returns `distance / v`. Constants: `Crr=0.004`, `CdA=0.32 m²`, `ρ=1.225 kg/m³`, drivetrain efficiency 95%.

## Segments table

`climbs` has a UUID primary key and these fields: `name`, `distance`, `average_grade`, `start_lat`, `start_lng`, `polyline`, `surface` (`'asphalt'` or `'non_asphalt'`, CHECK-constrained). Populated by the OSM importer; the app itself never writes to it.

## Migrations

Sequential files in `migrations/`. Order matters — PostGIS must be installed before any `geography` type is used:
1. `001_init.sql` — `CREATE EXTENSION postgis` (alone, because SQLx sends the whole file as one query and PostgreSQL can't resolve `geography` until the extension exists)
2. `002_tables.sql` — original `users` + `climbs` tables (users dropped in 007)
3. `003_gist_index.sql` — GIST index on `ST_MakePoint(start_lng, start_lat)::geography` (separate file for the same reason)
4. `004_sync_tracking.sql` — added `last_synced_at` to users (table later dropped)
5. `005_polyline.sql` — replaces `elevation_gain` with `polyline` + `star_count`
6. `006_climb_id.sql` — switches climbs primary key to UUID
7. `007_drop_strava_add_surface.sql` — drops `users` table, drops `climbs.strava_id` and `star_count`, adds `surface` column with CHECK constraint

## Environment

Copy `.env.example` to `.env`:
- `DATABASE_URL` — set automatically in Docker; override for local dev
- `BIND_ADDR` — defaults to `0.0.0.0:3000`

## Frontend

Single `templates/index.html` embedded at compile time via `include_str!`. Leaflet map on the left (double-click to pin, double-click+drag to set radius), form on the right. Hovering a result card draws the climb's polyline and places a marker at its start point. Each result shows a surface tag (asphalt vs unpaved).
