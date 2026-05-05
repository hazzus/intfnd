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

**Source layout:**
- `src/main.rs` — wires the router and `AppState`
- `src/config.rs` — env-driven `Config`
- `src/db.rs` — pool init + migration runner
- `src/models.rs` — `Climb` (search row) and `ClimbDetails` (full row with `elevation_profile` and `osm_way_ids`)
- `src/physics.rs` — cycling power model
- `src/routes/{pages,search,climb}.rs` — handlers

**Routes:**
- `GET /` — serves `templates/index.html`
- `GET /about` — serves `templates/about.html`
- `GET /climb/{id}` — serves `templates/climb.html`
- `GET /icon.png` — site icon
- `POST /api/search` — PostGIS radius query + physics filter, returns climbs ranked by composite score
- `GET /api/climb/{id}` — full `ClimbDetails` JSON for the climb page

**Physics** (`src/physics.rs`): three-force cycling model — gravity (`m·g·grade`), rolling resistance (`m·g·Crr`), and aerodynamic drag (`½·ρ·CdA·v²`). Solves for velocity using Newton's method on the cubic power equation (`P = F_total · v`), then returns `distance / v`. Constants: `Crr=0.004`, `CdA=0.32 m²`, `ρ=1.225 kg/m³`, drivetrain efficiency 95%.

**Ranking** (`src/routes/search.rs::calc_score`): combines a quadratic time-delta penalty (`(delta_s / interval_s)²`), an unimplemented distance term, and the climb's stored `score`. Lower scores rank first. Climbs whose estimated time falls more than 5% below the requested interval are filtered out before ranking.

## Climbs table

`climbs` has a UUID primary key with these fields:
- `name`, `distance`, `average_grade`, `start_lat`, `start_lng`, `polyline`
- `surfaces TEXT[]` — every OSM surface tag along the way
- `is_paved BOOLEAN` — derived rollup of `surfaces`
- `elevation_profile REAL[]` — sampled elevations along the polyline (note: source DEM is coarse, see `notes`)
- `osm_way_ids BIGINT[]` — source OSM ways
- `bidirectional BOOLEAN` — whether the climb is rideable in both directions
- `score DOUBLE PRECISION` — precomputed climb quality score

Uniqueness is `(start_lat, start_lng, osm_way_ids)`. Populated by the OSM importer; the app itself never writes to it.

## Migrations

Sequential files in `migrations/`, run by `src/db.rs` at startup. Order matters — PostGIS must be installed before any `geography` type is used, which is why some files are split:

1. `001_init.sql` — `CREATE EXTENSION postgis` (alone, because SQLx sends the whole file as one query)
2. `002_tables.sql` — original `users` + `segments` tables (users dropped in 007, segments renamed to climbs in 008)
3. `003_gist_index.sql` — GIST index on `ST_MakePoint(start_lng, start_lat)::geography`
4. `004_sync_tracking.sql` — added `last_synced_at` to users (table later dropped)
5. `005_polyline.sql` — replaces `elevation_gain` with `polyline` + `star_count`
6. `006_segment_id.sql` — switches primary key to UUID
7. `007_drop_strava_add_surface.sql` — drops `users`, drops `strava_id` and `star_count`, adds singular `surface` column
8. `008_rename_to_climbs.sql` — TRUNCATEs and renames `segments` → `climbs`; drops `surface`; adds `surfaces[]`, `elevation_profile[]`, `osm_way_ids[]`, `bidirectional`, `score`; UNIQUE on `osm_way_ids`
9. `009_way_ids_constraints.sql` — relaxes uniqueness to `(start_lat, start_lng, osm_way_ids)`
10. `010_auto_is_paved.sql` — adds `is_paved BOOLEAN`

## Environment

Copy `.env.example` to `.env`:
- `DATABASE_URL` — set automatically in Docker; override for local dev
- `BIND_ADDR` — defaults to `0.0.0.0:3000`

## Frontend

Three HTML templates embedded at compile time via `include_str!`:
- `templates/index.html` — Leaflet map on the left (double-click to pin, double-click+drag to set radius), search form on the right. Hovering a result card draws the climb's polyline and places a marker at its start.
- `templates/climb.html` — climb detail page, fetched via `/api/climb/{id}`
- `templates/about.html` — about page

Result cards show a surface tag (asphalt vs unpaved) derived from `is_paved`.

## Importer

`scripts/import_osm_climbs.py` extracts climbs from OSM PBF + DEM rasters in `scripts/resources/` (`*.osm.pbf`, `*.tif`) and writes them to the `climbs` table. The app never mutates climbs at runtime.
