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

Generate a cookie secret (must be ≥ 64 bytes):
```bash
openssl rand -hex 64
```

## Architecture

Axum 0.8 + Tokio web server, PostgreSQL + PostGIS for storage, SQLx 0.8 (runtime queries, no compile-time macros). Single binary, single process — background sync runs as `tokio::spawn`.

**AppState** (`src/main.rs`): holds `PgPool`, `Arc<Config>`, `Key` (cookie encryption), and `Arc<Mutex<HashSet<i64>>>` for in-flight sync job tracking. `FromRef<AppState> for Key` is implemented so `PrivateCookieJar` can extract the key automatically.

**Routes:**
- `GET /` — serves `templates/index.html` (single page, all auth/sync state handled by JS)
- `GET /auth/strava` — redirects to Strava OAuth
- `GET /auth/strava/callback` — exchanges code, upserts user, sets encrypted cookie (`user_id`, path `/`), spawns sync task
- `POST /api/search` — PostGIS radius query + physics filter, returns ranked segments
- `GET /api/sync/status` — returns `{total, done}` for the authed user; 401 if not logged in (frontend uses this to decide whether to show auth button or progress bar)
- `POST /mcp` — MCP streamable HTTP transport (rmcp 1.6.0); tools: `find_segments`, `list_known_locations`; no auth required

**Sync** (`src/sync.rs`): fetches all Strava activities (paginated, oldest-first), then fetches full detail for each and upserts segments. `last_synced_at` is updated per-activity using the activity's own `start_date + 1s`, enabling incremental sync on re-login. The in-memory `sync_jobs` set prevents duplicate tasks per user.

**Physics** (`src/physics.rs`): three-force cycling model — gravity (`m·g·grade`), rolling resistance (`m·g·Crr`), and aerodynamic drag (`½·ρ·CdA·v²`). Solves for velocity using Newton's method on the cubic power equation (`P = F_total · v`), then returns `distance / v`. Constants: `Crr=0.004`, `CdA=0.32 m²`, `ρ=1.225 kg/m³`, drivetrain efficiency 95%.

**Strava rate limiting** (`src/strava/client.rs`): reads `X-RateLimit-Usage` header and sleeps proportionally; on 429 response, sleeps until the next 15-minute window boundary.

## Migrations

Four sequential files in `migrations/`. Order matters — PostGIS must be installed before any `geography` type is used:
1. `001_init.sql` — `CREATE EXTENSION postgis` (alone, because SQLx sends the whole file as one query and PostgreSQL can't resolve `geography` until the extension exists)
2. `002_tables.sql` — `users` and `segments` tables
3. `003_gist_index.sql` — GIST index on `ST_MakePoint(start_lng, start_lat)::geography` (separate file for the same reason)
4. `004_sync_tracking.sql` — adds `last_synced_at` to users

## Environment

Copy `.env.example` to `.env` and fill in:
- `STRAVA_CLIENT_ID`, `STRAVA_CLIENT_SECRET` — from your Strava API app
- `STRAVA_REDIRECT_URI` — must match what's registered in Strava (e.g. `http://localhost:3000/auth/strava/callback`)
- `COOKIE_SECRET` — at least 64 bytes, generate with `openssl rand -hex 64`
- `DATABASE_URL` — set automatically in Docker; override for local dev

## Frontend

Single `templates/index.html` embedded at compile time via `include_str!`. Leaflet map on the left (double-click to pin, double-click+drag to set radius), form on the right. On load, JS calls `/api/sync/status`: 401 → shows Strava connect button; 200 → shows activity sync progress bar. Hovering a result card places an orange circle marker on the segment's start point.
