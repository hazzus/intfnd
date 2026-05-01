mod config;
mod db;
mod enrich;
mod models;
mod physics;
mod routes;
mod strava;
mod sync;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use axum::{
    routing::{get, post},
    Router,
};
use axum_extra::extract::cookie::Key;

use config::Config;
use strava::rate_limiter::RateLimiter;

#[derive(Clone)]
pub struct AppState {
    pub pool: sqlx::PgPool,
    pub config: Arc<Config>,
    pub cookie_key: Key,
    pub sync_jobs: Arc<Mutex<HashSet<i64>>>,
    pub rate_limiter: Arc<RateLimiter>,
}

impl axum::extract::FromRef<AppState> for Key {
    fn from_ref(state: &AppState) -> Self {
        state.cookie_key.clone()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = Arc::new(Config::from_env()?);
    let bind_addr = config.bind_addr.clone();
    let pool = db::init(&config.database_url).await?;
    let secret = config.cookie_secret.as_bytes();
    anyhow::ensure!(secret.len() >= 64, "COOKIE_SECRET must be at least 64 bytes");
    let cookie_key = Key::from(secret);

    let state = AppState {
        pool,
        config,
        cookie_key,
        sync_jobs: Arc::new(Mutex::new(HashSet::new())),
        rate_limiter: Arc::new(RateLimiter::new()),
    };

    enrich::spawn_enrich_task(state.pool.clone(), Arc::clone(&state.config), Arc::clone(&state.rate_limiter));

    let app = Router::new()
        .route("/", get(routes::pages::index))
        .route("/icon.png", get(routes::pages::icon))
        .route("/auth/strava", get(routes::auth::login))
        .route("/auth/strava/callback", get(routes::auth::callback))
        .route("/api/search", post(routes::search::search))
        .route("/api/sync/status", get(routes::sync::status))
        .route("/api/sync/start", post(routes::sync::start))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    tracing::info!("listening on {bind_addr}");
    axum::serve(listener, app).await?;
    Ok(())
}
