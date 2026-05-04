mod config;
mod db;
mod models;
mod physics;
mod routes;

use std::sync::Arc;

use axum::{
    routing::{get, post},
    Router,
};

use config::Config;

#[derive(Clone)]
pub struct AppState {
    pub pool: sqlx::PgPool,
    pub config: Arc<Config>,
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

    let state = AppState { pool, config };

    let app = Router::new()
        .route("/", get(routes::pages::index))
        .route("/about", get(routes::pages::about))
        .route("/icon.png", get(routes::pages::icon))
        .route("/api/search", post(routes::search::search))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    tracing::info!("listening on {bind_addr}");
    axum::serve(listener, app).await?;
    Ok(())
}
