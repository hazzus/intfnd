use axum::response::{Html, IntoResponse};
use axum::http::header;

const INDEX_PAGE: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/templates/index.html"));
const ICON: &[u8] = include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/templates/resources/icon.png"));
const STRAVA_CONNECT: &[u8] = include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/templates/resources/strava-connect.png"));
const STRAVA_POWERED: &[u8] = include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/templates/resources/strava-powered.png"));

pub async fn index() -> impl IntoResponse {
    Html(INDEX_PAGE)
}

pub async fn icon() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "image/png")], ICON)
}

pub async fn strava_connect() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "image/png")], STRAVA_CONNECT)
}

pub async fn strava_powered() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "image/png")], STRAVA_POWERED)
}
