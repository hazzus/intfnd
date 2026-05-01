use axum::response::{Html, IntoResponse};
use axum::http::header;

const INDEX_PAGE: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/templates/index.html"));
const ICON: &[u8] = include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/templates/resources/icon.png"));

pub async fn index() -> impl IntoResponse {
    Html(INDEX_PAGE)
}

pub async fn icon() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "image/png")], ICON)
}
