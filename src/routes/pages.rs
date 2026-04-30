use axum::response::{Html, IntoResponse};

const INDEX_PAGE: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/templates/index.html"));

pub async fn index() -> impl IntoResponse {
    Html(INDEX_PAGE)
}
