use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use axum_extra::extract::cookie::PrivateCookieJar;
use serde::Serialize;

use tracing::error;

use crate::AppState;

#[derive(Serialize)]
pub struct SyncStatus {
    pub total: i32,
    pub done: i32,
}

pub async fn status(State(state): State<AppState>, jar: PrivateCookieJar) -> Response {
    let user_id: i64 = match jar
        .get("user_id")
        .and_then(|c| c.value().parse().ok())
    {
        Some(id) => id,
        None => return StatusCode::UNAUTHORIZED.into_response(),
    };

    match sqlx::query_as::<_, (i32, i32)>(
        "SELECT sync_activities_total, sync_activities_done FROM users WHERE id = $1",
    )
    .bind(user_id)
    .fetch_one(&state.pool)
    .await
    {
        Ok((total, done)) => Json(SyncStatus { total, done }).into_response(),
        Err(e) => {
            error!(user_id, err = ?e, "failed to fetch sync status");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
