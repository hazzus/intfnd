use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use tracing::{error, info};
use uuid::Uuid;

use crate::{models::ClimbDetails, AppState};

pub async fn get_climb(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Response {
    info!(%id, "climb detail request");

    let climb = sqlx::query_as::<_, ClimbDetails>(
        "SELECT id, name, distance, average_grade, start_lat, start_lng, polyline,
                surfaces, is_paved, score, elevation_profile
         FROM climbs
         WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await;

    match climb {
        Ok(Some(c)) => Json(c).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            error!(err = ?e, "climb detail query failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
