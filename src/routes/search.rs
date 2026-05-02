use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info};
use uuid::Uuid;

use crate::{models::Segment, physics, AppState};

#[derive(Deserialize)]
pub struct SearchRequest {
    pub lat: f64,
    pub lng: f64,
    pub radius_m: f64,
    pub weight_kg: f64,
    pub power_w: f64,
    pub interval_s: f64,
}

#[derive(Serialize)]
pub struct SearchResult {
    pub id: Uuid,
    pub name: String,
    pub distance_m: f64,
    pub average_grade: f64,
    pub estimated_time_s: f64,
    pub delta_s: f64,
    pub start_lat: f64,
    pub start_lng: f64,
    pub polyline: Option<String>,
    pub surface: String,
}

pub async fn search(
    State(state): State<AppState>,
    Json(req): Json<SearchRequest>,
) -> Response {
    info!(lat = req.lat, lng = req.lng, radius_m = req.radius_m, weight_kg = req.weight_kg, power_w = req.power_w, interval_s = req.interval_s, "search request");

    let segments = match sqlx::query_as::<_, Segment>(
        "SELECT id, name, distance, average_grade, start_lat, start_lng, polyline, surface
         FROM segments
         WHERE ST_DWithin(
             ST_MakePoint(start_lng, start_lat)::geography,
             ST_MakePoint($1, $2)::geography,
             $3
         )",
    )
    .bind(req.lng)
    .bind(req.lat)
    .bind(req.radius_m)
    .fetch_all(&state.pool)
    .await
    {
        Ok(s) => s,
        Err(e) => {
            error!(err = ?e, "search query failed");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let mut results: Vec<SearchResult> = segments
        .into_iter()
        .filter_map(|seg| {
            let t = physics::estimated_time(
                seg.distance,
                seg.average_grade,
                req.weight_kg,
                req.power_w,
            )?;
            let margin = req.interval_s * 0.1;
            if t < req.interval_s - margin {
                return None;
            }
            Some(SearchResult {
                id: seg.id,
                name: seg.name,
                distance_m: seg.distance,
                average_grade: seg.average_grade,
                estimated_time_s: t,
                delta_s: t - req.interval_s,
                start_lat: seg.start_lat,
                start_lng: seg.start_lng,
                polyline: seg.polyline,
                surface: seg.surface,
            })
        })
        .collect();

    results.sort_by(|a, b| {
        let sa = (a.delta_s / req.interval_s).powi(2);
        let sb = (b.delta_s / req.interval_s).powi(2);
        sa.partial_cmp(&sb).unwrap_or(std::cmp::Ordering::Equal)
    });

    info!(count = results.len(), "search results");
    Json(results).into_response()
}
