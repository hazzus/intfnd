use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use axum_extra::extract::cookie::PrivateCookieJar;
use serde::{Deserialize, Serialize};
use tracing::{error, info};

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
    pub strava_id: i64,
    pub name: String,
    pub distance_m: f64,
    pub average_grade: f64,
    pub estimated_time_s: f64,
    pub delta_s: f64,
    pub start_lat: f64,
    pub start_lng: f64,
    pub polyline: Option<String>,
    pub star_count: i32,
}

pub async fn search(
    State(state): State<AppState>,
    jar: PrivateCookieJar,
    Json(req): Json<SearchRequest>,
) -> Response {
    if jar.get("user_id").is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    info!(lat = req.lat, lng = req.lng, radius_m = req.radius_m, weight_kg = req.weight_kg, power_w = req.power_w, interval_s = req.interval_s, "search request");

    let segments = match sqlx::query_as::<_, Segment>(
        "SELECT strava_id, name, distance, average_grade, start_lat, start_lng, polyline, star_count
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
                strava_id: seg.strava_id,
                name: seg.name,
                distance_m: seg.distance,
                average_grade: seg.average_grade,
                estimated_time_s: t,
                delta_s: t - req.interval_s,
                start_lat: seg.start_lat,
                start_lng: seg.start_lng,
                polyline: seg.polyline,
                star_count: seg.star_count,
            })
        })
        .collect();

    results.sort_by(|a, b| {
        a.delta_s
            .partial_cmp(&b.delta_s)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(b.star_count.cmp(&a.star_count))
    });

    info!(count = results.len(), "search results");
    Json(results).into_response()
}
