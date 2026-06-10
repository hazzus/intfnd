use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info};
use uuid::Uuid;

use crate::{models::Climb, AppState};

const MAX_RESULTS: usize = 150;

// Screen-fit ranking: prefer climbs whose length is a good fraction of the
// search radius, then rank by score (penalty). `TARGET_FRAC` sets the ideal
// climb length relative to the radius; `FIT_WEIGHT` trades off size-fit vs score.
const TARGET_FRAC: f64 = 0.75;
const FIT_WEIGHT: f64 = 30.0;

#[derive(Deserialize)]
pub struct ExploreRequest {
    // viewport bounding box: only climbs starting on screen are returned
    pub min_lat: f64,
    pub min_lng: f64,
    pub max_lat: f64,
    pub max_lng: f64,
    // half-diagonal of the view, used only for the fit-ranking scale
    pub radius_m: f64,
    pub min_distance_m: Option<f64>,
    pub max_distance_m: Option<f64>,
    pub min_grade: Option<f64>,
    pub max_grade: Option<f64>,
    // null = any surface, true = paved only, false = unpaved only
    #[serde(default)]
    pub paved: Option<bool>,
    #[serde(default)]
    pub bidirectional_only: bool,
}

#[derive(Serialize)]
pub struct ExploreResult {
    pub id: Uuid,
    pub name: String,
    pub distance_m: f64,
    pub average_grade: f64,
    pub start_lat: f64,
    pub start_lng: f64,
    pub polyline: Option<String>,
    pub surfaces: Vec<String>,
    pub is_paved: bool,
    pub bidirectional: bool,
    pub score: f64,
}

pub async fn explore(
    State(state): State<AppState>,
    Json(req): Json<ExploreRequest>,
) -> Response {
    info!(
        min_lat = req.min_lat,
        min_lng = req.min_lng,
        max_lat = req.max_lat,
        max_lng = req.max_lng,
        radius_m = req.radius_m,
        min_distance_m = req.min_distance_m,
        max_distance_m = req.max_distance_m,
        min_grade = req.min_grade,
        max_grade = req.max_grade,
        paved = req.paved,
        bidirectional_only = req.bidirectional_only,
        "explore request"
    );

    let climbs = match sqlx::query_as::<_, Climb>(
        "SELECT id, name, distance, average_grade, start_lat, start_lng, polyline, surfaces, is_paved, bidirectional, score
         FROM climbs
         WHERE start_lat BETWEEN $1 AND $2
         AND start_lng BETWEEN $3 AND $4
         AND ($5::float8 IS NULL OR distance >= $5)
         AND ($6::float8 IS NULL OR distance <= $6)
         AND ($7::float8 IS NULL OR average_grade >= $7)
         AND ($8::float8 IS NULL OR average_grade <= $8)
         AND ($9::bool IS NULL OR is_paved = $9)
         AND (NOT $10 OR bidirectional = TRUE)
         ORDER BY power(ln(distance / $12), 2) * $13 + score ASC
         LIMIT $11",
    )
    .bind(req.min_lat)
    .bind(req.max_lat)
    .bind(req.min_lng)
    .bind(req.max_lng)
    .bind(req.min_distance_m)
    .bind(req.max_distance_m)
    .bind(req.min_grade)
    .bind(req.max_grade)
    .bind(req.paved)
    .bind(req.bidirectional_only)
    .bind(MAX_RESULTS as i64)
    .bind((req.radius_m * TARGET_FRAC).max(1.0))
    .bind(FIT_WEIGHT)
    .fetch_all(&state.pool)
    .await
    {
        Ok(c) => c,
        Err(e) => {
            error!(err = ?e, "explore query failed");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let results: Vec<ExploreResult> = climbs
        .into_iter()
        .map(|c| ExploreResult {
            id: c.id,
            name: c.name,
            distance_m: c.distance,
            average_grade: c.average_grade,
            start_lat: c.start_lat,
            start_lng: c.start_lng,
            polyline: c.polyline,
            surfaces: c.surfaces,
            is_paved: c.is_paved,
            bidirectional: c.bidirectional,
            score: c.score,
        })
        .collect();

    info!(count = results.len(), "explore results");
    Json(results).into_response()
}
