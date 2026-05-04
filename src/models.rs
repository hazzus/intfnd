use serde::Serialize;
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, FromRow, Serialize)]
pub struct Climb {
    pub id: Uuid,
    pub name: String,
    pub distance: f64,
    pub average_grade: f64,
    pub start_lat: f64,
    pub start_lng: f64,
    pub polyline: Option<String>,
    pub surfaces: Vec<String>,
    pub is_paved: bool,
    pub score: f64,
}

#[derive(Debug, FromRow, Serialize)]
pub struct ClimbDetails {
    pub id: Uuid,
    pub name: String,
    pub distance: f64,
    pub average_grade: f64,
    pub start_lat: f64,
    pub start_lng: f64,
    pub polyline: Option<String>,
    pub surfaces: Vec<String>,
    pub is_paved: bool,
    pub score: f64,
    pub elevation_profile: Vec<f32>,
}
