use serde::Serialize;
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, FromRow, Serialize)]
pub struct Segment {
    pub id: Uuid,
    pub name: String,
    pub distance: f64,
    pub average_grade: f64,
    pub start_lat: f64,
    pub start_lng: f64,
    pub polyline: Option<String>,
    pub surface: String,
}
