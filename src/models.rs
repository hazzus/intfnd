use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, FromRow)]
#[allow(dead_code)]
pub struct User {
    pub id: i64,
    pub access_token: String,
    pub refresh_token: String,
    pub token_expires_at: DateTime<Utc>,
    pub sync_activities_total: i32,
    pub sync_activities_done: i32,
    pub last_synced_at: Option<DateTime<Utc>>,
}

#[derive(Debug, FromRow, Serialize)]
pub struct Segment {
    pub id: Uuid,
    pub strava_id: Option<i64>,
    pub name: String,
    pub distance: f64,
    pub average_grade: f64,
    pub start_lat: f64,
    pub start_lng: f64,
    pub polyline: Option<String>,
    pub star_count: i32,
}
