use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: i64,
    pub athlete: Option<AthleteInfo>,
}

#[derive(Debug, Deserialize)]
pub struct AthleteInfo {
    pub id: i64,
}

#[derive(Debug, Deserialize)]
pub struct ActivitySummary {
    pub id: i64,
    pub start_date: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct DetailedActivity {
    #[serde(default)]
    pub segment_efforts: Vec<SegmentEffort>,
}

#[derive(Debug, Deserialize)]
pub struct SegmentEffort {
    pub segment: SegmentSummary,
}

#[derive(Debug, Deserialize)]
pub struct SegmentSummary {
    pub id: i64,
    pub name: String,
    pub distance: f64,
    pub average_grade: f64,
    #[serde(default)]
    pub start_latlng: Vec<f64>,
    pub total_elevation_gain: Option<f64>,
}
