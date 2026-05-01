use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::models::User;
use crate::strava::client::StravaClient;
use crate::strava::rate_limiter::RateLimiter;
use crate::strava::types::SegmentEffort;

pub fn spawn_sync_task(
    pool: PgPool,
    user_id: i64,
    config: Arc<Config>,
    sync_jobs: Arc<Mutex<HashSet<i64>>>,
    rate_limiter: Arc<RateLimiter>,
) {
    tokio::spawn(async move {
        if let Err(e) = run_sync(&pool, user_id, config, rate_limiter).await {
            error!(user_id, err = ?e, "sync failed");
        }
        sync_jobs.lock().unwrap().remove(&user_id);
    });
}

async fn run_sync(pool: &PgPool, user_id: i64, config: Arc<Config>, rate_limiter: Arc<RateLimiter>) -> Result<()> {
    let strava = StravaClient::new(config, rate_limiter);
    let user = load_user(pool, user_id).await?;
    let access_token = ensure_fresh_token(pool, &strava, &user).await?;
    let after = user.last_synced_at.map(|t| t.timestamp());

    if let Some(ts) = after {
        info!(user_id, after = ts, "incremental sync");
    } else {
        info!(user_id, "full sync");
    }

    // collect all (id, start_date) pairs, oldest first
    let mut activities: Vec<(i64, DateTime<Utc>)> = Vec::new();
    let mut page = 1u32;
    loop {
        let batch = strava.list_activities(&access_token, page, after).await?;
        if batch.is_empty() { break; }
        activities.extend(batch.iter().map(|a| (a.id, a.start_date)));
        page += 1;
    }
    activities.sort_by_key(|&(_, date)| date);

    if activities.is_empty() {
        info!(user_id, "no new activities to sync");
        return Ok(());
    }

    sqlx::query("UPDATE users SET sync_activities_total = $1, sync_activities_done = 0 WHERE id = $2")
        .bind(activities.len() as i32)
        .bind(user_id)
        .execute(pool)
        .await?;

    info!(user_id, total = activities.len(), "syncing segments");

    for (activity_id, start_date) in activities {
        match strava.get_activity(&access_token, activity_id).await {
            Ok(activity) => {
                if let Err(e) = upsert_segments(pool, &activity.segment_efforts).await {
                    warn!(activity_id, err = ?e, "failed to upsert segments");
                }
            }
            Err(e) => warn!(activity_id, err = ?e, "skipping activity"),
        }

        sqlx::query(
            "UPDATE users SET sync_activities_done = sync_activities_done + 1,
                              last_synced_at = $1
             WHERE id = $2",
        )
        .bind(start_date + Duration::seconds(1))
        .bind(user_id)
        .execute(pool)
        .await?;
    }

    info!(user_id, "sync complete");
    Ok(())
}

async fn upsert_segments(pool: &PgPool, efforts: &[SegmentEffort]) -> Result<()> {
    for effort in efforts {
        let seg = &effort.segment;
        if seg.private || seg.start_latlng.len() < 2 {
            continue;
        }
        let polyline = seg.map.as_ref().and_then(|m| m.polyline.clone());
        sqlx::query(
            "INSERT INTO segments (strava_id, name, distance, average_grade, start_lat, start_lng, polyline)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (strava_id) DO NOTHING",
        )
        .bind(seg.id)
        .bind(&seg.name)
        .bind(seg.distance)
        .bind(seg.average_grade)
        .bind(seg.start_latlng[0])
        .bind(seg.start_latlng[1])
        .bind(polyline)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn load_user(pool: &PgPool, user_id: i64) -> Result<User> {
    sqlx::query_as::<_, User>(
        "SELECT id, access_token, refresh_token, token_expires_at,
                sync_activities_total, sync_activities_done, last_synced_at
         FROM users WHERE id = $1",
    )
    .bind(user_id)
    .fetch_one(pool)
    .await
    .map_err(Into::into)
}

async fn ensure_fresh_token(pool: &PgPool, strava: &StravaClient, user: &User) -> Result<String> {
    if user.token_expires_at > Utc::now() + Duration::minutes(5) {
        return Ok(user.access_token.clone());
    }
    let refreshed = strava.refresh_token(&user.refresh_token).await?;
    let expires_at = DateTime::from_timestamp(refreshed.expires_at, 0)
        .ok_or_else(|| anyhow::anyhow!("invalid expires_at"))?;
    sqlx::query(
        "UPDATE users SET access_token = $1, refresh_token = $2, token_expires_at = $3 WHERE id = $4",
    )
    .bind(&refreshed.access_token)
    .bind(&refreshed.refresh_token)
    .bind(expires_at)
    .bind(user.id)
    .execute(pool)
    .await?;
    Ok(refreshed.access_token)
}
