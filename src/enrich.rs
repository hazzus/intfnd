use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;
use tokio::time::sleep;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::config::Config;
use crate::models::User;
use crate::strava::client::StravaClient;
use crate::strava::rate_limiter::RateLimiter;

pub fn spawn_enrich_task(pool: PgPool, config: Arc<Config>, rate_limiter: Arc<RateLimiter>) {
    info!("starting segment enrichment task");
    tokio::spawn(async move {
        loop {
            match run_pass(&pool, &config, &rate_limiter).await {
                Ok(0) => sleep(tokio::time::Duration::from_secs(300)).await,
                Ok(n) => info!(enriched = n, "segment enrichment pass complete"),
                Err(e) => {
                    error!(err = ?e, "segment enrichment pass failed");
                    sleep(tokio::time::Duration::from_secs(60)).await;
                }
            }
        }
    });
}

async fn run_pass(pool: &PgPool, config: &Arc<Config>, rate_limiter: &Arc<RateLimiter>) -> Result<usize> {
    let user = sqlx::query_as::<_, User>(
        "SELECT id, access_token, refresh_token, token_expires_at,
                sync_activities_total, sync_activities_done, last_synced_at
         FROM users ORDER BY token_expires_at DESC LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;

    let Some(user) = user else {
        info!("no users registered yet, skipping enrichment pass");
        return Ok(0);
    };

    let strava = StravaClient::new(Arc::clone(config), Arc::clone(rate_limiter));
    let access_token = fresh_token(pool, &strava, &user).await?;

    let rows: Vec<(Uuid, i64)> = sqlx::query_as(
        "SELECT id, strava_id FROM segments
         WHERE polyline IS NULL AND strava_id IS NOT NULL
         LIMIT 100",
    )
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        info!("all segments enriched, nothing to do");
        return Ok(0);
    }

    info!(count = rows.len(), "enriching segments");
    let mut enriched = 0usize;

    for (id, strava_id) in rows {
        match strava.get_segment(&access_token, strava_id).await {
            Ok(seg) => {
                sqlx::query(
                    "UPDATE segments SET polyline = $1, star_count = $2 WHERE id = $3",
                )
                .bind(seg.map.and_then(|m| m.polyline))
                .bind(seg.star_count)
                .bind(id)
                .execute(pool)
                .await?;
                enriched += 1;
            }
            Err(e) => warn!(strava_id, err = ?e, "failed to fetch segment detail, skipping"),
        }
    }

    Ok(enriched)
}

async fn fresh_token(pool: &PgPool, strava: &StravaClient, user: &User) -> Result<String> {
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
