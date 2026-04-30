use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{Timelike, Utc};
use reqwest::{Client, Response, StatusCode};
use serde::de::DeserializeOwned;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

use crate::config::Config;
use super::types::{ActivitySummary, DetailedActivity, TokenResponse};

const STRAVA_API: &str = "https://www.strava.com/api/v3";
const STRAVA_AUTH: &str = "https://www.strava.com/oauth/token";

#[derive(Clone)]
pub struct StravaClient {
    http: Client,
    config: Arc<Config>,
}

impl StravaClient {
    pub fn new(config: Arc<Config>) -> Self {
        Self { http: Client::new(), config }
    }

    pub async fn exchange_token(&self, code: &str) -> Result<TokenResponse> {
        self.http
            .post(STRAVA_AUTH)
            .form(&[
                ("client_id", self.config.strava_client_id.as_str()),
                ("client_secret", self.config.strava_client_secret.as_str()),
                ("code", code),
                ("grant_type", "authorization_code"),
            ])
            .send().await.context("token exchange request")?
            .error_for_status().context("token exchange status")?
            .json::<TokenResponse>().await.context("token exchange parse")
    }

    pub async fn refresh_token(&self, refresh_token: &str) -> Result<TokenResponse> {
        self.http
            .post(STRAVA_AUTH)
            .form(&[
                ("client_id", self.config.strava_client_id.as_str()),
                ("client_secret", self.config.strava_client_secret.as_str()),
                ("refresh_token", refresh_token),
                ("grant_type", "refresh_token"),
            ])
            .send().await.context("token refresh request")?
            .error_for_status().context("token refresh status")?
            .json::<TokenResponse>().await.context("token refresh parse")
    }

    pub async fn list_activities(
        &self,
        access_token: &str,
        page: u32,
        after: Option<i64>,
    ) -> Result<Vec<ActivitySummary>> {
        let mut req = self.http
            .get(format!("{STRAVA_API}/athlete/activities"))
            .bearer_auth(access_token)
            .query(&[("per_page", "200"), ("page", &page.to_string())]);
        if let Some(ts) = after {
            req = req.query(&[("after", ts)]);
        }
        let response = req.send().await.context("list activities request")?;
        handle_response(response).await.context("list activities")
    }

    pub async fn get_activity(&self, access_token: &str, id: i64) -> Result<DetailedActivity> {
        let response = self.http
            .get(format!("{STRAVA_API}/activities/{id}"))
            .bearer_auth(access_token)
            .query(&[("include_all_efforts", "true")])
            .send().await.context("get activity request")?;
        handle_response(response).await.context("get activity")
    }
}

async fn handle_response<T: DeserializeOwned>(response: Response) -> Result<T> {
    if response.status() == StatusCode::TOO_MANY_REQUESTS {
        let wait = secs_until_next_window();
        warn!(wait_secs = wait, "Strava 429: sleeping until next rate limit window");
        sleep(Duration::from_secs(wait)).await;
        return Err(anyhow::anyhow!("rate limited by Strava (429)"));
    }

    let delay = rate_limit_delay(&response);
    let body = response.error_for_status()?.json::<T>().await?;
    sleep(delay).await;
    Ok(body)
}

fn rate_limit_delay(response: &Response) -> Duration {
    let parse_header = |name: &str| -> Option<(u32, u32)> {
        let val = response.headers().get(name)?.to_str().ok()?;
        let mut parts = val.split(',');
        let a: u32 = parts.next()?.trim().parse().ok()?;
        let b: u32 = parts.next()?.trim().parse().ok()?;
        Some((a, b))
    };

    let Some((fifteen_limit, _)) = parse_header("X-RateLimit-Limit") else {
        return Duration::from_millis(200);
    };
    let Some((fifteen_used, _)) = parse_header("X-RateLimit-Usage") else {
        return Duration::from_millis(200);
    };

    let remaining = fifteen_limit.saturating_sub(fifteen_used);
    info!(fifteen_used, fifteen_limit, remaining, "rate limit status");

    if remaining == 0 {
        let wait = secs_until_next_window();
        warn!(wait_secs = wait, "rate limit exhausted, sleeping until next window");
        return Duration::from_secs(wait);
    }

    // spread remaining quota evenly across the rest of the window
    let window_secs = secs_until_next_window();
    let delay_ms = (window_secs * 1000) / remaining as u64;
    Duration::from_millis(delay_ms.max(100))
}

fn secs_until_next_window() -> u64 {
    let now = Utc::now();
    let mins = now.minute() as u64;
    let secs = now.second() as u64;
    let secs_past_boundary = (mins % 15) * 60 + secs;
    let window_secs = 15 * 60u64;
    // +5s buffer so we don't hit the boundary exactly
    window_secs - secs_past_boundary + 5
}
