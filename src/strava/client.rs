use std::sync::Arc;

use anyhow::{Context, Result};
use reqwest::{Client, RequestBuilder, Response, StatusCode};
use serde::de::DeserializeOwned;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::config::Config;
use super::rate_limiter::RateLimiter;
use super::types::{ActivitySummary, DetailedActivity, DetailedSegment, TokenResponse};

const STRAVA_API: &str = "https://www.strava.com/api/v3";
const STRAVA_AUTH: &str = "https://www.strava.com/oauth/token";
const MAX_RETRIES: u32 = 3;

#[derive(Clone)]
pub struct StravaClient {
    http: Client,
    config: Arc<Config>,
    rate_limiter: Arc<RateLimiter>,
}

impl StravaClient {
    pub fn new(config: Arc<Config>, rate_limiter: Arc<RateLimiter>) -> Self {
        Self { http: Client::new(), config, rate_limiter }
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
        let http = self.http.clone();
        let token = access_token.to_owned();
        self.api_request(move || {
            let mut req = http
                .get(format!("{STRAVA_API}/athlete/activities"))
                .bearer_auth(&token)
                .query(&[("per_page", "200"), ("page", &page.to_string())]);
            if let Some(ts) = after {
                req = req.query(&[("after", ts)]);
            }
            req
        })
        .await
        .context("list activities")
    }

    pub async fn get_segment(&self, access_token: &str, id: i64) -> Result<DetailedSegment> {
        let http = self.http.clone();
        let token = access_token.to_owned();
        self.api_request(move || {
            http.get(format!("{STRAVA_API}/segments/{id}"))
                .bearer_auth(&token)
        })
        .await
        .context("get segment")
    }

    pub async fn get_activity(&self, access_token: &str, id: i64) -> Result<DetailedActivity> {
        let http = self.http.clone();
        let token = access_token.to_owned();
        self.api_request(move || {
            http.get(format!("{STRAVA_API}/activities/{id}"))
                .bearer_auth(&token)
                .query(&[("include_all_efforts", "true")])
        })
        .await
        .context("get activity")
    }

    async fn api_request<T, F>(&self, build: F) -> Result<T>
    where
        T: DeserializeOwned,
        F: Fn() -> RequestBuilder,
    {
        for attempt in 0..MAX_RETRIES {
            while let Some(wait) = self.rate_limiter.try_acquire() {
                warn!(wait_secs = wait.as_secs(), "rate limit exhausted, sleeping until next window");
                sleep(wait).await;
            }

            let resp = build().send().await?;

            if let Some((used, limit)) = parse_rate_headers(&resp) {
                info!(used, limit, "rate limit status");
                self.rate_limiter.update_from_headers(used, limit);
            }

            if resp.status() == StatusCode::TOO_MANY_REQUESTS {
                let wait = self.rate_limiter.mark_exhausted();
                warn!(wait_secs = wait.as_secs(), attempt, "Strava 429, sleeping until next window");
                sleep(wait).await;
                continue;
            }

            return Ok(resp.error_for_status()?.json::<T>().await?);
        }
        Err(anyhow::anyhow!("rate limited after {MAX_RETRIES} retries"))
    }
}

fn parse_rate_headers(resp: &Response) -> Option<(u32, u32)> {
    let first = |name: &str| -> Option<u32> {
        resp.headers().get(name)?.to_str().ok()?.split(',').next()?.trim().parse().ok()
    };
    Some((first("X-RateLimit-Usage")?, first("X-RateLimit-Limit")?))
}
