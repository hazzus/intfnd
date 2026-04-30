use std::sync::Arc;

use axum::{
    extract::{Query, State},
    response::{IntoResponse, Redirect, Response},
};
use axum_extra::extract::cookie::{Cookie, PrivateCookieJar};
use chrono::DateTime;
use serde::Deserialize;
use tracing::{error, info};

use crate::{strava::client::StravaClient, AppState};

pub async fn login(State(state): State<AppState>) -> impl IntoResponse {
    let mut url = reqwest::Url::parse("https://www.strava.com/oauth/authorize").unwrap();
    url.query_pairs_mut()
        .append_pair("client_id", &state.config.strava_client_id)
        .append_pair("redirect_uri", &state.config.strava_redirect_uri)
        .append_pair("response_type", "code")
        .append_pair("scope", "activity:read_all");
    Redirect::to(url.as_str())
}

#[derive(Deserialize)]
pub struct CallbackParams {
    pub code: String,
}

pub async fn callback(
    State(state): State<AppState>,
    jar: PrivateCookieJar,
    Query(params): Query<CallbackParams>,
) -> Response {
    info!("oauth callback received");
    let strava = StravaClient::new(Arc::clone(&state.config));

    info!("exchanging token with strava");
    let token = match strava.exchange_token(&params.code).await {
        Ok(t) => t,
        Err(e) => {
            error!(err = ?e, "token exchange failed");
            return (jar, Redirect::to("/auth/error")).into_response();
        }
    };

    let athlete = match token.athlete {
        Some(a) => a,
        None => {
            error!("no athlete in token response");
            return (jar, Redirect::to("/auth/error")).into_response();
        }
    };
    info!(athlete_id = athlete.id, "token exchange ok");

    let expires_at = match DateTime::from_timestamp(token.expires_at, 0) {
        Some(dt) => dt,
        None => {
            error!("invalid expires_at");
            return (jar, Redirect::to("/auth/error")).into_response();
        }
    };

    if let Err(e) = sqlx::query(
        "INSERT INTO users (id, access_token, refresh_token, token_expires_at)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (id) DO UPDATE
           SET access_token = EXCLUDED.access_token,
               refresh_token = EXCLUDED.refresh_token,
               token_expires_at = EXCLUDED.token_expires_at",
    )
    .bind(athlete.id)
    .bind(&token.access_token)
    .bind(&token.refresh_token)
    .bind(expires_at)
    .execute(&state.pool)
    .await
    {
        error!(err = ?e, "failed to upsert user");
        return (jar, Redirect::to("/auth/error")).into_response();
    }

    let should_sync = state.sync_jobs.lock().unwrap().insert(athlete.id);
    if should_sync {
        info!(athlete_id = athlete.id, "spawning sync task");
        crate::sync::spawn_sync_task(
            state.pool.clone(),
            athlete.id,
            Arc::clone(&state.config),
            Arc::clone(&state.sync_jobs),
        );
    } else {
        info!(athlete_id = athlete.id, "sync already running, skipping");
    }

    let jar = jar.add(Cookie::build(("user_id", athlete.id.to_string())).path("/").build());
    info!(athlete_id = athlete.id, "cookie set, redirecting to /");
    (jar, Redirect::to("/")).into_response()
}
