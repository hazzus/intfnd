use std::env;

#[derive(Clone, Debug)]
pub struct Config {
    pub database_url: String,
    pub strava_client_id: String,
    pub strava_client_secret: String,
    pub strava_redirect_uri: String,
    pub cookie_secret: String,
    pub bind_addr: String,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            database_url: env::var("DATABASE_URL")
                .map_err(|_| anyhow::anyhow!("DATABASE_URL not set"))?,
            strava_client_id: env::var("STRAVA_CLIENT_ID")
                .map_err(|_| anyhow::anyhow!("STRAVA_CLIENT_ID not set"))?,
            strava_client_secret: env::var("STRAVA_CLIENT_SECRET")
                .map_err(|_| anyhow::anyhow!("STRAVA_CLIENT_SECRET not set"))?,
            strava_redirect_uri: env::var("STRAVA_REDIRECT_URI")
                .map_err(|_| anyhow::anyhow!("STRAVA_REDIRECT_URI not set"))?,
            cookie_secret: env::var("COOKIE_SECRET")
                .map_err(|_| anyhow::anyhow!("COOKIE_SECRET not set"))?,
            bind_addr: env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".to_string()),
        })
    }
}
