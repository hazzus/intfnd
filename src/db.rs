use anyhow::Result;
use sqlx::PgPool;

pub async fn init(database_url: &str) -> Result<PgPool> {
    let pool = PgPool::connect(database_url).await?;
    sqlx::migrate!().run(&pool).await?;
    Ok(pool)
}
