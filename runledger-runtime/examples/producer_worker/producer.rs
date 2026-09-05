pub mod shared;

use runledger_postgres::jobs::enqueue_job_tx;
use shared::{Greeting, request};
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let name = std::env::args()
        .nth(1)
        .ok_or("usage: producer <name> <request-key>")?;
    let key = std::env::args().nth(2).ok_or("missing request-key")?;
    let pool = PgPoolOptions::new()
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;
    runledger_postgres::ensure_schema_compatible_after_idempotency_cutover(&pool).await?;

    let payload = serde_json::to_value(Greeting { name })?;
    let mut tx = pool.begin().await?;
    // Persist application changes with this same transaction when needed.
    let job_id = enqueue_job_tx(&mut tx, &request(&payload, &key)).await?;
    tx.commit().await?;
    println!("enqueued {job_id}");
    pool.close().await;
    Ok(())
}
