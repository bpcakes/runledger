pub mod shared;

use runledger_postgres::run_atomic;
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
    let outcome = run_atomic(&pool, async |scope| {
        // Persist application changes through queue.application(...) when needed.
        scope.queue().enqueue_job(&request(&payload, &key)).await
    })
    .await?;
    println!("enqueued {}", outcome.job_id);
    pool.close().await;
    Ok(())
}
