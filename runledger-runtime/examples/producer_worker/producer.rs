pub mod shared;

use runledger_postgres::PgAtomicTransaction;
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
    let tx = PgAtomicTransaction::begin(&pool).await?;
    // Persist application changes through tx.application(...) when needed.
    let (tx, outcome) = tx.enqueue_job(&request(&payload, &key)).await?;
    let _confirmed = tx.commit().await?;
    println!("enqueued {}", outcome.job_id);
    pool.close().await;
    Ok(())
}
