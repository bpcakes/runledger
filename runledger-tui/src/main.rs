mod app;
mod config;
mod data;
mod format;
mod scope;
mod terminal;
mod ui;

use clap::Parser;
use config::Config;
use runledger_postgres::{DbPool, ensure_schema_compatible_after_idempotency_cutover};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::parse();

    let pool = DbPool::connect(&config.database_url).await?;

    if !config.skip_schema_check {
        ensure_schema_compatible_after_idempotency_cutover(&pool).await?;
    }

    terminal::run(pool, config).await?;

    Ok(())
}
