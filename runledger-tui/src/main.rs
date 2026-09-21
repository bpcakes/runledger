mod app;
mod config;
mod data;
mod database;
mod format;
mod scope;
mod terminal;
mod ui;

use clap::Parser;
use config::Config;
use runledger_postgres::ensure_schema_compatible_after_idempotency_cutover;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::parse();

    let database = database::connect(&config.database_url).await?;
    let pool = database.pool().clone();

    if !config.skip_schema_check {
        ensure_schema_compatible_after_idempotency_cutover(&database).await?;
    }

    terminal::run(pool, config).await?;

    Ok(())
}
