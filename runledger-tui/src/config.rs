use clap::Parser;
use uuid::Uuid;

#[derive(Debug, Clone, Parser)]
#[command(
    name = "runledger-tui",
    about = "Read-only terminal UI for Runledger queue and workflow monitoring"
)]
pub struct Config {
    /// PostgreSQL connection URL (or set DATABASE_URL).
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: String,

    /// Scope all queries to this organization UUID. Omit for global (all orgs).
    #[arg(long)]
    pub org: Option<Uuid>,

    /// Background refresh interval in milliseconds.
    #[arg(long, default_value_t = 2000)]
    pub refresh_ms: u64,

    /// Maximum rows returned per list query.
    #[arg(long, default_value_t = 100)]
    pub limit: i64,

    /// Skip read-only schema compatibility check on startup.
    #[arg(long)]
    pub skip_schema_check: bool,
}

impl Config {
    #[must_use]
    pub fn refresh_interval(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.refresh_ms)
    }
}
