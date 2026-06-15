use clap::Parser;
use runledger_postgres::jobs::JOB_LIST_PAGE_LIMIT_MAX;
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
    #[arg(long, default_value_t = 100, value_parser = parse_limit)]
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

fn parse_limit(value: &str) -> Result<i64, String> {
    let limit = value
        .parse::<i64>()
        .map_err(|error| format!("limit must be an integer: {error}"))?;

    if !(1..=JOB_LIST_PAGE_LIMIT_MAX).contains(&limit) {
        return Err(format!(
            "limit must be between 1 and {JOB_LIST_PAGE_LIMIT_MAX}"
        ));
    }

    Ok(limit)
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    fn parse_with_limit(limit: &str) -> clap::error::Result<Config> {
        Config::try_parse_from([
            "runledger-tui",
            "--database-url",
            "postgres://localhost/runledger",
            "--limit",
            limit,
        ])
    }

    #[test]
    fn limit_accepts_positive_values_within_api_bound() {
        let config = parse_with_limit("250").expect("valid limit should parse");
        assert_eq!(config.limit, 250);
    }

    #[test]
    fn limit_rejects_non_positive_values() {
        assert!(parse_with_limit("0").is_err());
        assert!(parse_with_limit("-1").is_err());
    }

    #[test]
    fn limit_rejects_values_above_api_bound() {
        assert!(parse_with_limit(&(JOB_LIST_PAGE_LIMIT_MAX + 1).to_string()).is_err());
    }
}
