use crate::{DbTx, Error, QueryError, QueryErrorCategory, Result};

pub(crate) async fn ensure_read_committed_tx(
    tx: &mut DbTx<'_>,
    operation: &'static str,
    code: &'static str,
    client_message: &'static str,
) -> Result<()> {
    // SHOW reports the effective isolation for the current transaction, so a
    // caller's SET TRANSACTION change is visible here. PostgreSQL treats READ
    // UNCOMMITTED as READ COMMITTED.
    //
    // Call this at the start of each transaction-sensitive operation. The guard
    // is not meant to defend against callers changing isolation again later in
    // the same transaction.
    let isolation: String = sqlx::query_scalar("SHOW transaction_isolation")
        .fetch_one(&mut **tx)
        .await
        .map_err(|error| {
            Error::from_query_sqlx_with_context("inspect transaction isolation", error)
        })?;

    let normalized = isolation.to_ascii_lowercase();
    if matches!(normalized.as_str(), "read committed" | "read uncommitted") {
        return Ok(());
    }

    Err(Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        code,
        client_message,
        format!("{operation} requires READ COMMITTED isolation; got {isolation}"),
    )))
}
