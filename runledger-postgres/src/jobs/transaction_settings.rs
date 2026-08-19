use crate::{DbTx, Error, Result};

/// Caps the transaction-local PostgreSQL `lock_timeout` while preserving any
/// stricter caller setting and returns the exact prior value for restoration.
pub(in crate::jobs) async fn cap_local_lock_timeout_tx(
    tx: &mut DbTx<'_>,
    lock_timeout: &str,
    lock_timeout_ms: i64,
    context: &'static str,
) -> Result<String> {
    cap_local_timeout_tx(tx, "lock_timeout", lock_timeout, lock_timeout_ms, context).await
}

/// Caps the transaction-local PostgreSQL `statement_timeout` while preserving
/// any stricter caller setting and returns the exact prior value for restoration.
pub(in crate::jobs) async fn cap_local_statement_timeout_tx(
    tx: &mut DbTx<'_>,
    statement_timeout: &str,
    statement_timeout_ms: i64,
    context: &'static str,
) -> Result<String> {
    cap_local_timeout_tx(
        tx,
        "statement_timeout",
        statement_timeout,
        statement_timeout_ms,
        context,
    )
    .await
}

/// Caps the transaction-local PostgreSQL `transaction_timeout` while preserving
/// any stricter caller setting and returns the exact prior value for restoration.
pub(in crate::jobs) async fn cap_local_transaction_timeout_tx(
    tx: &mut DbTx<'_>,
    transaction_timeout: &str,
    transaction_timeout_ms: i64,
    context: &'static str,
) -> Result<String> {
    cap_local_timeout_tx(
        tx,
        "transaction_timeout",
        transaction_timeout,
        transaction_timeout_ms,
        context,
    )
    .await
}

async fn cap_local_timeout_tx(
    tx: &mut DbTx<'_>,
    setting_name: &'static str,
    timeout: &str,
    timeout_ms: i64,
    context: &'static str,
) -> Result<String> {
    // Preserve PostgreSQL's own GUC text so restore keeps units and special
    // values such as "0" exactly as the connection reported them.
    // MATERIALIZED is load-bearing: it forces current_setting to be captured
    // before set_config mutates the transaction-local value. pg_settings.setting
    // stores timeout GUCs in their base unit of milliseconds.
    sqlx::query_scalar::<_, String>(
        "WITH previous AS MATERIALIZED (
             SELECT
                current_setting($1) AS timeout,
                setting::bigint AS timeout_ms
             FROM pg_settings
             WHERE name = $1
         )
         SELECT previous.timeout
         FROM previous,
              LATERAL (
                SELECT set_config(
                    $1,
                    CASE
                        WHEN previous.timeout_ms = 0 THEN $2
                        WHEN previous.timeout_ms <= $3 THEN previous.timeout
                        ELSE $2
                    END,
                    true
                )
              ) AS applied",
    )
    .bind(setting_name)
    .bind(timeout)
    .bind(timeout_ms)
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context(context, error))
}

/// Restores a transaction-local PostgreSQL `lock_timeout` value returned by
/// [`cap_local_lock_timeout_tx`].
pub(in crate::jobs) async fn set_local_lock_timeout_tx(
    tx: &mut DbTx<'_>,
    lock_timeout: &str,
    context: &'static str,
) -> Result<()> {
    set_local_timeout_tx(tx, "lock_timeout", lock_timeout, context).await
}

/// Restores a transaction-local PostgreSQL `statement_timeout` value returned
/// by [`cap_local_statement_timeout_tx`].
pub(in crate::jobs) async fn set_local_statement_timeout_tx(
    tx: &mut DbTx<'_>,
    statement_timeout: &str,
    context: &'static str,
) -> Result<()> {
    set_local_timeout_tx(tx, "statement_timeout", statement_timeout, context).await
}

async fn set_local_timeout_tx(
    tx: &mut DbTx<'_>,
    setting_name: &'static str,
    timeout: &str,
    context: &'static str,
) -> Result<()> {
    sqlx::query_scalar::<_, String>("SELECT set_config($1, $2, true)")
        .bind(setting_name)
        .bind(timeout)
        .fetch_one(&mut **tx)
        .await
        .map_err(|error| Error::from_query_sqlx_with_context(context, error))?;

    Ok(())
}
