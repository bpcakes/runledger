use crate::{DbTx, Error, Result};

/// Caps the transaction-local PostgreSQL `lock_timeout` while preserving any
/// stricter caller setting and returns the exact prior value for restoration.
pub(in crate::jobs) async fn cap_local_lock_timeout_tx(
    tx: &mut DbTx<'_>,
    lock_timeout: &str,
    lock_timeout_ms: i64,
    context: &'static str,
) -> Result<String> {
    // Preserve PostgreSQL's own GUC text so restore keeps units and special
    // values such as "0" exactly as the connection reported them.
    // MATERIALIZED is load-bearing: it forces current_setting to be captured
    // before set_config mutates the transaction-local value. pg_settings.setting
    // stores lock_timeout in its base unit of milliseconds.
    sqlx::query_scalar::<_, String>(
        "WITH previous AS MATERIALIZED (
             SELECT
                current_setting('lock_timeout') AS lock_timeout,
                setting::bigint AS lock_timeout_ms
             FROM pg_settings
             WHERE name = 'lock_timeout'
         )
         SELECT previous.lock_timeout
         FROM previous,
              LATERAL (
                SELECT set_config(
                    'lock_timeout',
                    CASE
                        WHEN previous.lock_timeout_ms = 0 THEN $1
                        WHEN previous.lock_timeout_ms <= $2 THEN previous.lock_timeout
                        ELSE $1
                    END,
                    true
                )
              ) AS applied",
    )
    .bind(lock_timeout)
    .bind(lock_timeout_ms)
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
    sqlx::query_scalar::<_, String>("SELECT set_config('lock_timeout', $1, true)")
        .bind(lock_timeout)
        .fetch_one(&mut **tx)
        .await
        .map_err(|error| Error::from_query_sqlx_with_context(context, error))?;

    Ok(())
}
