use crate::{DbTx, Error, Result};

use super::super::transaction_settings::{cap_local_lock_timeout_tx, set_local_lock_timeout_tx};

const SCHEDULE_EXACT_SYNC_LOCK_TIMEOUT: &str = "5s";
const SCHEDULE_EXACT_SYNC_LOCK_TIMEOUT_MS: i64 = 5_000;
const SCHEDULE_EXACT_SYNC_STATEMENT_TIMEOUT: &str = "30s";
const SCHEDULE_EXACT_SYNC_STATEMENT_TIMEOUT_MS: i64 = 30_000;
const SCHEDULE_DUE_CLAIM_LOCK_TIMEOUT: &str = "1s";
const SCHEDULE_DUE_CLAIM_LOCK_TIMEOUT_MS: i64 = 1_000;

/// Applies transaction-local bounds and locks `job_schedules` for exact
/// schedule sync.
///
/// Call this before catalog exact schedule upserts and absent-schedule
/// deactivation. The lock serializes overlapping exact syncs for the same table
/// and prevents additive schedule writes from interleaving with the exact sync
/// window. Scheduler claims acquire their table-level write lock before row
/// locks, so due-schedule claims and fire-cursor updates can also wait behind
/// this lock instead of deadlocking with exact sync. It caps `lock_timeout` at
/// 5 seconds and `statement_timeout` at 30
/// seconds for the current transaction, preserving stricter caller settings.
/// The `lock_timeout` cap is restored after the table lock is acquired; the
/// `statement_timeout` cap intentionally remains active until the transaction
/// ends so the whole exact-sync critical section stays bounded. Call this in a
/// short-lived transaction dedicated to exact schedule sync.
///
/// # Errors
/// Returns an error if PostgreSQL rejects the timeout update or table lock.
pub async fn prepare_schedule_exact_sync_critical_section_tx(tx: &mut DbTx<'_>) -> Result<()> {
    cap_local_statement_timeout_tx(
        tx,
        SCHEDULE_EXACT_SYNC_STATEMENT_TIMEOUT,
        SCHEDULE_EXACT_SYNC_STATEMENT_TIMEOUT_MS,
        "set exact schedule sync statement timeout",
    )
    .await?;
    lock_job_schedules_for_schedule_exact_sync_tx(tx).await
}

async fn lock_job_schedules_for_schedule_exact_sync_tx(tx: &mut DbTx<'_>) -> Result<()> {
    let previous_lock_timeout = cap_local_lock_timeout_tx(
        tx,
        SCHEDULE_EXACT_SYNC_LOCK_TIMEOUT,
        SCHEDULE_EXACT_SYNC_LOCK_TIMEOUT_MS,
        "set exact schedule sync lock timeout",
    )
    .await?;

    let lock_result = sqlx::query("LOCK TABLE job_schedules IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut **tx)
        .await;

    match lock_result {
        Ok(_) => {
            // After the table lock is held, restore the caller's lock timeout so
            // only lock acquisition gets the exact-sync cap. The transaction's
            // statement timeout still bounds the following sync statements.
            set_local_lock_timeout_tx(
                tx,
                &previous_lock_timeout,
                "restore exact schedule sync lock timeout",
            )
            .await
        }
        Err(error) => {
            // No restore is needed on the error path: the caller rolls back the
            // transaction and PostgreSQL discards the SET LOCAL lock_timeout.
            Err(Error::from_query_sqlx_with_context(
                "lock job schedules for exact schedule sync",
                error,
            ))
        }
    }
}

pub(super) async fn lock_job_schedules_for_due_schedule_claim_tx(tx: &mut DbTx<'_>) -> Result<()> {
    // The runtime scheduler cannot observe shutdown while this table lock is
    // pending, so cap only lock acquisition and restore the caller's setting
    // after the lock is held.
    let previous_lock_timeout = cap_local_lock_timeout_tx(
        tx,
        SCHEDULE_DUE_CLAIM_LOCK_TIMEOUT,
        SCHEDULE_DUE_CLAIM_LOCK_TIMEOUT_MS,
        "set due schedule claim lock timeout",
    )
    .await?;

    let lock_result = sqlx::query("LOCK TABLE job_schedules IN ROW EXCLUSIVE MODE")
        .execute(&mut **tx)
        .await;

    match lock_result {
        Ok(_) => {
            set_local_lock_timeout_tx(
                tx,
                &previous_lock_timeout,
                "restore due schedule claim lock timeout",
            )
            .await
        }
        Err(error) => Err(Error::from_query_sqlx_with_context(
            "lock job schedules before claiming due schedules",
            error,
        )),
    }
}

async fn cap_local_statement_timeout_tx(
    tx: &mut DbTx<'_>,
    statement_timeout: &str,
    statement_timeout_ms: i64,
    context: &'static str,
) -> Result<()> {
    super::super::transaction_settings::cap_local_statement_timeout_tx(
        tx,
        statement_timeout,
        statement_timeout_ms,
        context,
    )
    .await?;

    Ok(())
}
