use crate::{DbTx, Result};

use super::super::super::errors::lease_owner_mismatch_error;

pub(super) const HEARTBEAT_LEASE_MISMATCH_CONTEXT: &str =
    "heartbeat job transaction lease mismatch";
pub(super) const UPDATE_PROGRESS_LEASE_MISMATCH_CONTEXT: &str =
    "update job progress transaction lease mismatch";
pub(super) const COMPLETE_SUCCESS_LEASE_MISMATCH_CONTEXT: &str =
    "complete job success transaction lease mismatch";
pub(super) const COMPLETE_FAILURE_LEASE_MISMATCH_CONTEXT: &str =
    "complete job failure transaction missing leased row";
pub(super) const INSERT_FAILED_EVENT_TERMINAL_CONTEXT: &str = "insert failed event terminal";
pub(super) const INSERT_FAILED_EVENT_RETRY_CONTEXT: &str = "insert failed event retry";

pub(super) async fn rollback_and_return_lease_mismatch(
    tx: DbTx<'_>,
    context: &'static str,
) -> Result<()> {
    if let Err(error) = tx.rollback().await {
        tracing::warn!(
            error = %error,
            lease_mismatch_context = context,
            "failed to rollback transaction due to lease mismatch"
        );
    }
    Err(lease_owner_mismatch_error())
}
