//! Compile-only consumer exercise. No binary or tests execute this function.
use std::time::Duration;

use runledger_core::jobs::JobType;
use runledger_postgres::{
    AcceptedIntentOutcome, PgAtomicError, PgAtomicUncertainty, PgProfileError, PgScopeError,
    PgScopeLoss, PgSessionProfile, PgTransactionError, RequiredIntentError, RunledgerDatabase,
    jobs::{JobEnqueue, JobEnqueueIntent, JobEnqueueOutcome},
    run_atomic,
};
use serde_json::Value;
use sqlx::{postgres::PgConnectOptions, types::Uuid};

/// Values produced inside the callback are evidence only until commit is acknowledged.
pub struct ProvisionalOutput {
    pub intent: AcceptedIntentOutcome,
    pub queued: JobEnqueueOutcome,
}

/// Constructed only in the acknowledged-success match arm below.
pub struct CommittedReceipt {
    pub intent_id: Uuid,
    pub job_id: Uuid,
}

pub enum WorkError {
    Application(PgScopeError<sqlx::Error>),
    RequiredIntent(PgScopeError<RequiredIntentError>),
    Queue(PgScopeError<runledger_postgres::Error>),
}

/// Intentionally no Debug/Display derived over retained application/error data.
pub enum SubmitError {
    Profile(PgProfileError),
    Connect(sqlx::Error),
    Begin(PgTransactionError),
    RejectedAndRolledBack(WorkError),
    CommitUnconfirmed {
        provisional: ProvisionalOutput,
        cause: PgTransactionError,
    },
    RollbackUnconfirmed {
        rejection: WorkError,
        cause: PgTransactionError,
    },
    ScopeLost {
        provisional: Result<ProvisionalOutput, WorkError>,
        cause: PgScopeLoss,
    },
}

/// Requires PostgreSQL 18, preprovisioned roles/schema, current Runledger migrations,
/// application_jobs.submissions(request_key text PRIMARY KEY, payload jsonb), and an
/// enabled example.process definition. example.followup may be registered later.
/// The caller must derive request_key and payload from its trusted application boundary.
/// No error authorizes automatic replay. Cancellation produces no receipt and must
/// also be reconciled by request_key; a pending intent is no promotion guarantee.
pub async fn submit(
    options: PgConnectOptions,
    request_key: &str,
    payload: &Value,
) -> Result<CommittedReceipt, SubmitError> {
    // Declared login authority, SET ROLE application_writer, one custom schema,
    // statement_timeout = 30 seconds, lock_timeout = 5 seconds.
    let profile = PgSessionProfile::new(
        options.get_username(),
        "application_writer",
        vec!["application_jobs".into()],
        Duration::from_secs(30),
        Duration::from_secs(5),
    )
    .map_err(SubmitError::Profile)?;
    let database = RunledgerDatabase::connect(options, profile, 2)
        .await
        .map_err(SubmitError::Connect)?;

    let intent = JobEnqueueIntent::new(JobType::new("example.followup"), payload, request_key);
    let enqueue = JobEnqueue {
        job_type: JobType::new("example.process"),
        organization_id: None,
        payload,
        priority: None,
        max_attempts: None,
        timeout_seconds: None,
        next_run_at: None,
        idempotency_key: Some(request_key),
        stage: None,
    };

    let result = run_atomic(&database, async |mut scope| {
        scope
            .application(async |sql| {
                sqlx::query(
                    "INSERT INTO application_jobs.submissions (request_key, payload) VALUES ($1, $2)",
                )
                .bind(request_key)
                .bind(payload)
                .execute(sql.executor())
                .await?;
                Ok::<(), sqlx::Error>(())
            })
            .await
            .map_err(WorkError::Application)?;
        let intent = scope
            .record_required_job_enqueue_intent(&intent)
            .await
            .map_err(WorkError::RequiredIntent)?;
        // Consuming this scope makes subsequent intent recording unavailable.
        let mut queue = scope.queue();
        let queued = queue.enqueue_job(&enqueue).await.map_err(WorkError::Queue)?;
        Ok::<_, WorkError>(ProvisionalOutput { intent, queued })
    })
    .await;

    // All atomic sessions have completed or been retired; this local pool has no
    // worker consumers. Keep cleanup explicit on every returned disposition.
    database.pool().close().await;
    match result {
        Ok(output) => Ok(CommittedReceipt {
            intent_id: output.intent.intent_id(),
            job_id: output.queued.job_id,
        }),
        Err(PgAtomicError::Begin(cause)) => Err(SubmitError::Begin(cause)),
        Err(PgAtomicError::Rejected(rejection)) => {
            Err(SubmitError::RejectedAndRolledBack(rejection))
        }
        Err(PgAtomicError::Uncertain(uncertain)) => match uncertain {
            PgAtomicUncertainty::CommitUnconfirmed { output, cause } => {
                Err(SubmitError::CommitUnconfirmed {
                    provisional: output,
                    cause,
                })
            }
            PgAtomicUncertainty::RollbackUnconfirmed { rejection, cause } => {
                Err(SubmitError::RollbackUnconfirmed { rejection, cause })
            }
            PgAtomicUncertainty::ScopeLost { result, cause } => Err(SubmitError::ScopeLost {
                provisional: result,
                cause,
            }),
        },
    }
}
