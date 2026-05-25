use runledger_core::jobs::JobType;
use runledger_postgres::jobs::{JobDefinitionUpsert, upsert_job_definition_tx};
use sqlx::PgPool;

pub async fn register_job_definition(pool: &PgPool, job_type: JobType<'static>) {
    let mut tx = pool.begin().await.expect("begin setup tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type,
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 60,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit setup tx");
}
