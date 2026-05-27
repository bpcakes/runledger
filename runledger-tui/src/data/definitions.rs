use runledger_postgres::DbPool;
use runledger_postgres::jobs::{
    JobDefinitionListFilter, JobDefinitionRecord, list_job_definitions,
};

#[derive(Debug, Clone)]
pub struct DefinitionsData {
    pub definitions: Vec<JobDefinitionRecord>,
}

pub async fn fetch(
    pool: &DbPool,
    job_type: Option<&str>,
    limit: i64,
) -> runledger_postgres::Result<DefinitionsData> {
    let filter = JobDefinitionListFilter {
        job_type,
        limit,
        offset: 0,
    };
    let definitions = list_job_definitions(pool, &filter).await?;
    Ok(DefinitionsData { definitions })
}
