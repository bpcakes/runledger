use runledger_postgres::DbPool;
use runledger_postgres::jobs::{
    WorkflowRunDbRecord, WorkflowRunListFilter, WorkflowStepDbRecord,
    WorkflowStepDependencyDbRecord, count_workflow_step_dependencies, count_workflow_steps,
    get_workflow_run_by_id, list_workflow_runs, list_workflow_step_dependencies_page,
    list_workflow_steps_page,
};
use uuid::Uuid;

use super::pg_error;
use crate::scope::Scope;

#[derive(Debug, Clone)]
pub struct WorkflowsData {
    pub runs: Vec<WorkflowRunDbRecord>,
}

#[derive(Debug, Clone)]
pub struct WorkflowDetailData {
    pub run: WorkflowRunDbRecord,
    pub steps: Vec<WorkflowStepDbRecord>,
    pub dependencies: Vec<WorkflowStepDependencyDbRecord>,
    pub steps_total: i64,
    pub dependencies_total: i64,
}

impl WorkflowDetailData {
    pub fn steps_truncated(&self) -> bool {
        self.steps.len() < self.steps_total as usize
    }

    pub fn dependencies_truncated(&self) -> bool {
        self.dependencies.len() < self.dependencies_total as usize
    }
}

pub async fn fetch_runs(
    pool: &DbPool,
    scope: Scope,
    workflow_type: Option<&str>,
    limit: i64,
) -> runledger_postgres::Result<WorkflowsData> {
    let filter = WorkflowRunListFilter {
        organization_id: scope.organization_id,
        status: None,
        workflow_type,
        limit,
        offset: 0,
    };
    let runs = list_workflow_runs(pool, &filter).await?;
    Ok(WorkflowsData { runs })
}

pub async fn fetch_detail(
    pool: &DbPool,
    scope: Scope,
    run_id: Uuid,
    limit: i64,
) -> Result<WorkflowDetailData, String> {
    let run = get_workflow_run_by_id(pool, scope.organization_id, run_id)
        .await
        .map_err(pg_error)?
        .ok_or_else(|| "Workflow run not found.".to_owned())?;
    let steps = list_workflow_steps_page(pool, scope.organization_id, run_id, limit, 0)
        .await
        .map_err(pg_error)?;
    let dependencies =
        list_workflow_step_dependencies_page(pool, scope.organization_id, run_id, limit, 0)
            .await
            .map_err(pg_error)?;
    let steps_total = count_workflow_steps(pool, scope.organization_id, run_id)
        .await
        .map_err(pg_error)?;
    let dependencies_total = count_workflow_step_dependencies(pool, scope.organization_id, run_id)
        .await
        .map_err(pg_error)?;
    Ok(WorkflowDetailData {
        run,
        steps,
        dependencies,
        steps_total,
        dependencies_total,
    })
}
