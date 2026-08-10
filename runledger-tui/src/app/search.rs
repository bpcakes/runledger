use runledger_postgres::jobs::{
    JobDefinitionRecord, JobEventRecord, JobLogRecord, JobMetricsRecord, JobQueueRecord,
    WorkflowRunDbRecord, WorkflowStepDbRecord,
};

use crate::data::DashboardData;
use crate::format::{job_status_label, workflow_run_status_label, workflow_step_status_label};

use super::App;

impl App {
    pub fn table_search_query(&self) -> Option<&str> {
        self.table_search
            .as_deref()
            .filter(|query| !query.is_empty())
    }

    pub fn matches_table_search<I, S, F>(&self, fields: F) -> bool
    where
        F: FnOnce() -> I,
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let Some(query) = self.table_search_query() else {
            return true;
        };
        let query = query.to_ascii_lowercase();
        fields()
            .into_iter()
            .any(|field| field.as_ref().to_ascii_lowercase().contains(&query))
    }

    pub(crate) fn visible_dashboard_metrics<'a>(
        &'a self,
        data: &'a DashboardData,
    ) -> impl Iterator<Item = &'a JobMetricsRecord> + 'a {
        data.metrics
            .iter()
            .filter(move |metric| self.matches_table_search(|| data.row_for(metric).into_fields()))
    }

    pub(crate) fn visible_jobs<'a>(
        &'a self,
        jobs: &'a [JobQueueRecord],
    ) -> impl Iterator<Item = &'a JobQueueRecord> + 'a {
        jobs.iter().filter(move |job| {
            self.matches_table_search(|| {
                vec![
                    job.id.to_string(),
                    job.job_type.as_str().to_owned(),
                    job_status_label(job.status).to_owned(),
                    job.stage.as_db_value().to_owned(),
                    job.worker_id.as_deref().unwrap_or("").to_owned(),
                ]
            })
        })
    }

    pub(crate) fn visible_job_events<'a>(
        &'a self,
        events: &'a [JobEventRecord],
    ) -> impl Iterator<Item = &'a JobEventRecord> + 'a {
        events.iter().filter(move |event| {
            self.matches_table_search(|| {
                vec![
                    event.id.to_string(),
                    event.event_type.as_db_value().to_owned(),
                    event
                        .stage
                        .map(|stage| stage.as_db_value())
                        .unwrap_or("")
                        .to_owned(),
                    event.payload.to_string(),
                ]
            })
        })
    }

    pub(crate) fn visible_job_logs<'a>(
        &'a self,
        logs: &'a [JobLogRecord],
    ) -> impl Iterator<Item = &'a JobLogRecord> + 'a {
        logs.iter().filter(move |log| {
            self.matches_table_search(|| {
                vec![log.id.to_string(), log.level.clone(), log.message.clone()]
            })
        })
    }

    pub(crate) fn visible_workflow_runs<'a>(
        &'a self,
        runs: &'a [WorkflowRunDbRecord],
    ) -> impl Iterator<Item = &'a WorkflowRunDbRecord> + 'a {
        runs.iter().filter(move |run| {
            self.matches_table_search(|| {
                vec![
                    run.id.to_string(),
                    run.workflow_type.as_str().to_owned(),
                    workflow_run_status_label(run.status).to_owned(),
                ]
            })
        })
    }

    pub(crate) fn visible_workflow_steps<'a>(
        &'a self,
        steps: &'a [WorkflowStepDbRecord],
    ) -> impl Iterator<Item = &'a WorkflowStepDbRecord> + 'a {
        steps.iter().filter(move |step| {
            self.matches_table_search(|| {
                vec![
                    step.step_key.as_str().to_owned(),
                    workflow_step_status_label(step.status).to_owned(),
                    step.job_type
                        .as_ref()
                        .map(|job_type| job_type.as_str().to_owned())
                        .unwrap_or_default(),
                    step.job_id
                        .map(|job_id| job_id.to_string())
                        .unwrap_or_default(),
                ]
            })
        })
    }

    pub(crate) fn visible_definitions<'a>(
        &'a self,
        definitions: &'a [JobDefinitionRecord],
    ) -> impl Iterator<Item = &'a JobDefinitionRecord> + 'a {
        definitions.iter().filter(move |definition| {
            self.matches_table_search(|| {
                vec![
                    definition.job_type.as_str().to_owned(),
                    definition.version.to_string(),
                    if definition.is_enabled {
                        "enabled"
                    } else {
                        "disabled"
                    }
                    .to_owned(),
                ]
            })
        })
    }
}
