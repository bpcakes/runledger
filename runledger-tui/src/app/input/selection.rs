use uuid::Uuid;

use crate::data::QueueStatusFilter;

use super::super::{App, JobDetailPane, Screen, TopScreen};
use super::clipboard::copy_to_terminal_clipboard;

impl App {
    pub(in crate::app) fn move_selection(&mut self, delta: i32) {
        if matches!(
            (&self.screen, self.job_detail_pane),
            (Screen::JobDetail { .. }, JobDetailPane::Payload)
        ) {
            let max_scroll = self.payload_scroll_max();
            if max_scroll == 0 {
                return;
            }
            let next = if delta.is_positive() {
                self.detail_scroll.saturating_add(1)
            } else {
                self.detail_scroll.saturating_sub(1)
            };
            self.detail_scroll = next.min(max_scroll);
            return;
        }

        let len = self.list_len();
        if len == 0 {
            return;
        }
        let step = delta.unsigned_abs() as usize;
        let next = if delta.is_positive() {
            self.list_selection.saturating_add(step)
        } else {
            self.list_selection.saturating_sub(step)
        };
        self.list_selection = next.min(len - 1);
    }

    pub(in crate::app::input) fn move_to_start(&mut self) {
        if matches!(
            (&self.screen, self.job_detail_pane),
            (Screen::JobDetail { .. }, JobDetailPane::Payload)
        ) {
            self.detail_scroll = 0;
        } else {
            self.list_selection = 0;
        }
    }

    pub(in crate::app::input) fn move_to_end(&mut self) {
        if matches!(
            (&self.screen, self.job_detail_pane),
            (Screen::JobDetail { .. }, JobDetailPane::Payload)
        ) {
            self.detail_scroll = self.payload_scroll_max();
            return;
        }
        let len = self.list_len();
        if len > 0 {
            self.list_selection = len - 1;
        }
    }

    pub(in crate::app) fn payload_scroll_max(&self) -> usize {
        let Some(detail) = &self.job_detail else {
            return 0;
        };
        let lines = if self.payload_raw {
            crate::format::job_payload_raw_lines(&detail.job.payload)
        } else {
            crate::format::job_payload_lines(&detail.job.payload)
        };
        crate::format::job_payload_scroll_max(lines.len(), self.payload_visible_rows)
    }

    pub(in crate::app::input) fn activate_selection(&mut self) {
        match &self.screen {
            Screen::Dashboard => {
                if let Some(job_type) = self.selected_dashboard_job_type() {
                    self.job_type_filter = Some(job_type);
                    self.queue_filter = QueueStatusFilter::All;
                    self.navigate_top(TopScreen::Queue);
                }
            }
            Screen::Queue => {
                if let Some(job_id) = self.selected_job_id() {
                    self.push_job_detail(job_id);
                }
            }
            Screen::Workflows => {
                if let Some(run_id) = self.selected_workflow_run_id() {
                    self.push_workflow_detail(run_id);
                }
            }
            Screen::WorkflowDetail { .. } => {
                if let Some(job_id) = self.selected_workflow_step_job_id() {
                    self.push_job_detail(job_id);
                }
            }
            _ => {}
        }
    }

    fn selected_dashboard_job_type(&self) -> Option<String> {
        let dashboard = self.dashboard.as_ref()?;
        self.visible_dashboard_metrics(dashboard)
            .nth(self.list_selection)
            .map(|metric| metric.job_type.as_str().to_owned())
    }

    fn selected_job_id(&self) -> Option<Uuid> {
        let jobs = &self.jobs.as_ref()?.jobs;
        self.visible_jobs(jobs)
            .nth(self.list_selection)
            .map(|job| job.id)
    }

    fn selected_workflow_run_id(&self) -> Option<Uuid> {
        let runs = &self.workflows.as_ref()?.runs;
        self.visible_workflow_runs(runs)
            .nth(self.list_selection)
            .map(|run| run.id)
    }

    pub(crate) fn selected_workflow_step_job_id(&self) -> Option<Uuid> {
        let steps = &self.workflow_detail.as_ref()?.steps;
        self.visible_workflow_steps(steps)
            .nth(self.list_selection)
            .and_then(|step| step.job_id)
    }

    fn selected_identifier(&self) -> Option<String> {
        match self.screen {
            Screen::Dashboard => self.selected_dashboard_job_type(),
            Screen::Queue => self.selected_job_id().map(|id| id.to_string()),
            Screen::JobDetail { job_id } => Some(job_id.to_string()),
            Screen::Workflows => self.selected_workflow_run_id().map(|id| id.to_string()),
            Screen::WorkflowDetail { run_id } => self
                .selected_workflow_step_job_id()
                .map(|id| id.to_string())
                .or_else(|| Some(run_id.to_string())),
            Screen::Definitions => self
                .visible_definitions(&self.definitions.as_ref()?.definitions)
                .nth(self.list_selection)
                .map(|definition| definition.job_type.as_str().to_owned()),
        }
    }

    pub(in crate::app::input) fn copy_selected_identifier(&mut self) {
        let Some(value) = self.selected_identifier() else {
            self.notice = Some("Nothing selected to copy".to_owned());
            return;
        };
        match copy_to_terminal_clipboard(&value) {
            Ok(()) => self.notice = Some(format!("Copied {value}")),
            Err(error) => self.notice = Some(format!("Copy failed: {error}")),
        }
    }
}
