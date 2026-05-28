use crate::data::QueueStatusFilter;
use crate::scope::Scope;

use super::super::{App, Screen, TopScreen, format_duration_short};

impl App {
    pub(in crate::app::input) fn clear_context_filters(&mut self) -> bool {
        let mut refresh = false;
        self.table_search = None;
        match self.screen {
            Screen::Queue | Screen::JobDetail { .. } => {
                if self.queue_filter != QueueStatusFilter::All || self.job_type_filter.is_some() {
                    self.queue_filter = QueueStatusFilter::All;
                    self.job_type_filter = None;
                    self.jobs = None;
                    self.definitions = None;
                    self.bump_fetch_generation();
                    refresh = true;
                }
            }
            Screen::Workflows | Screen::WorkflowDetail { .. } => {
                if self.workflow_type_filter.is_some() {
                    self.workflow_type_filter = None;
                    self.workflows = None;
                    self.bump_fetch_generation();
                    refresh = true;
                }
            }
            Screen::Definitions => {
                if self.job_type_filter.is_some() {
                    self.job_type_filter = None;
                    self.definitions = None;
                    self.bump_fetch_generation();
                    refresh = true;
                }
            }
            Screen::Dashboard => {}
        }
        self.list_selection = 0;
        self.notice = Some("Cleared filters".to_owned());
        refresh
    }

    pub(in crate::app::input) fn execute_command(&mut self, command: &str) -> bool {
        let parts: Vec<&str> = command.split_whitespace().collect();
        match parts.as_slice() {
            [] => false,
            ["scope", "global"] => {
                self.scope = Scope::global();
                self.invalidate_cache();
                self.notice = Some("Scope set to global".to_owned());
                true
            }
            ["filter", "status", status] => {
                let Some(filter) = QueueStatusFilter::from_command(status) else {
                    self.notice = Some(format!("Unknown status filter: {status}"));
                    return false;
                };
                self.queue_filter = filter;
                self.jobs = None;
                self.bump_fetch_generation();
                self.navigate_top(TopScreen::Queue);
                true
            }
            ["refresh", value] => {
                if let Some(ms) = parse_refresh_ms(value) {
                    self.config.refresh_ms = ms;
                    self.notice = Some(format!(
                        "Refresh interval set to {}",
                        format_duration_short(self.config.refresh_interval())
                    ));
                } else {
                    self.notice = Some(format!("Invalid refresh interval: {value}"));
                }
                false
            }
            ["copy", "id"] => {
                self.copy_selected_identifier();
                false
            }
            _ => {
                self.notice = Some(format!("Unknown command: {command}"));
                false
            }
        }
    }
}

fn parse_refresh_ms(value: &str) -> Option<u64> {
    if let Some(raw) = value.strip_suffix("ms") {
        return raw.parse().ok();
    }
    if let Some(raw) = value.strip_suffix('s') {
        return raw.parse::<u64>().ok().map(|seconds| seconds * 1000);
    }
    value.parse().ok()
}
