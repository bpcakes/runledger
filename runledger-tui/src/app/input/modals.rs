use crossterm::event::{KeyCode, KeyEvent};
use uuid::Uuid;

use crate::scope::Scope;

use super::super::{ActiveInput, App, FilterTarget};

impl App {
    pub(in crate::app::input) fn handle_active_input_key(&mut self, key: KeyEvent) -> bool {
        match key.code {
            KeyCode::Esc => {
                self.active_input = ActiveInput::None;
                false
            }
            KeyCode::Enter => self.commit_active_input(),
            KeyCode::Backspace => {
                if let Some(text) = self.active_input.text_mut() {
                    text.pop();
                }
                false
            }
            KeyCode::Char(character) => {
                if let Some(text) = self.active_input.text_mut() {
                    text.push(character);
                }
                false
            }
            _ => false,
        }
    }

    fn commit_active_input(&mut self) -> bool {
        match std::mem::replace(&mut self.active_input, ActiveInput::None) {
            ActiveInput::None => false,
            ActiveInput::Organization { text } => self.commit_organization_input(&text),
            ActiveInput::Filter { target, text } => self.commit_filter_input(target, &text),
            ActiveInput::Search { text } => self.commit_search_input(&text),
            ActiveInput::Command { text } => self.execute_command(text.trim()),
        }
    }

    fn commit_organization_input(&mut self, text: &str) -> bool {
        let trimmed = text.trim();
        self.scope = if trimmed.is_empty() {
            Scope::global()
        } else {
            match Uuid::parse_str(trimmed) {
                Ok(id) => Scope::for_org(id),
                Err(_) => {
                    self.last_error = Some("Invalid organization UUID".to_owned());
                    return false;
                }
            }
        };
        self.invalidate_cache();
        true
    }

    fn commit_filter_input(&mut self, target: FilterTarget, text: &str) -> bool {
        let trimmed = text.trim();
        let value = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        };
        match target {
            FilterTarget::Workflow => {
                self.workflow_type_filter = value;
                self.workflows = None;
            }
            FilterTarget::Job => {
                self.job_type_filter = value;
                self.jobs = None;
                self.definitions = None;
            }
        }
        true
    }

    fn commit_search_input(&mut self, text: &str) -> bool {
        let trimmed = text.trim();
        self.table_search = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        };
        self.list_selection = 0;
        self.clamp_selection();
        false
    }
}
