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
        let scope = if trimmed.is_empty() {
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
        self.transition_scope(scope)
    }

    fn commit_filter_input(&mut self, target: FilterTarget, text: &str) -> bool {
        let trimmed = text.trim();
        let value = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        };
        match target {
            FilterTarget::Workflow => self.transition_workflow_type_filter(value),
            FilterTarget::Job => self.transition_job_type_filter(value),
        }
    }

    fn commit_search_input(&mut self, text: &str) -> bool {
        let trimmed = text.trim();
        let query = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        };
        self.transition_table_search(query)
    }
}
