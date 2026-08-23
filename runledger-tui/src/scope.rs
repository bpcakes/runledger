use runledger_postgres::jobs::WorkflowRunReadScope;
use uuid::Uuid;

use crate::format;

/// TUI query scope: `None` means all organizations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scope {
    pub organization_id: Option<Uuid>,
}

impl Scope {
    #[must_use]
    pub const fn global() -> Self {
        Self {
            organization_id: None,
        }
    }

    #[must_use]
    pub const fn for_org(organization_id: Uuid) -> Self {
        Self {
            organization_id: Some(organization_id),
        }
    }

    #[must_use]
    pub const fn workflow_read_scope(self) -> WorkflowRunReadScope {
        match self.organization_id {
            Some(organization_id) => WorkflowRunReadScope::Organization(organization_id),
            None => WorkflowRunReadScope::Admin,
        }
    }

    #[must_use]
    pub fn label(&self) -> String {
        match self.organization_id {
            None => "global".to_owned(),
            Some(id) => format::short_uuid(id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_read_scope_preserves_all_organization_tui_visibility() {
        let organization_id = Uuid::from_u128(22_001);

        assert_eq!(
            Scope::global().workflow_read_scope(),
            WorkflowRunReadScope::Admin,
            "the TUI's historical global view includes every organization"
        );
        assert_eq!(
            Scope::for_org(organization_id).workflow_read_scope(),
            WorkflowRunReadScope::Organization(organization_id)
        );
    }
}
