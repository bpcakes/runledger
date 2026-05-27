use uuid::Uuid;

use crate::format;

/// Query scope: `None` means global (all organizations).
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
    pub fn label(&self) -> String {
        match self.organization_id {
            None => "global".to_owned(),
            Some(id) => format::short_uuid(id),
        }
    }
}
