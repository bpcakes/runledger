use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

/// The data scope the host has authorized for one request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminScope {
    /// Global jobs and every organization's jobs.
    All,
    /// Only rows owned by this organization.
    Organization(Uuid),
}

impl AdminScope {
    pub(crate) const fn organization_id(self) -> Option<Uuid> {
        match self {
            Self::All => None,
            Self::Organization(organization_id) => Some(organization_id),
        }
    }
}

/// Whether sensitive stored data may be returned to this request.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DataVisibility {
    /// Return operational metadata without selecting payloads or diagnostic text.
    #[default]
    MetadataOnly,
    /// Return stored JSON, idempotency values, worker identifiers, and messages.
    Full,
}

/// Authorization decision supplied by the embedding application.
///
/// Construct this value only after the host has authenticated and authorized
/// the caller, then add it as an Axum request extension.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminAccess {
    scope: AdminScope,
    visibility: DataVisibility,
    service_wide_definitions: bool,
}

impl AdminAccess {
    /// Grants service-wide read access.
    #[must_use]
    pub const fn all(visibility: DataVisibility) -> Self {
        Self {
            scope: AdminScope::All,
            visibility,
            service_wide_definitions: true,
        }
    }

    /// Grants read access to exactly one organization.
    #[must_use]
    pub const fn organization(organization_id: Uuid, visibility: DataVisibility) -> Self {
        Self {
            scope: AdminScope::Organization(organization_id),
            visibility,
            service_wide_definitions: false,
        }
    }

    /// Grants access to the service-wide job-definition catalog.
    ///
    /// Organization access does not include this resource by default because
    /// definitions have no organization ownership. Service-wide access already
    /// includes it.
    #[must_use]
    pub const fn with_service_wide_definitions(mut self) -> Self {
        self.service_wide_definitions = true;
        self
    }

    /// Returns the authorized scope.
    #[must_use]
    pub const fn scope(self) -> AdminScope {
        self.scope
    }

    /// Returns the authorized sensitive-data visibility.
    #[must_use]
    pub const fn visibility(self) -> DataVisibility {
        self.visibility
    }

    /// Returns whether the service-wide job-definition catalog may be read.
    #[must_use]
    pub const fn can_read_service_wide_definitions(self) -> bool {
        self.service_wide_definitions
    }
}
