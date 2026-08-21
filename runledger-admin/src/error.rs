use axum::{Json, http::StatusCode, response::IntoResponse};
use serde::Serialize;
use utoipa::ToSchema;

/// Safe HTTP error returned by the admin contract.
#[derive(Debug)]
pub struct AdminApiError {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
}

#[derive(Serialize, ToSchema)]
#[schema(as = AdminErrorResponse)]
pub(crate) struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Serialize, ToSchema)]
struct ErrorDetail {
    code: &'static str,
    message: &'static str,
}

impl AdminApiError {
    /// HTTP status associated with this error.
    #[must_use]
    pub const fn status(&self) -> StatusCode {
        self.status
    }

    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }

    /// Safe message suitable for an administrator-facing response.
    #[must_use]
    pub const fn client_message(&self) -> &'static str {
        self.message
    }

    pub(crate) const fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "admin.unauthorized",
            message: "Admin access is required.",
        }
    }

    pub(crate) const fn invalid_query() -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "admin.invalid_query",
            message: "The request query is invalid.",
        }
    }

    pub(crate) const fn invalid_identifier() -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "admin.invalid_identifier",
            message: "The resource identifier is invalid.",
        }
    }

    pub(crate) const fn not_found() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "admin.not_found",
            message: "The requested resource was not found.",
        }
    }

    pub(crate) const fn forbidden() -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code: "admin.forbidden",
            message: "Admin access does not include this resource.",
        }
    }

    pub(crate) const fn storage() -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "admin.storage_error",
            message: "The admin data could not be loaded.",
        }
    }
}

impl std::fmt::Display for AdminApiError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for AdminApiError {}

impl IntoResponse for AdminApiError {
    fn into_response(self) -> axum::response::Response {
        (
            self.status,
            Json(ErrorBody {
                error: ErrorDetail {
                    code: self.code,
                    message: self.message,
                },
            }),
        )
            .into_response()
    }
}
