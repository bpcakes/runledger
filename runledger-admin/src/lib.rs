//! Read-only, host-authorized HTTP administration for Runledger.
//!
//! The embedding application owns authentication. After authenticating a
//! request it inserts an [`AdminAccess`] extension and mounts [`router`] at a
//! versioned path such as `/api/admin/runledger/v1`. Requests without that
//! extension fail closed. This crate intentionally exposes no mutation routes.

mod access;
mod dto;
mod error;
mod http;
mod service;

pub use access::{AdminAccess, AdminScope, DataVisibility};
pub use dto::*;
pub use error::AdminApiError;
pub use http::{openapi_json, router};
pub use service::AdminService;
/// OpenAPI schema traits implemented by the public admin DTOs.
///
/// Use this re-export instead of adding a separate `utoipa` dependency so the
/// trait version always matches the implementations provided by this crate.
pub use utoipa;

/// Stable API version represented by this crate.
pub const API_VERSION: &str = "v1";

/// Common imports for embedding the read-only admin API.
pub mod prelude {
    pub use crate::{
        AdminAccess, AdminApiError, AdminScope, AdminService, DataVisibility, openapi_json, router,
    };
}
