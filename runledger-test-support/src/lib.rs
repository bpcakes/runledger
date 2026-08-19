//! PostgreSQL integration-test support for Runledger on Unix-like systems.
//!
//! Runledger does not support Windows. The container lifecycle implementation
//! relies on Unix process groups and shell semantics.

mod container_lifecycle;
mod db_lifecycle;
mod env;
mod postgres_container;

pub use db_lifecycle::{
    EphemeralDatabase, TestDbConnectionBudgetPermit, acquire_test_db_connection_budget,
    create_ephemeral_database, drop_database, setup_ephemeral_pool,
    setup_ephemeral_pool_with_untracked_migrations, setup_unmigrated_ephemeral_pool,
    teardown_ephemeral_pool,
};
pub use env::ScopedEnv;
