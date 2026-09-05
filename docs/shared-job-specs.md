# Shared producer and worker contracts

Keep job identity, payload, and operational settings in a provider-free module
that both API and worker builds can import. `runledger-core` owns these contracts;
only the worker constructs provider clients.

```rust
use runledger_core::jobs::{JobContract, JobDefinitionSettings, JobSpec, JobType};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct DeliveryPayload {
    pub request_id: uuid::Uuid,
}

pub struct Delivery;
impl JobContract for Delivery {
    type Payload = DeliveryPayload;

    fn spec() -> JobSpec {
        JobSpec::new(JobType::new("app.delivery"))
            .expect("static job identity")
            .with_settings(JobDefinitionSettings::new().max_attempts(5))
            .expect("static job settings")
    }
}
```

A producer builds `Delivery::submit(&payload)?.idempotency_key(key)` and borrows
it with `JobEnqueue::from(&submission)` for any existing PostgreSQL enqueue API.
`enqueue_job_with_outcome(&pool, &request)` commits its own transaction and returns
`JobEnqueueDisposition::Inserted` or `Existing`. Count only `Inserted` when
reporting new work. Use `enqueue_job_with_outcome_tx` inside a caller transaction;
the pool convenience's returned status is an observation, not a retained lock.
Dynamic JSON callers can use `JobSpec::submit(value)` or the direct owned builder
`JobSubmission::new(job_type, value)`. Organization, priority, attempts, timeout,
schedule, idempotency key, and stage are fluent, explicit options.

Build `JobSpecs::new([Delivery::spec(), ...])` once to reject duplicate identities.
For producer definition setup, materialize
`specs.iter().map(JobDefinitionUpsert::from).collect::<Vec<_>>()` and use the
existing PostgreSQL synchronization operations:

- `sync_catalog_job_definitions_tx` with
  `PreserveExistingEnabledForEnabledDefinitions` preserves operator disables.
- `sync_catalog_job_definitions_exact_tx` takes an explicit owned job-type scope,
  disables absent scoped definitions, and restores the supplied enabled states.

These modes retain the existing schedule checks and transaction guarantees.
Specification-enabled checks do not bypass the database's operator-disable check.

A worker implements `TypedJobHandler`, selects `type Contract = Delivery`, and
receives `DeliveryPayload` in `execute`. Register `handler.into_job_handler()`.
`JobCatalog::from_specs(&specs, handlers)` requires exactly one handler per spec,
including disabled specs that may still have queued work. It rejects missing,
unknown, and duplicate bindings before returning a catalog. The same catalog
schedule, retry-override, supervisor, and sync APIs remain available. For staged
adoption in an existing catalog, `try_handler_for_spec(&spec, handler)` validates
the pairing and applies the shared metadata. Existing JSON handlers also bind.

The typed adapter decodes existing JSON at execution time. It does not wrap the
payload, add a version field, deny unknown fields, or upgrade rows. Choose Serde
attributes deliberately and retain decoding for every durable payload version
that can remain queued. The default malformed-payload result is terminal
`job.invalid_payload` with the static message `Job payload has an invalid shape.`
Override `malformed_payload` for application codes or sanitized diagnostics;
Serde errors can contain attacker-controlled values. Raw JSON still reaches
`on_dead_letter`, including malformed rows. `execute_with_services` can be
overridden to use the existing typed checkpoint and lease-fenced progress APIs.

Definition versions describe operational metadata, not payload schema versions.
Defaults do **not** become implicit request overrides: changing a timeout or
attempt policy must not change an identical request's stored snapshot. If an
existing producer explicitly snapshots attempts or timeout, retain those explicit
builder options during migration. Also retain original scheduled timestamps and
payload serialization; changed keyed requests still fail with
`job.idempotency_conflict`.

See the [IdentityPro and CreditKit migration pilots](shared-job-specs-migrations/README.md)
for concrete downstream conversions and reproduction commands.
