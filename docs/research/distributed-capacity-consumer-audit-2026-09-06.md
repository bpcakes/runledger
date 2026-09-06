# Distributed capacity: consumer fit audit

Date: 2026-09-06. Runledger baseline: `4cba487` (0.12.0).
Tracking: `runledger-zr5`. Implementation epic: `runledger-distributed-capacity-bp2`.
Plan: [distributed capacity controls](../distributed-capacity-plan.md).

## Decision

Proceed with distributed concurrency, and prove it with consumer-shaped adoption
fixtures before release. The strongest observed need is shared limits on expensive
job families across worker processes, with excess work waiting in the queue.
Per-customer limits are a useful composition capability, but this audit did not
find evidence establishing a particular customer fairness policy or target limit.

Make rolling admission rates conditional. None of the inspected integrations
establishes that exact trailing-window **job admissions** are the control it needs
to replace. The concrete rate controls operate on provider attempts, calendar-day
spend, HTTP ingress, or paced dispatch. This is a reason to defer P17, not to change
its algorithm silently into a different promise. P18–P21 remain dependent on P17.

The existing concurrency design has the right durable ownership boundary:
capacity admission belongs with the job claim, denial does not spend an attempt
or occupy a handler slot, and multiple requirements share one transaction.
The missing evidence was consumer adoption, rather than another SQL protocol review.

## Scope and source inventory

Searched Cargo manifests and Rust references under `~/Documents`, including hidden
workspaces, excluding build output, dependencies, and Git internals. Found **16
consumer checkouts representing six product families**. Five have execution
integrations; HOCR2 currently consumes migration compatibility and has a worker
scaffold. The Runledger repository and its smoke fixture are not customer demand.

These are local source and lockfile versions, not verified deployed versions.
Primary checkouts were inspected in their current working trees. In particular,
OneSales and Vatbot have uncommitted work; a source observation is not necessarily
part of their recorded HEAD. No consumer files were changed or applications run.

| Primary checkout | HEAD at inventory | Locked Runledger | Scope examined |
| --- | --- | --- | --- |
| `creditkit-platform` | `62b02b65029d7e88950b72a178882ffa757c3d71` | 0.12.0 | Document enqueue, workflow handlers, local tool/provider capacity, native synthesis phases |
| `onesales` | `b0bec49466d37e9d11a690098b5faa85b06835e8` | 0.12.0 | Buyer enrichment, execution resource, direct/queued Leadspicker provider gates |
| `identitypro` | `119146784a65419c0dbc68da2157f363b4bc15e0` | 0.12.0 | Alert hydration, worker settings, email provider boundary, authentication cleanup |
| `perdify` | `1fa17c0b3cb22a861c7f52745684d7cea7a1d86b` | 0.4.0 | Round opener jobs, image job fan-out, progress and enqueue adapters |
| `vatbot` | `cd98fd9922c6d3ba5cf756bbe02bd32576ca8b1c` | 0.4.0 | Scheduled pump, nested extraction lifecycle, paid-AI admission and saved/local result paths |
| `hocr2` | `f6c54c7ce178380d4be460f71edbb6b76cd16e30` | 0.5.0 | Migration validator, worker scaffold, approved runtime work package |

At inventory OneSales had 103 dirty paths and Vatbot 11; the other four primary
checkouts were clean. Counts describe checkout state, not audit changes.

Deduplicated: `creditkit-backup-large-git` (0.6.0); `onesales-checkout-1` and `-2`
(0.8.0); `onesales-checkout-3` through `-6` (in-tree 0.1.0); `perdify-checkout-1`
and `-2` (0.4.0); and `.ingot-workspaces/wrk_019ce65cf5e77b5284fd546e2f489de1`
(an older OneSales workspace with in-tree Runledger). Their manifests/identity
were inventoried; they were not treated as independent adoption successes or
subjected to the same detailed source review as the six primary checkouts.

This supersedes older local-consumer version assumptions in the September 5 audit.
No compilation, database experiment, provider request, load test, production
incident investigation, or claim of measured code deletion is part of this audit.

## Consumer findings

### CreditKit: strong concurrency candidate, with two distinct resource boundaries

[Document constants](/Users/aa/Documents/creditkit-platform/crates/creditkit/src/documents.rs:125)
set local extraction and synthesis capacity to two and allow a 120-second synthesis
semaphore wait. [Synthesis capacity](/Users/aa/Documents/creditkit-platform/crates/creditkit/src/documents/credit_report_synthesis/capacity.rs:8)
is an `Arc<Semaphore>` acquired before the durable paid-call claim and released
when the provider future settles. Clones share a pool within a process; these
objects do not coordinate independent processes.

This is concrete demand for an optional fleet-wide job budget: adding workers
currently adds independent local pools. Queue admission could keep excess bounded
synthesis jobs pending instead of starting handlers that wait for capacity or
return the capacity-unavailable error. This is a predicted benefit from source,
not evidence that production currently exhausts those pools.

The integration already has
[workflow submission helpers](/Users/aa/Documents/creditkit-platform/crates/creditkit/src/documents/extraction_workflow/submission.rs:62)
and [separate extraction/synthesis handlers](/Users/aa/Documents/creditkit-platform/crates/creditkit/src/jobs/documents.rs:255),
which are useful places to attach explicit provider and tenant keys. Keep the
fluent builder path usable; adopting capacity must not require hand-written SQL.
The current local limit of two is a fixture input, not an automatically approved
fleet limit.

Two exceptions prevent claiming that all semaphores disappear:

- The extraction semaphore also protects
  [synchronous upload PDF inspection](/Users/aa/Documents/creditkit-platform/crates/creditkit/src/documents/upload/authorization.rs:208).
  Host CPU/memory and interactive admission remain local responsibilities.
- Native synthesis is a
  [phase-driven continuation](/Users/aa/Documents/creditkit-platform/crates/creditkit/src/documents/credit_report_synthesis/native_text_workflow.rs:475).
  A [mapping phase](/Users/aa/Documents/creditkit-platform/crates/creditkit/src/documents/credit_report_synthesis/evidence_map_run/execution.rs:94)
  acquires a provider permit before claiming a window; other slices can make no
  provider call. One lease is not universally one paid call. Durable application
  claims, saved artifacts, repair policy, and paid-call accounting stay owned by
  CreditKit.

**Adoption target:** a bounded synthesis job or provider-bearing step, with a
documented relationship between one job lease and maximum simultaneous calls.
Exercise cached/no-work and repair paths before removing any existing gate.

### IdentityPro: the clearest small concurrency pilot, with a phase trap

[Alert hydration](/Users/aa/Documents/identitypro/crates/identitypro-jobs/src/identity_protect_alert.rs:27)
allows four concurrent hydrations per worker. Its
[handler](/Users/aa/Documents/identitypro/crates/identitypro-jobs/src/identity_protect_alert.rs:183)
tries a local permit after the job has started and returns a 60-second successful
continuation when the permit is unavailable. A shared queue policy could cap this
job family across workers and eliminate this capacity-only continuation path.
This is a change from a per-worker limit to a fleet limit; select that limit
explicitly rather than silently redefining the existing constant.

The `PersistExhaustion` checkpoint deliberately executes **before** the semaphore
check. It performs durable exhaustion bookkeeping without entering the hydration
gate. Attaching an immutable capacity requirement to the entire job would also
gate this bookkeeping continuation, particularly while a policy is paused.

**Adoption target:** preserve this distinction in a consumer-shaped fixture.
If provider and bookkeeping work need separate queued identities, demonstrate
that decomposition and its idempotent handoff. Do not remove the existing bypass
or invent a generic checkpoint-based capacity exemption to make the example pass.
Domain attempt identity, exhaustion persistence, and recovery remain in IdentityPro.

[Email sending](/Users/aa/Documents/identitypro/crates/identitypro-jobs/src/resend_provider.rs:49)
has a separate provider request boundary. It is a possible future admission-rate
experiment, but the source does not establish a requirement for exact rolling
queue counts. The
[Runlimit cleanup job](/Users/aa/Documents/identitypro/crates/identitypro-jobs/src/rate_limit_cleanup.rs:23)
cleans authentication counters; this does not make authentication limiting a
Runledger capacity use case.

### OneSales: real coordination pain, but the provider gate is a different feature

[Buyer enrichment](/Users/aa/Documents/onesales/crates/onesales-jobs/src/buyer_enrichment/launcher.rs:485)
uses the shared execution resource `provider:leadspicker` and terminal dependencies
between account steps. A count greater than one could eventually support bounded
parallelism across appropriate work, but adding a capacity policy while retaining
that exclusive resource still serializes it. Removing the resource or sequential
dependencies requires an explicit application behavior decision.

The current
[Leadspicker gate](/Users/aa/Documents/onesales/crates/onesales-leadspicker-resilience/src/limits.rs:101)
already coordinates provider-account permits in PostgreSQL and charges attempted
calls without refunds. Its
[configuration](/Users/aa/Documents/onesales/crates/onesales-leadspicker-resilience/src/config.rs:149)
covers in-flight calls, requests per minute, daily combined usage, and a separate
daily endpoint allowance. Its
[window rules](/Users/aa/Documents/onesales/crates/onesales-leadspicker-resilience/src/limit_windows.rs:21)
use cost one or two, calendar minute/day boundaries, and minimum spacing.

The gate wraps the
[actual live fetch](/Users/aa/Documents/onesales/crates/onesales-leadspicker-resilience/src/cache.rs:270),
including direct and queued paths, circuit state, and provider feedback.
[Buyer enrichment](/Users/aa/Documents/onesales/crates/onesales-jobs/src/buyer_enrichment.rs:206)
can perform different provider operations, use cached data, and skip unnecessary
calls. Queue admission sees neither the eventual number of calls nor their
dispatch time. A rolling, unit-cost job log cannot preserve these contracts.

**Adoption target:** use queue concurrency for bounded job-family work and to
avoid avoidable handler contention. Retain the provider gate. Claims that this
epic deletes `onesales-leadspicker-resilience` or replaces its rate accounting are
unsupported. Queue rates would be an additional workload policy, not equivalent
provider protection.

### Vatbot: job granularity is the prerequisite

The [Runledger handler](/Users/aa/Documents/vatbot/crates/vatbot/src/extraction_worker/controller.rs:120)
executes `run_one()` from a scheduled pump with a local worker limit of one.
[That operation](/Users/aa/Documents/vatbot/crates/vatbot/src/extraction_worker/mod.rs:87)
runs extraction and reconciliation; it can also find no work. A pump admission
does not identify one paid document operation.

[Preparation](/Users/aa/Documents/vatbot/crates/vatbot/src/extraction_worker/prepare.rs:18)
reuses saved results and parses native ISDOC without paid-AI admission. The
[paid-call reservation](/Users/aa/Documents/vatbot/crates/vatbot/src/extraction_worker/db_transitions.rs:119)
occurs later. Its
[SQL ledger](/Users/aa/Documents/vatbot/crates/vatbot-db/src/extractions/queue.rs:539)
counts UTC calendar-day usage by provider, model, and prompt version, alongside
the extraction lease and result state. A trailing 24-hour job-admission limit
has different boundaries and would also count empty/local/reconciliation work.

**Adoption target:** first evaluate document-level or provider-stage jobs while
preserving the paid-call completion budget and saved-result recovery. A shared
pump concurrency cap is possible, but does not replace the nested domain queue
or spend ledger. Do not count this consumer as proof for rolling rate demand.

### Perdify: useful later, after a real upgrade fixture

[Round opener jobs](/Users/aa/Documents/perdify/crates/perdify-server/src/jobs/game_round_opener_generation.rs:143)
configure local concurrency four. A shared job-family or provider policy would
add a fleet-wide ceiling. Image and audio generation offer additional candidate
families, but actual desired fleet limits were not established by this audit.

The [cover reference-image semaphore](/Users/aa/Documents/perdify/crates/perdify-admin-server/src/jobs/cover_generation.rs:133)
is created within the operation and bounds parallel downloads. It does not map
one-for-one to whole-job capacity. AI
[request budgets](/Users/aa/Documents/perdify/crates/perdify-ai/src/request_budget.rs:1)
bound HTTP attempts/backoff within a deadline and remain necessary.

There is a concrete migration cost: the consumer is on 0.4.0 and calls the
[legacy progress API](/Users/aa/Documents/perdify/crates/perdify-server/src/jobs/game_round_opener_generation.rs:638).
Its [enqueue adapter](/Users/aa/Documents/perdify/crates/perdify-runledger-support/src/lib.rs:452)
also updates payloads and reconstructs canonical enqueue JSON. The new immutable
capacity membership must survive that operation, and constrained progress needs
the token/execution-service API. Strict idempotent enqueue is not a substitute
for the adapter's deliberately different payload-update semantics.

**Adoption target:** compile and exercise a historical consumer upgrade before
advertising easy adoption. Retain local fan-out bounds, request deadlines, and
domain idempotency; demonstrate precisely which job-level controls improve.

### HOCR2: compatibility evidence, not current execution demand

The [worker](/Users/aa/Documents/hocr2/apps/hocr-worker/src/main.rs:32)
explicitly registers no job handlers yet. The
[migration tool](/Users/aa/Documents/hocr2/apps/hocr-migrate/src/main.rs:159)
validates the Runledger schema, and the
[runtime work package](/Users/aa/Documents/hocr2/docs/delivery/work-packages/foundation/WP-FOUND-009.md:1)
specifies future runtime/backpressure work against a sealed 0.5.0 migration set.

Keep this consumer in migration compatibility examples. Its roadmap is not proof
of an adopted capacity feature, and a Runledger upgrade must respect its separately
owned schema foundation and Rails rollback contract.

## Changes to the epic

1. **Keep P01–P16 focused on distributed concurrency.** Preserve atomic admission,
   explicit multi-policy keys, denial without handler execution, lifecycle fencing,
   and all producer/recovery paths. These choices serve observable integration
   structures rather than hypothetical queue features.
2. **Give P14–P16 named consumer acceptance cases.** CreditKit and IdentityPro are
   the primary pilots. Exercise two independent workers, saturated and unrelated
   work, no-provider continuations, and legacy progress/producer adapters. Record
   the exact code removable, code retained, and behavior changed. A source-inspired
   fixture is not a deployed migration; label the evidence accurately.
3. **Keep local and provider controls where their resource boundary requires it.**
   Queue leases do not cover interactive calls or per-handler fan-out. Bookkeeping
   work must remain operable when a provider policy is paused. Demonstrate any
   needed consumer job decomposition before declaring a gate removable.
4. **Make bootstrap and upgrade usable.** Provide a create/read provisioning
   recipe that preserves operator-managed limits on restart, explicit attachment
   through producer helpers, and a cutover inventory of old pending unbound jobs.
   A same-type unbound backlog or synchronous caller is outside the new cap.
   Global key scope means one shared database, not all product databases.
5. **Defer the P17 entry task pending a qualifying consumer.** To resume, name the
   consumer/job, why queue admissions are the desired unit, accepted no-op/retry/
   continuation/prestart charges, required trailing-window semantics, provider
   controls retained, and an adoption fixture. P16 closure alone is insufficient.
   P18–P21 retain their dependency chain and designed contract for that future case.

The rate design is not rejected as technically invalid. Its product justification
is weaker than the concurrency work, and source evidence does not establish that
its storage, time-floor, and cleanup costs buy a desired consumer behavior yet.

## Ownership and positioning

Lead with: **shared limits for expensive background work across all workers, with
excess work waiting in the queue.** Describe reduced capacity-only handler waits
or continuations as a benefit to be demonstrated by the pilots. Avoid promising
that all application rate limiters or concurrency controls disappear.

Provider dispatch is a separate shared-library opportunity. Runlimit already owns
request limiting: its [current contract](/Users/aa/Documents/runlimit/README.md:190)
provides anchored fixed windows, and its
[GCRA implementation](/Users/aa/Documents/runlimit/README.md:218) is currently
process-local; PostgreSQL GCRA is not an available replacement. Any attempt to
move generic OneSales provider coordination upstream should evaluate that boundary
explicitly, retaining provider-specific cache, circuit, quota, and outcome policy.
That is a separate design investigation, not work silently added to this epic.

No production capacity setting, throughput improvement, eliminated code count,
or consumer migration success is established until the implementation pilots run.
