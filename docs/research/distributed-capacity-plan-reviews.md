# Distributed capacity planning review record

Date: 2026-09-06. Source baseline: `7cdb9cb`.
Plan: [distributed capacity implementation epic](../distributed-capacity-plan.md).

This record distinguishes completed planning checks from future implementation
validation. Reviews use the native GPT-6 agent available in this session, with
independent reviewer contexts where stated. No GPT Pro web session, Claude,
Cursor, or other external model review is claimed.

## Prerequisite reinvestigation

Read the current enqueue, claim, lifecycle, workflow locking/recovery, intent,
catalog schedule, migration, and public type sources. An independent native
agent reviewed the proposed savepoint protocol and found the workflow-step
prelock, FK-compatible policy lock, 24-savepoint precedent, and admission-token
fence requirements. The root agent integrated those decisions before round 1.

Ran `uv run docs/research/probes/capacity_protocol.py` successfully against a
new disposable PostgreSQL 18.4 container. It applied 17 migration SQL files and
exercised the assertions in the checked-in JSON result. Early development runs
failed on a PL/pgSQL variable/alias collision and an incorrectly narrow expected
exception class; both probe bugs were fixed before the recorded successful run.
A subsequent review corrected the resource-contention fixture to use disjoint
policies and added actual blocked-prefix admission and reverse step-order checks.
The probe remains a SQL protocol model, not execution of the production Rust APIs.

## Plan review round 1 — structural corrections

Independent reviewer read the entire initial 1,728-line plan and relevant source.
Accepted revisions:

- Separate new attachment validation from materializing already-persisted archived references.
- Protect rate-only canceled owners' legacy resources from premature queue deletion.
- Clear the queue admission generation on lease exit and record immutable rate-proof origins.
- Derive raw lease deadlines before resource writes; keep rate logical time separate.
- Define bytewise key canonicalization with PostgreSQL COLLATE "C".
- Assign automatic rate cleanup and rate diagnostics to explicit implementation tasks.

A separate fresh-context P17 check found missing cleanup ownership/bounds and
the pre-P18 evaluator/race-harness contract. These were added: pool-owned API,
automatic reaper caller, 16-policy/1,000-row bounds, continuation cursor, all
policy states, and a shared private floor/window evaluator.

Validation: the 21-task planned DAG is acyclic; P01 is the entry task, P21 is
the sole terminal gate, all tasks feed it, and inverse dependency labels match.
Reviewer sampled D01/D03/D04/D06/D09 and found their rationale adequate.
The diff was structural, so the plan required another review round.

## Plan review round 2 — guard and lifetime clarification

The independent reviewer read the full revised plan and verified three additions:
lease-entry policy enumeration from authoritative owner arrays; deletion checks
using immutable membership and retained legacy claim job IDs after cancellation;
and long leases whose rate history expires before later token-based lifecycle work.
The revised standalone P17 task passed. Its uncertain-commit default was adopted:
stop cleanup, retain the confirmed cursor, and retry on the next reaper tick.

Validation: 21 tasks and 31 blocking edges remain acyclic, all tasks reach P21,
inverse Unblocks labels match, and P01 remains the only entry task. Reviewer
sampled D02/D05/D07/D08/D10 and found each justification adequate. The round's
diff added 12 lines of guard/verification detail; no architecture replacement
was needed. The reviewer reported no remaining actionable issue after integration.

## Plan review round 3 — task execution and resize semantics

The independent reviewer read the complete 1,790-line revision. One assertion
needed clarification: a live limit reduction may leave valid existing occupancy
above the new limit. P14 now checks admission against its locked limit revision,
checks occupancy with unchanged limits, and proves no eviction/new admission
when a limit is reduced below current occupancy.

A separate fresh-context P09 review confirmed implementability and clarified
schedule Preserve/Replace defaults and archived keys retained during catalog
resync. These defaults were adopted to avoid ordinary startup failing when an
already-bound policy is archived.

Validation: the DAG/inverse dependency checks still pass for all 21 tasks;
P01 remains the entry and P21 the final gate. D01/D03/D04/D06/D09 rationale
sampling passed. Changes concern verification and schedule update edge semantics;
the admission architecture and delivery graph remain unchanged.

## Plan review round 4 — fresh whole-plan review

A fresh native reviewer read the entire 1,798-line revision and targeted source,
without relying on the previous reviewers' findings. It reported no actionable
findings and steady state. API compatibility, all producer paths, generation
fencing, cancellation retention, rate-history lifetime, and both release gates
had explicit implementation and verification ownership.

Validation: all 21 tasks and 31 blocking edges pass DAG, final-gate reachability,
and inverse dependency checks. D01/D03/D04/D06/D08 rationale samples passed.
The standalone revised P09 task also passed with no essential behavior decision
missing. A final source check made the existing capacity-exclusion requirement
explicit in both legacy resource-head and final-candidate CTEs and added its
regression case; this changes no architecture or dependency.

Four complete native plan review rounds are finished. The most recent round
required no architectural revisions. Production Rust integration and performance
checks remain future implementation gates, as documented in the plan.

## Beads conversion and validation

Created epic `runledger-distributed-capacity-bp2` with 21 open implementation
tasks. The six completed passes were:

1. Read back all 21 saved task descriptions with `br show --json`; each matched
   its source description and retained rationale, work, verification, and
   acceptance criteria.
2. Replaced planning labels with actual prerequisite and consumer issue IDs in
   every task, and corrected P07's source path to `workflows/mutate/append.rs`.
3. Inspected the saved graph with `bv --robot-graph --graph-format=json` and
   `br dep cycles --json`: 31 internal blocking edges, 21 parent-child edges,
   the external planning prerequisite, and no cycles.
4. Compared every saved dependency set and description with the plan. Confirmed
   all tasks remain open, both producer/lifecycle test matrices have owners,
   and P17 remains blocked on the P16 concurrency release gate.
5. Fresh reviewers independently read the actual P09 and P17 Beads. Added
   explicit key-count and rate-window bounds, the raw-clock requirement, and
   P17's runtime reaper source path. Both revised Beads passed their follow-up
   reviews with no remaining findings.
6. Closed the completed research/planning prerequisites and checked live
   readiness with `br ready --json`, `bv --robot-plan --label capacity-controls`,
   and `br dep cycles --json`. Only `runledger-distributed-capacity-bp2.1` is
   ready within the epic. The epic and all 21 children remain open; there are
   no dependency cycles.

The completed prerequisites are `runledger-cod` (SQL protocol-model evidence),
`runledger-ze4` (durable API/rollout contract), and `runledger-bva` (reviewed plan
and epic conversion). Their closure does not claim production implementation.

Artifact checks covered Python syntax, JSON parsing, local Markdown links,
the task catalog and graph, README consistency, and whitespace. The disposable
protocol probe passed against PostgreSQL 18.4 with all 17 current migrations
applied. It models the proposed SQL protocol; production Rust integration,
fault coverage, and performance validation remain implementation work.

## Consumer audit amendment

After the initial plan was committed, the user requested a fresh audit of local
Runledger consumers. The root agent inspected the six primary product checkouts,
deduplicating 16 manifests/workspaces into five execution integrations and one
migration/scaffold integration. The [consumer audit](distributed-capacity-consumer-audit-2026-09-06.md)
records current lockfile versions, source revisions, dirty-tree limitations,
call sites, and adoption consequences.

Accepted changes: named CreditKit/IdentityPro concurrency fixtures; explicit
local-resource and no-provider bookkeeping boundaries; OneSales's retained
provider gate and exclusive resource; Perdify's historical progress/enqueue
adapters; usable operator-preserving bootstrap and legacy-backlog coverage;
and conditional rate delivery. P17 is deferred until a consumer accepts exact
job-admission accounting. Its dependent tasks remain open and blocked.

These changes add consumer acceptance requirements and a delivery precondition;
they do not change the SQL protocol or the 21-task/31-edge implementation graph.
The earlier four native planning rounds reviewed the pre-audit plan. No additional
independent model review, downstream compilation, consumer migration, production
load measurement, or database experiment is claimed for this source audit.

Validation checked 64 local links/line anchors, all 21 plan task sections, exact
readback of the ten revised task descriptions/acceptance criteria, unchanged
dependency sets, 31 internal blocking edges, and a cycle-free Beads graph.
`bv --robot-plan --label capacity-controls` succeeded. P01 remains the sole
ready implementation task; P17 is deferred. Whitespace checks passed.
