use super::super::QueryErrorSpec;

pub(super) fn classify_constraint(constraint: &str) -> Option<QueryErrorSpec> {
    match constraint {
        "uq_workflow_runs_type_idempotency_org" | "uq_workflow_runs_type_idempotency_global" => {
            Some(QueryErrorSpec::conflict(
                "workflow.already_enqueued",
                "Workflow is already enqueued for this idempotency key.",
            ))
        }
        "os_workflow_job_linkage_symmetry" => Some(QueryErrorSpec::validation(
            "workflow.linkage_symmetry_violation",
            "Workflow job linkage is invalid.",
        )),
        "os_workflow_job_linkage_symmetry_trigger_table" => Some(QueryErrorSpec::validation(
            "workflow.linkage_symmetry_trigger_table_invalid",
            "Workflow linkage symmetry trigger was called from an unsupported table.",
        )),
        "os_workflow_job_linkage_expand_audit"
        | "os_workflow_job_linkage_expand_rollback_audit"
        | "os_workflow_job_linkage_contract_audit" => Some(QueryErrorSpec::validation(
            "workflow.linkage_cutover_audit_failed",
            "Workflow job linkage cutover found inconsistent stored relationships.",
        )),
        "os_workflow_job_linkage_compatibility_trigger_table" => Some(QueryErrorSpec::validation(
            "workflow.linkage_compatibility_trigger_table_invalid",
            "Workflow linkage compatibility trigger was called from an unsupported table.",
        )),
        "os_workflow_external_gate_downgrade_external_steps_exist"
        | "os_workflow_external_gate_downgrade_waiting_steps_exist"
        | "os_workflow_external_gate_downgrade_waiting_runs_exist" => {
            Some(QueryErrorSpec::validation(
                "workflow.external_gate_downgrade_blocked",
                "External workflow gate downgrade is blocked by existing workflow state.",
            ))
        }
        _ => None,
    }
}
