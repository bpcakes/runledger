use super::super::QueryErrorSpec;

pub(super) fn classify_constraint(constraint: &str) -> Option<QueryErrorSpec> {
    match constraint {
        "uq_job_queue_type_idempotency_org" | "uq_job_queue_type_idempotency_global" => {
            Some(QueryErrorSpec::conflict(
                "job.already_enqueued",
                "Job is already enqueued for this idempotency key.",
            ))
        }
        "fk_job_queue_job_type" | "fk_job_runtime_configs_job_type" => {
            Some(QueryErrorSpec::validation(
                "job.definition_not_found",
                "Job definition does not exist.",
            ))
        }
        "fk_job_queue_organization" => Some(QueryErrorSpec::validation(
            "job.organization_not_found",
            "Job organization does not exist.",
        )),
        _ => None,
    }
}
