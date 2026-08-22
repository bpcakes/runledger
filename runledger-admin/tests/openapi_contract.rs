use std::collections::BTreeSet;

use serde_json::Value;

const EXPECTED_PATHS: [&str; 11] = [
    "/capabilities",
    "/definitions",
    "/jobs",
    "/jobs/{job_id}",
    "/jobs/{job_id}/events",
    "/jobs/{job_id}/logs",
    "/metrics",
    "/workflows",
    "/workflows/{workflow_id}",
    "/workflows/{workflow_id}/dependencies",
    "/workflows/{workflow_id}/steps",
];

#[test]
fn openapi_contract_is_versioned_and_covers_every_route() {
    let document: Value = serde_json::from_str(&runledger_admin::openapi_json())
        .expect("generated OpenAPI must be valid JSON");

    assert_eq!(document["openapi"], "3.1.0");
    assert_eq!(document["info"]["version"], runledger_admin::API_VERSION);
    assert_eq!(document["servers"][0]["url"], "/api/admin/runledger/v1");

    let paths = document["paths"]
        .as_object()
        .expect("OpenAPI paths must be an object");
    let actual_paths = paths.keys().map(String::as_str).collect::<BTreeSet<_>>();
    assert_eq!(actual_paths, EXPECTED_PATHS.into_iter().collect());

    let operation_ids = paths
        .values()
        .map(|path| {
            path["get"]["operationId"]
                .as_str()
                .expect("every admin GET must have an operation id")
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(operation_ids.len(), EXPECTED_PATHS.len());
}

#[test]
fn openapi_contract_distinguishes_null_from_redaction() {
    let document: Value = serde_json::from_str(&runledger_admin::openapi_json())
        .expect("generated OpenAPI must be valid JSON");
    let job = &document["components"]["schemas"]["Job"];
    let required = job["required"]
        .as_array()
        .expect("Job.required must be an array")
        .iter()
        .filter_map(Value::as_str)
        .collect::<BTreeSet<_>>();

    assert!(required.contains("organization_id"));
    assert!(required.contains("finished_at"));
    assert!(!required.contains("payload"));
    assert!(!required.contains("last_error_message"));
    assert_eq!(job["properties"]["organization_id"]["type"][1], "null");
    assert_eq!(
        document["components"]["schemas"]["Capabilities"]["properties"]["api_version"]["enum"][0],
        "v1"
    );
}

#[test]
fn openapi_contract_keeps_sensitive_fields_out_of_list_summaries() {
    let document: Value = serde_json::from_str(&runledger_admin::openapi_json())
        .expect("generated OpenAPI must be valid JSON");
    let schemas = &document["components"]["schemas"];

    let job_summary = schemas["JobSummary"]["properties"]
        .as_object()
        .expect("JobSummary.properties must be an object");
    for detail_only_field in [
        "payload",
        "checkpoint",
        "output",
        "idempotency_key",
        "worker_id",
        "status_reason",
        "last_error_message",
        "redacted_fields",
    ] {
        assert!(!job_summary.contains_key(detail_only_field));
    }

    let workflow_summary = schemas["WorkflowSummary"]["properties"]
        .as_object()
        .expect("WorkflowSummary.properties must be an object");
    for detail_only_field in ["idempotency_key", "metadata", "redacted_fields"] {
        assert!(!workflow_summary.contains_key(detail_only_field));
    }
}

#[test]
fn openapi_contract_uses_exact_decimal_strings_for_unbounded_integers() {
    let document: Value = serde_json::from_str(&runledger_admin::openapi_json())
        .expect("generated OpenAPI must be valid JSON");
    let schemas = &document["components"]["schemas"];

    assert_eq!(schemas["ExactI64"]["type"], "string");
    assert_eq!(schemas["ExactI64"]["pattern"], "^-?[0-9]+$");

    for (schema, fields) in [
        (
            "JobMetrics",
            &[
                "pending_count",
                "leased_count",
                "stale_leases",
                "succeeded_24h",
                "retryable_24h",
                "terminal_24h",
                "panicked_24h",
                "timeout_24h",
                "dead_lettered_24h",
                "continued_24h",
                "active_continued_count",
            ][..],
        ),
        ("JobSummary", &["progress_done", "progress_total"]),
        ("Job", &["progress_done", "progress_total"]),
        ("JobEvent", &["progress_done", "progress_total"]),
    ] {
        for field in fields {
            let property = &schemas[schema]["properties"][field];
            let exact_reference = property["$ref"].as_str().or_else(|| {
                ["oneOf", "anyOf"].iter().find_map(|composition| {
                    property[*composition].as_array().and_then(|options| {
                        options.iter().find_map(|option| option["$ref"].as_str())
                    })
                })
            });
            assert_eq!(
                exact_reference,
                Some("#/components/schemas/ExactI64"),
                "{schema}.{field} must preserve the full signed 64-bit range"
            );
        }
    }
}

#[test]
fn openapi_contract_bounds_every_offset_parameter() {
    let document: Value = serde_json::from_str(&runledger_admin::openapi_json())
        .expect("generated OpenAPI must be valid JSON");
    let expected_maximum = Value::from(runledger_admin::MAX_PAGE_OFFSET);
    let mut offset_parameters = 0;

    for path in document["paths"]
        .as_object()
        .expect("OpenAPI paths must be an object")
        .values()
    {
        for parameter in path["get"]["parameters"].as_array().into_iter().flatten() {
            let Some(name) = parameter["name"].as_str() else {
                continue;
            };
            if name == "offset" || name.ends_with("_offset") {
                offset_parameters += 1;
                assert_eq!(
                    parameter["schema"]["maximum"], expected_maximum,
                    "{name} must match MAX_PAGE_OFFSET"
                );
            }
        }
    }

    assert_eq!(offset_parameters, 6);
}
