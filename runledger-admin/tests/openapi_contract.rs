use std::collections::BTreeSet;

use serde_json::Value;

const EXPECTED_PATHS: [&str; 9] = [
    "/capabilities",
    "/definitions",
    "/jobs",
    "/jobs/{job_id}",
    "/jobs/{job_id}/events",
    "/jobs/{job_id}/logs",
    "/metrics",
    "/workflows",
    "/workflows/{workflow_id}",
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
