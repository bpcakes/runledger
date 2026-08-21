use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{HeaderMap, Method, Request, StatusCode, header};
use runledger_admin::{AdminAccess, AdminService, DataVisibility, router};
use runledger_core::jobs::{
    JobType, StepKey, WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder, WorkflowType,
};
use runledger_postgres::DbPool;
use runledger_postgres::jobs::{
    JobDefinitionUpsert, JobEnqueue, JobLogRecordInput, enqueue_job, enqueue_workflow_run,
    insert_job_log, upsert_job_definition_tx,
};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

const BASE: &str = "/api/admin/runledger/v1";
const JOB_TYPE: &str = "jobs.admin.contract";
const UNUSED_JOB_TYPE: &str = "jobs.admin.unused_catalog_entry";

fn assert_duration_close(actual: f64, expected: f64) {
    let tolerance = (16.0 * f64::EPSILON * expected.abs()).max(1e-9);
    assert!(
        (actual - expected).abs() <= tolerance,
        "expected {expected}, got {actual} (tolerance {tolerance})"
    );
}

async fn register_definition(pool: &DbPool, job_type: &str) {
    let mut tx = pool.begin().await.expect("begin definition transaction");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new(job_type),
            version: 3,
            max_attempts: 4,
            default_timeout_seconds: 90,
            default_priority: 25,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert definition");
    tx.commit().await.expect("commit definition transaction");
}

async fn enqueue(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    payload: &Value,
    idempotency_key: &str,
) -> Uuid {
    enqueue_job(
        pool,
        &JobEnqueue {
            job_type: JobType::new(JOB_TYPE),
            organization_id,
            payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: Some(idempotency_key),
            stage: None,
        },
    )
    .await
    .expect("enqueue test job")
}

async fn insert_log(pool: &DbPool, job_id: Uuid, message: &str) {
    insert_job_log(
        pool,
        &JobLogRecordInput {
            job_id,
            run_number: 1,
            attempt: None,
            level: "info".to_owned(),
            message: message.to_owned(),
            payload: json!({"message": message}),
        },
    )
    .await
    .expect("insert job log");
}

async fn insert_finished_attempt(pool: &DbPool, job_id: Uuid, attempt: i32, duration_ms: i64) {
    sqlx::query(
        "INSERT INTO job_attempts (
            job_id,
            run_number,
            attempt,
            worker_id,
            leased_at,
            started_at,
            finished_at,
            outcome
         )
         VALUES (
            $1,
            1,
            $2,
            'admin-metrics-test',
            now() - interval '1 hour',
            now() - interval '1 hour',
            now() - interval '1 hour' + $3::float8 * interval '1 millisecond',
            'TERMINAL'
         )",
    )
    .bind(job_id)
    .bind(attempt)
    .bind(duration_ms as f64)
    .execute(pool)
    .await
    .expect("insert finished metrics attempt");
}

async fn set_large_progress(pool: &DbPool, job_id: Uuid) {
    const LARGE_PROGRESS: i64 = 9_007_199_254_740_993;

    sqlx::query(
        "UPDATE job_queue
         SET progress_done = $2,
             progress_total = $2
         WHERE id = $1",
    )
    .bind(job_id)
    .bind(LARGE_PROGRESS)
    .execute(pool)
    .await
    .expect("set progress outside the JavaScript safe-integer range");
}

async fn request_json(
    app: &Router,
    method: Method,
    uri: String,
    access: Option<AdminAccess>,
) -> (StatusCode, HeaderMap, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .expect("build request");
    if let Some(access) = access {
        request.extensions_mut().insert(access);
    }
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("admin router response");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), 1_000_000)
        .await
        .expect("read response body");
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("response is JSON")
    };
    (status, headers, value)
}

async fn get_json(
    app: &Router,
    path: &str,
    access: Option<AdminAccess>,
) -> (StatusCode, HeaderMap, Value) {
    request_json(app, Method::GET, format!("{BASE}{path}"), access).await
}

fn item_ids(response: &Value) -> Vec<Uuid> {
    response["items"]
        .as_array()
        .expect("items array")
        .iter()
        .map(|item| {
            Uuid::parse_str(item["id"].as_str().expect("item id string")).expect("item id UUID")
        })
        .collect()
}

#[tokio::test]
async fn read_only_contract_is_scoped_redacted_and_postgres_18_backed() {
    let (pool, database) = setup_ephemeral_pool("admin_http_contract", 6).await;
    let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
        .fetch_one(&pool)
        .await
        .expect("read PostgreSQL server version");
    let server_version_num = sqlx::query_scalar::<_, String>("SHOW server_version_num")
        .fetch_one(&pool)
        .await
        .expect("read PostgreSQL numeric server version")
        .parse::<i32>()
        .expect("parse PostgreSQL numeric server version");
    eprintln!(
        "runledger-admin contract PostgreSQL server_version={server_version} server_version_num={server_version_num}"
    );
    assert!(
        (180_000..190_000).contains(&server_version_num),
        "contract test must run against PostgreSQL 18; got {server_version}"
    );

    register_definition(&pool, JOB_TYPE).await;
    register_definition(&pool, UNUSED_JOB_TYPE).await;
    let organization_a = Uuid::from_u128(0xaaaaaaaa_aaaa_4aaa_8aaa_aaaaaaaaaaaa);
    let organization_b = Uuid::from_u128(0xbbbbbbbb_bbbb_4bbb_8bbb_bbbbbbbbbbbb);
    let secret_payload = json!({"customer_email": "private@example.test"});
    let global_job = enqueue(&pool, None, &json!({"scope": "global"}), "global-key").await;
    let organization_a_job = enqueue(
        &pool,
        Some(organization_a),
        &secret_payload,
        "organization-a-key",
    )
    .await;
    let organization_b_job = enqueue(
        &pool,
        Some(organization_b),
        &json!({"scope": "organization-b"}),
        "organization-b-key",
    )
    .await;
    insert_log(
        &pool,
        organization_a_job,
        "customer private@example.test imported",
    )
    .await;
    insert_log(&pool, organization_a_job, "newer log").await;
    insert_log(&pool, organization_a_job, "newest log").await;
    insert_finished_attempt(&pool, organization_a_job, 1, 10).await;
    insert_finished_attempt(&pool, organization_b_job, 1, 1_000).await;
    insert_finished_attempt(&pool, organization_b_job, 2, 2_000).await;
    insert_finished_attempt(&pool, organization_b_job, 3, 3_000).await;
    set_large_progress(&pool, organization_a_job).await;

    let workflow_payload = json!({"customer_email": "workflow-private@example.test"});
    let workflow_metadata = json!({"source": "admin-contract", "secret": true});
    let workflow_step = WorkflowStepEnqueueBuilder::new(
        StepKey::new("import"),
        JobType::new(JOB_TYPE),
        &workflow_payload,
    )
    .depends_on_success(&[StepKey::new("foreign-import")])
    .try_build()
    .expect("build workflow step");
    let foreign_workflow_payload = json!({"organization_b_secret": true});
    let foreign_workflow_step = WorkflowStepEnqueueBuilder::new(
        StepKey::new("foreign-import"),
        JobType::new(JOB_TYPE),
        &foreign_workflow_payload,
    )
    .organization_id(organization_b)
    .try_build()
    .expect("build cross-organization workflow step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.admin.contract"),
        &workflow_metadata,
    )
    .organization_id(organization_a)
    .idempotency_key("workflow-organization-a-key")
    .step(workflow_step)
    .step(foreign_workflow_step)
    .try_build()
    .expect("build workflow");
    let workflow_run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow");

    let app = Router::new().nest(BASE, router(AdminService::new(pool.clone())));
    let metadata_access = AdminAccess::organization(organization_a, DataVisibility::MetadataOnly);
    let full_access = AdminAccess::organization(organization_a, DataVisibility::Full);
    let definition_access = metadata_access.with_service_wide_definitions();
    let all_access = AdminAccess::all(DataVisibility::MetadataOnly);
    let all_full_access = AdminAccess::all(DataVisibility::Full);

    let (status, headers, body) = get_json(&app, "/capabilities", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["error"]["code"], "admin.unauthorized");
    assert_eq!(
        headers
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("private, no-store")
    );

    let (status, headers, body) = get_json(&app, "/capabilities", Some(metadata_access)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("private, no-store")
    );
    assert_eq!(body["api_version"], "v1");
    assert_eq!(body["scope"]["kind"], "organization");
    assert_eq!(body["scope"]["organization_id"], organization_a.to_string());
    assert_eq!(body["visibility"], "metadata_only");
    assert_eq!(body["actions"], json!([]));
    assert!(
        !body["resources"]
            .as_array()
            .expect("capability resources")
            .contains(&json!("definitions"))
    );

    let (status, _, body) = get_json(&app, "/jobs?limit=1", Some(metadata_access)).await;
    assert_eq!(status, StatusCode::OK);
    let organization_a_ids = item_ids(&body);
    assert_eq!(organization_a_ids.len(), 1);
    assert!(!organization_a_ids.contains(&organization_b_job));
    assert!(!organization_a_ids.contains(&global_job));
    assert_eq!(body["page"]["has_more"], false);
    assert_eq!(body["page"]["max_offset"], runledger_admin::MAX_PAGE_OFFSET);
    assert!(
        body["items"]
            .as_array()
            .expect("job items")
            .iter()
            .all(|item| item["organization_id"] == organization_a.to_string())
    );
    assert_eq!(body["items"][0]["progress_done"], "9007199254740993");
    assert_eq!(body["items"][0]["progress_total"], "9007199254740993");

    let (status, _, body) = get_json(&app, "/jobs", Some(full_access)).await;
    assert_eq!(status, StatusCode::OK);
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
        assert!(
            body["items"][0].get(detail_only_field).is_none(),
            "job list summaries must not expose or load {detail_only_field}"
        );
    }

    let (status, _, body) = get_json(
        &app,
        &format!("/jobs/{organization_a_job}"),
        Some(metadata_access),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["job"]["progress_done"], "9007199254740993");
    assert_eq!(body["job"]["progress_total"], "9007199254740993");
    assert!(body["job"].get("payload").is_none());
    assert!(body["job"].get("idempotency_key").is_none());
    assert!(
        body["job"]["redacted_fields"]
            .as_array()
            .expect("redacted fields")
            .contains(&json!("payload"))
    );

    let (status, _, body) = get_json(
        &app,
        &format!("/jobs/{organization_a_job}"),
        Some(full_access),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["job"]["payload"], secret_payload);
    assert_eq!(body["job"]["idempotency_key"], "organization-a-key");
    assert_eq!(body["job"]["redacted_fields"], json!([]));

    let (status, _, body) = get_json(
        &app,
        &format!("/jobs/{organization_b_job}"),
        Some(metadata_access),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "admin.not_found");

    let (status, _, body) = get_json(
        &app,
        &format!("/jobs/{organization_a_job}/events"),
        Some(metadata_access),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(!body["items"].as_array().expect("event items").is_empty());
    assert!(body["items"][0]["id"].is_string());
    assert!(body["items"][0].get("payload").is_none());

    let (status, _, body) = get_json(
        &app,
        &format!("/jobs/{organization_a_job}/logs"),
        Some(metadata_access),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["items"][0].get("message").is_none());
    assert!(body["items"][0].get("payload").is_none());

    let (status, _, body) = get_json(
        &app,
        &format!("/jobs/{organization_a_job}/logs?limit=2"),
        Some(full_access),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["items"][0]["message"], "newest log");
    assert_eq!(body["items"][1]["message"], "newer log");
    assert_eq!(body["page"]["order"], "newest_first");
    assert_eq!(body["page"]["has_more"], true);
    let next_cursor = body["page"]["next_cursor"]
        .as_str()
        .expect("next log cursor")
        .to_owned();

    let (status, _, body) = get_json(
        &app,
        &format!("/jobs/{organization_a_job}/logs?limit=2&cursor={next_cursor}"),
        Some(full_access),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["items"][0]["message"],
        "customer private@example.test imported"
    );
    assert_eq!(body["page"]["has_more"], false);
    assert_eq!(body["page"]["next_cursor"], Value::Null);

    let (status, _, body) = get_json(
        &app,
        &format!("/jobs/{organization_a_job}/logs?limit=1&order=oldest_first"),
        Some(full_access),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["items"][0]["message"],
        "customer private@example.test imported"
    );

    let (status, _, body) = get_json(&app, "/metrics", Some(metadata_access)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["items"]
            .as_array()
            .expect("metric items")
            .iter()
            .any(|item| item["job_type"] == JOB_TYPE
                && item["pending_count"]
                    .as_str()
                    .and_then(|count| count.parse::<i64>().ok())
                    .unwrap_or(0)
                    >= 1)
    );
    assert!(
        body["items"]
            .as_array()
            .expect("metric items")
            .iter()
            .all(|item| item["job_type"] != UNUSED_JOB_TYPE)
    );
    assert_eq!(body["page"]["limit"], 50);
    assert_eq!(body["page"]["offset"], 0);
    assert_eq!(body["page"]["max_offset"], runledger_admin::MAX_PAGE_OFFSET);
    let organization_metric = body["items"]
        .as_array()
        .expect("metric items")
        .iter()
        .find(|item| item["job_type"] == JOB_TYPE)
        .expect("organization metric");
    assert_eq!(
        organization_metric["p50_duration_ms_24h"].as_f64(),
        Some(10.0)
    );
    assert_eq!(
        organization_metric["p95_duration_ms_24h"].as_f64(),
        Some(10.0)
    );

    let (status, _, body) = get_json(
        &app,
        &format!("/metrics?job_type={UNUSED_JOB_TYPE}"),
        Some(metadata_access),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["items"], json!([]));

    let (status, _, body) = get_json(&app, "/metrics", Some(all_access)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["items"]
            .as_array()
            .expect("service-wide metric items")
            .iter()
            .any(|item| item["job_type"] == UNUSED_JOB_TYPE)
    );
    let service_metric = body["items"]
        .as_array()
        .expect("service-wide metric items")
        .iter()
        .find(|item| item["job_type"] == JOB_TYPE)
        .expect("service-wide job metric");
    assert_eq!(
        service_metric["p50_duration_ms_24h"].as_f64(),
        Some(1_500.0)
    );
    let service_p95 = service_metric["p95_duration_ms_24h"]
        .as_f64()
        .expect("service-wide p95 duration");
    assert_duration_close(service_p95, 2_850.0);

    let (status, _, first_metrics_page) =
        get_json(&app, "/metrics?limit=1", Some(all_access)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        first_metrics_page["items"]
            .as_array()
            .expect("first metrics page items")
            .len(),
        1
    );
    assert_eq!(first_metrics_page["page"]["has_more"], true);
    let (status, _, second_metrics_page) =
        get_json(&app, "/metrics?limit=1&offset=1", Some(all_access)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        second_metrics_page["items"]
            .as_array()
            .expect("second metrics page items")
            .len(),
        1
    );
    assert_ne!(
        first_metrics_page["items"][0]["job_type"],
        second_metrics_page["items"][0]["job_type"]
    );

    let (status, _, body) = get_json(&app, "/workflows", Some(metadata_access)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(item_ids(&body).contains(&workflow_run.id));

    let (status, _, body) = get_json(&app, "/workflows", Some(all_full_access)).await;
    assert_eq!(status, StatusCode::OK);
    for detail_only_field in ["idempotency_key", "metadata", "redacted_fields"] {
        assert!(
            body["items"][0].get(detail_only_field).is_none(),
            "workflow list summaries must not expose or load {detail_only_field}"
        );
    }

    let (status, _, body) = get_json(
        &app,
        &format!("/workflows/{}", workflow_run.id),
        Some(metadata_access),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["workflow"].get("metadata").is_none());
    assert_eq!(body["steps"].as_array().expect("workflow steps").len(), 1);
    assert_eq!(
        body["steps"][0]["organization_id"],
        organization_a.to_string()
    );
    assert!(body["steps"][0].get("payload").is_none());
    assert_eq!(body["steps"][0]["status"], "BLOCKED");
    assert_eq!(body["steps"][0]["visible_dependency_count_total"], 0);
    assert_eq!(body["steps"][0]["visible_dependency_count_pending"], 0);
    assert_eq!(body["steps"][0]["visible_dependency_count_unsatisfied"], 0);
    assert_eq!(body["steps"][0]["has_hidden_prerequisites"], true);
    assert!(body["steps"][0].get("dependency_count_total").is_none());
    assert_eq!(body["steps_page"]["has_more"], false);
    assert_eq!(body["dependencies"], json!([]));

    let (status, _, body) = get_json(
        &app,
        &format!(
            "/workflows/{}?step_limit=1&dependency_limit=1",
            workflow_run.id
        ),
        Some(all_full_access),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["steps"].as_array().expect("workflow steps").len(), 1);
    assert_eq!(body["steps"][0]["visible_dependency_count_total"], 1);
    assert_eq!(body["steps"][0]["visible_dependency_count_pending"], 1);
    assert_eq!(body["steps"][0]["visible_dependency_count_unsatisfied"], 0);
    assert_eq!(body["steps"][0]["has_hidden_prerequisites"], false);
    assert_eq!(body["steps_page"]["has_more"], true);
    assert_eq!(
        body["dependencies"].as_array().expect("dependencies").len(),
        1
    );

    let (status, _, body) = get_json(
        &app,
        &format!("/workflows/{}?step_limit=1&step_offset=1", workflow_run.id),
        Some(all_full_access),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["steps"][0]["organization_id"],
        organization_b.to_string()
    );
    assert_eq!(body["steps"][0]["payload"], foreign_workflow_payload);

    let (status, _, body) = get_json(&app, "/definitions", Some(metadata_access)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"]["code"], "admin.forbidden");

    let (status, _, body) = get_json(&app, "/definitions", Some(definition_access)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["items"]
            .as_array()
            .expect("definition items")
            .iter()
            .any(|item| item["job_type"] == JOB_TYPE)
    );

    let (status, _, body) = get_json(&app, "/jobs?job_type=%25", Some(metadata_access)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["items"], json!([]));
    let (status, _, body) =
        get_json(&app, "/workflows?workflow_type=%25", Some(metadata_access)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["items"], json!([]));
    let (status, _, body) =
        get_json(&app, "/definitions?job_type=%25", Some(definition_access)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["items"], json!([]));

    let (status, _, body) = get_json(&app, "/jobs?limit=0", Some(metadata_access)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "admin.invalid_query");
    let (status, _, _) = get_json(
        &app,
        &format!("/jobs?offset={}", runledger_admin::MAX_PAGE_OFFSET),
        Some(metadata_access),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, body) = get_json(
        &app,
        &format!("/jobs?offset={}", runledger_admin::MAX_PAGE_OFFSET + 1),
        Some(metadata_access),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "admin.invalid_query");
    let (status, _, body) = get_json(&app, "/jobs/not-a-uuid", Some(metadata_access)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "admin.invalid_identifier");
    let (status, _, body) = get_json(
        &app,
        &format!("/jobs/{organization_a_job}/logs?cursor=not-an-id"),
        Some(metadata_access),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "admin.invalid_query");

    let (status, _, _) = request_json(
        &app,
        Method::POST,
        format!("{BASE}/jobs/{organization_a_job}/cancel"),
        Some(all_access),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _, body) = get_json(&app, "/jobs?limit=50", Some(all_access)).await;
    assert_eq!(status, StatusCode::OK);
    let all_ids = item_ids(&body);
    assert!(all_ids.contains(&global_job));
    assert!(all_ids.contains(&organization_a_job));
    assert!(all_ids.contains(&organization_b_job));

    teardown_ephemeral_pool(pool, database).await;
}
