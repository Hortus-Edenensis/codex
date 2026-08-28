use super::*;
use crate::function_tool::FunctionCallError;
use crate::session::tests::make_session_and_context;
use chrono::Utc;
use codex_utils_absolute_path::AbsolutePathBuf;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn required_agent_job_store_rejects_non_postgres_sessions() {
    let (session, _) = make_session_and_context().await;

    let err = match required_agent_job_store(&Arc::new(session)) {
        Ok(_) => panic!("non-PG session should not expose an agent job store"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        FunctionCallError::Fatal(message) if message == "agent job store is unavailable for this session"
    ));
}

#[tokio::test]
async fn report_agent_job_result_rejects_non_object_payloads_before_store_lookup() {
    let (session, _) = make_session_and_context().await;

    let err = match report_agent_job_result::handle(
        Arc::new(session),
        json!({
            "job_id": "job-1",
            "item_id": "item-1",
            "result": ["not", "an", "object"]
        })
        .to_string(),
    )
    .await
    {
        Ok(_) => panic!("non-object result should fail closed"),
        Err(err) => err,
    };

    assert!(matches!(
        err,
        FunctionCallError::RespondToModel(message) if message == "result must be a JSON object"
    ));
}

#[test]
fn normalize_concurrency_clamps_to_requested_maximums() {
    assert_eq!(normalize_concurrency(None, None), 16);
    assert_eq!(normalize_concurrency(Some(0), None), 1);
    assert_eq!(normalize_concurrency(Some(128), None), 64);
    assert_eq!(normalize_concurrency(Some(12), Some(4)), 4);
}

#[test]
fn normalize_max_runtime_seconds_rejects_zero() {
    let err = normalize_max_runtime_seconds(Some(0)).expect_err("zero runtime should fail");
    assert!(matches!(
        err,
        FunctionCallError::RespondToModel(message) if message == "max_runtime_seconds must be >= 1"
    ));
    assert_eq!(
        normalize_max_runtime_seconds(Some(9)).expect("runtime"),
        Some(9)
    );
}

#[test]
fn render_instruction_template_replaces_placeholders_and_preserves_escaped_braces() {
    let rendered = render_instruction_template(
        "Hello {name}; keep {{json}} and count={count}",
        &json!({"name": "Ada", "count": 3}),
    );

    assert_eq!(rendered, "Hello Ada; keep {json} and count=3");
}

#[test]
fn parse_csv_strips_bom_and_skips_blank_rows() {
    let (headers, rows) =
        parse_csv("\u{feff}id,name\n1,Ada\n,\n2,Bob\n").expect("csv should parse");

    assert_eq!(headers, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "Ada".to_string()],
            vec!["2".to_string(), "Bob".to_string()],
        ]
    );
}

#[test]
fn ensure_unique_headers_rejects_duplicates() {
    let err = ensure_unique_headers(&["id".to_string(), "id".to_string()])
        .expect_err("duplicate headers should fail");
    assert!(matches!(
        err,
        FunctionCallError::RespondToModel(message) if message == "csv header id is duplicated"
    ));
}

#[test]
fn default_output_csv_path_uses_input_stem_and_job_prefix() {
    let path =
        AbsolutePathBuf::from_absolute_path(tempdir().expect("tempdir").path().join("people.csv"))
            .expect("absolute input path");

    let output = default_output_csv_path(&path, "12345678abcdef00");

    assert_eq!(
        output.as_path().file_name().and_then(|name| name.to_str()),
        Some("people.agent-job-12345678.csv")
    );
}

#[test]
fn render_job_csv_includes_job_metadata_columns() {
    let now = Utc::now();
    let csv = render_job_csv(
        &["name".to_string()],
        &[codex_state::AgentJobItem {
            job_id: "job-1".to_string(),
            item_id: "item-1".to_string(),
            row_index: 0,
            source_id: Some("src-1".to_string()),
            row_json: json!({"name": "Ada", "nested": {"ok": true}}),
            status: codex_state::AgentJobItemStatus::Completed,
            assigned_thread_id: Some("thread-1".to_string()),
            attempt_count: 2,
            result_json: Some(json!({"score": 9, "notes": ["ok"]})),
            last_error: None,
            created_at: now,
            updated_at: now,
            completed_at: Some(now),
            reported_at: Some(now),
        }],
    )
    .expect("csv should render");

    let (headers, rows) = parse_csv(&csv).expect("rendered csv should parse");
    assert_eq!(
        headers,
        vec![
            "name",
            "job_id",
            "item_id",
            "row_index",
            "source_id",
            "status",
            "attempt_count",
            "last_error",
            "result_json",
            "reported_at",
            "completed_at",
        ]
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        vec![
            "Ada".to_string(),
            "job-1".to_string(),
            "item-1".to_string(),
            "0".to_string(),
            "src-1".to_string(),
            "completed".to_string(),
            "2".to_string(),
            String::new(),
            "{\"notes\":[\"ok\"],\"score\":9}".to_string(),
            now.to_rfc3339(),
            now.to_rfc3339(),
        ]
    );
}

#[test]
fn render_job_csv_rejects_non_object_rows() {
    let now = Utc::now();
    let err = render_job_csv(
        &["name".to_string()],
        &[codex_state::AgentJobItem {
            job_id: "job-1".to_string(),
            item_id: "item-1".to_string(),
            row_index: 0,
            source_id: None,
            row_json: json!(["bad"]),
            status: codex_state::AgentJobItemStatus::Pending,
            assigned_thread_id: None,
            attempt_count: 0,
            result_json: None,
            last_error: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
            reported_at: None,
        }],
    )
    .expect_err("non-object row_json should fail");

    assert!(matches!(
        err,
        FunctionCallError::RespondToModel(message)
            if message == "row_json for item item-1 is not a JSON object"
    ));
}

#[test]
fn job_runtime_timeout_uses_override_or_default() {
    let now = Utc::now();
    let job = codex_state::AgentJob {
        id: "job-1".to_string(),
        name: "job".to_string(),
        status: codex_state::AgentJobStatus::Pending,
        instruction: "do work".to_string(),
        auto_export: true,
        max_runtime_seconds: Some(7),
        output_schema_json: None,
        input_headers: vec!["name".to_string()],
        input_csv_path: "/tmp/in.csv".to_string(),
        output_csv_path: "/tmp/out.csv".to_string(),
        created_at: now,
        updated_at: now,
        started_at: None,
        completed_at: None,
        last_error: None,
    };
    assert_eq!(job_runtime_timeout(&job), Duration::from_secs(7));

    let job = codex_state::AgentJob {
        max_runtime_seconds: None,
        ..job
    };
    assert_eq!(job_runtime_timeout(&job), DEFAULT_AGENT_JOB_ITEM_TIMEOUT);
}
