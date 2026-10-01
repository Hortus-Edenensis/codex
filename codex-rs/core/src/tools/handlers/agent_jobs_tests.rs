use super::*;
use pretty_assertions::assert_eq;
use serde_json::json;

#[test]
fn parse_csv_supports_quotes_and_commas() {
    let input = "id,name\n1,\"alpha, beta\"\n2,gamma\n";
    let (headers, rows) = parse_csv(input).expect("csv parse");
    assert_eq!(headers, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "alpha, beta".to_string()],
            vec!["2".to_string(), "gamma".to_string()]
        ]
    );
}

#[test]
fn csv_escape_quotes_when_needed() {
    assert_eq!(csv_escape("simple"), "simple");
    assert_eq!(csv_escape("a,b"), "\"a,b\"");
    assert_eq!(csv_escape("a\"b"), "\"a\"\"b\"");
}

#[test]
fn render_instruction_template_expands_placeholders_and_escapes_braces() {
    let row = json!({
        "path": "src/lib.rs",
        "area": "test",
        "file path": "docs/readme.md",
    });
    let rendered = render_instruction_template(
        "Review {path} in {area}. Also see {file path}. Use {{literal}}.",
        &row,
    );
    assert_eq!(
        rendered,
        "Review src/lib.rs in test. Also see docs/readme.md. Use {literal}."
    );
}

#[test]
fn render_instruction_template_leaves_unknown_placeholders() {
    let row = json!({
        "path": "src/lib.rs",
    });
    let rendered = render_instruction_template("Check {path} then {missing}", &row);
    assert_eq!(rendered, "Check src/lib.rs then {missing}");
}

#[test]
fn ensure_unique_headers_rejects_duplicates() {
    let headers = vec!["path".to_string(), "path".to_string()];
    let Err(err) = ensure_unique_headers(headers.as_slice()) else {
        panic!("expected duplicate header error");
    };
    assert_eq!(
        err,
        FunctionCallError::RespondToModel("csv header path is duplicated".to_string())
    );
}

#[tokio::test]
async fn completed_worker_snapshot_survives_closed_status_stream() {
    let (session, _) = crate::session::tests::make_session_and_context().await;
    let thread_id = ThreadId::new();
    let final_status = AgentStatus::Completed(Some("done".to_string()));
    let info = AgentInfo::Loaded {
        agent: crate::agent::types::LiveAgent {
            thread_id,
            metadata: Default::default(),
            status: final_status.clone(),
        },
        config: Box::new(session.thread_config_snapshot().await),
    };
    let mut active = HashMap::from([(
        thread_id,
        ActiveJobItem {
            item_id: "row-1".to_string(),
            started_at: Instant::now(),
            status_rx: Some(futures::stream::once(async move { Ok(info) }).boxed()),
            last_status: None,
        },
    )]);
    wait_for_status_change(&mut active).await;
    wait_for_status_change(&mut active).await;
    let item = active.get_mut(&thread_id).expect("worker item");
    assert!(item.status_rx.is_none());
    assert_eq!(
        active_item_status(&session, thread_id, item).await,
        final_status
    );
}

#[tokio::test]
async fn drained_worker_status_stream_is_not_polled_again() {
    let (session, _) = crate::session::tests::make_session_and_context().await;
    let thread_id = ThreadId::new();
    let final_status = AgentStatus::Completed(Some("done".to_string()));
    for failed in [false, true] {
        let mut polled = false;
        let status_rx = futures::stream::poll_fn(move |_| {
            assert!(!polled, "closed status stream was polled again");
            polled = true;
            std::task::Poll::Ready(failed.then(|| {
                Err(codex_protocol::error::CodexErr::Fatal(
                    "status stream failed".to_string(),
                ))
            }))
        })
        .boxed();
        let mut active = HashMap::from([(
            thread_id,
            ActiveJobItem {
                item_id: "row-1".to_string(),
                started_at: Instant::now(),
                status_rx: Some(status_rx),
                last_status: Some(final_status.clone()),
            },
        )]);
        assert_eq!(
            active_item_status(
                &session,
                thread_id,
                active.get_mut(&thread_id).expect("worker item")
            )
            .await,
            final_status,
        );
        wait_for_status_change(&mut active).await;
        assert!(active[&thread_id].status_rx.is_none());
        assert_eq!(active[&thread_id].last_status, Some(final_status.clone()));
    }
}
