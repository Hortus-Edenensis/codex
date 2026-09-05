use chrono::TimeZone;
use chrono::Utc;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionSource;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use uuid::Uuid;

use super::search_threads;
use crate::SearchThreadsParams;
use crate::SortDirection;
use crate::ThreadSortKey;
use crate::ThreadStoreError;
use crate::local::LocalThreadStore;
use crate::local::test_support::test_config;

fn search_params() -> SearchThreadsParams {
    SearchThreadsParams {
        page_size: 2,
        cursor: None,
        sort_key: ThreadSortKey::RecencyAt,
        sort_direction: SortDirection::Desc,
        allowed_sources: vec![SessionSource::Cli],
        archived: false,
        search_term: "needle".to_string(),
    }
}

#[tokio::test]
async fn concurrent_searches_page_metadata_without_rollout_or_name_index_repair() {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let state = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("initialize state");
    let store = LocalThreadStore::new(config.clone(), Some(state.clone()));
    let timestamp = Utc.with_ymd_and_hms(2026, 1, 27, 12, 34, 56).unwrap();
    let mut ids = Vec::new();
    for index in 0..5 {
        let thread_id = ThreadId::from_string(&Uuid::from_u128(123 + index).to_string())
            .expect("valid thread id");
        ids.push(thread_id);
        let rollout_path = home.path().join(format!("rollout-{thread_id}.jsonl"));
        // Metadata remains searchable even when the rollout contains no messages.
        std::fs::write(&rollout_path, "").expect("placeholder rollout");
        let mut builder = codex_state::ThreadMetadataBuilder::new(
            thread_id,
            rollout_path,
            timestamp,
            if index == 3 {
                SessionSource::Exec
            } else {
                SessionSource::Cli
            },
        );
        builder.cwd = home.path().to_path_buf();
        let mut metadata = builder.build(&config.default_model_provider_id);
        metadata.title = "plain preview".to_string();
        metadata.first_user_message = Some("plain preview".to_string());
        metadata.preview = Some("plain preview".to_string());
        match index {
            0 => metadata.name = Some("needle name".to_string()),
            1 => metadata.title = "needle title".to_string(),
            _ => metadata.preview = Some("needle preview".to_string()),
        }
        if index == 4 {
            metadata.archived_at = Some(timestamp);
        }
        state
            .upsert_thread(&metadata)
            .await
            .expect("insert metadata");
        codex_rollout::append_thread_name(home.path(), thread_id, "stale index name")
            .await
            .expect("append legacy index name");
    }
    let pages =
        futures::future::join_all((0..4).map(|_| search_threads(&store, search_params()))).await;
    for page in pages {
        let page = page.expect("metadata search");
        assert_eq!(
            page.items
                .iter()
                .map(|item| (
                    item.thread.thread_id,
                    item.thread.name.as_deref(),
                    item.snippet.as_str()
                ))
                .collect::<Vec<_>>(),
            vec![
                (ids[2], None, "needle preview"),
                (ids[1], Some("needle title"), "plain preview"),
            ]
        );
        let cursor = page.next_cursor.expect("next metadata page");
        assert!(cursor.ends_with(&ids[1].to_string()));
        let last_page = search_threads(
            &store,
            SearchThreadsParams {
                cursor: Some(cursor),
                ..search_params()
            },
        )
        .await
        .expect("next metadata page");
        assert_eq!(
            last_page
                .items
                .iter()
                .map(|item| (
                    item.thread.thread_id,
                    item.thread.name.as_deref(),
                    item.snippet.as_str()
                ))
                .collect::<Vec<_>>(),
            vec![(ids[0], None, "plain preview")]
        );
        assert_eq!(last_page.next_cursor, None);
    }
    for (params, expected) in [
        (
            SearchThreadsParams {
                archived: true,
                ..search_params()
            },
            ids[4],
        ),
        (
            SearchThreadsParams {
                allowed_sources: vec![SessionSource::Exec],
                ..search_params()
            },
            ids[3],
        ),
    ] {
        let page = search_threads(&store, params)
            .await
            .expect("filtered search");
        assert_eq!(
            page.items
                .iter()
                .map(|item| item.thread.thread_id)
                .collect::<Vec<_>>(),
            vec![expected]
        );
        assert_eq!(page.next_cursor, None);
    }
}

#[tokio::test]
async fn search_requires_state_database() {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let state = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("initialize state");
    state.close().await;
    for state_db in [None, Some(state)] {
        let store = LocalThreadStore::new(config.clone(), state_db);
        let error = search_threads(&store, search_params())
            .await
            .expect_err("unavailable state database must not become an empty search result");
        assert!(
            matches!(error, ThreadStoreError::Internal { message } if message.contains("state DB unavailable"))
        );
    }
}

#[tokio::test]
async fn search_rejects_invalid_parameters() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    for params in [
        SearchThreadsParams {
            search_term: String::new(),
            ..search_params()
        },
        SearchThreadsParams {
            page_size: 0,
            ..search_params()
        },
        SearchThreadsParams {
            cursor: Some("invalid".to_string()),
            ..search_params()
        },
    ] {
        assert!(matches!(
            search_threads(&store, params).await,
            Err(ThreadStoreError::InvalidRequest { .. })
        ));
    }
}
