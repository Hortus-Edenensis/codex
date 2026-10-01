use std::time::Duration;

use chrono::Utc;
use codex_protocol::models::PermissionProfile;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use sqlx::migrate::AppliedMigration;
use sqlx::migrate::Migrate;
use sqlx::migrate::MigrateError;
use sqlx::migrate::Migration;

use super::*;

#[test]
fn remote_control_app_server_client_name_key_uses_empty_string_for_none() {
    assert_eq!(remote_control_app_server_client_name_key(None), "");
    assert_eq!(
        remote_control_app_server_client_name_key(Some("desktop-client")),
        "desktop-client"
    );
}

#[test]
fn app_server_client_name_from_key_restores_none_for_empty_string() {
    assert_eq!(app_server_client_name_from_key(String::new()), None);
    assert_eq!(
        app_server_client_name_from_key("desktop-client".to_string()),
        Some("desktop-client".to_string())
    );
}

#[test]
fn canonical_session_source_key_uses_serde_values() {
    assert_eq!(
        canonical_session_source_key(&SessionSource::VSCode).expect("vscode key"),
        "vscode"
    );
    assert_eq!(
        canonical_session_source_key(&SessionSource::Cli).expect("cli key"),
        "cli"
    );
    assert_eq!(
        canonical_session_source_key(&SessionSource::Exec).expect("exec key"),
        "exec"
    );
}

#[test]
fn canonical_session_source_key_serializes_structured_sources_as_json() {
    let source = SessionSource::SubAgent(SubAgentSource::Other("guardian".to_string()));

    assert_eq!(
        canonical_session_source_key(&source).expect("subagent key"),
        r#"{"subagent":{"other":"guardian"}}"#
    );
}

#[test]
fn session_source_filter_keys_include_legacy_debug_values() {
    assert_eq!(
        session_source_filter_keys(&SessionSource::VSCode).expect("vscode filter keys"),
        vec!["vscode".to_string(), "VSCode".to_string()]
    );
}

#[test]
fn session_source_filter_keys_include_custom_display_fallback() {
    assert_eq!(
        session_source_filter_keys(&SessionSource::Custom("atlas".to_string()))
            .expect("custom filter keys"),
        vec![
            r#"{"custom":"atlas"}"#.to_string(),
            r#"Custom("atlas")"#.to_string(),
            "atlas".to_string(),
            r#"{"custom": "atlas"}"#.to_string(),
        ]
    );
}

#[test]
fn stored_thread_json_with_memory_mode_sets_canonical_field() {
    let value = stored_thread_json_with_memory_mode_key(
        &sample_stored_thread(),
        thread_memory_mode_key(ThreadMemoryMode::Disabled),
    )
    .expect("stored thread JSON");

    assert_eq!(value["memory_mode"], "disabled");
}

#[test]
fn stored_thread_json_preserves_model_metadata() {
    let stored = StoredThread {
        model: Some("gpt-5.5".to_string()),
        reasoning_effort: Some(ReasoningEffort::XHigh),
        ..sample_stored_thread()
    };

    let value = stored_thread_json_with_memory_mode_key(
        &stored,
        thread_memory_mode_key(ThreadMemoryMode::Enabled),
    )
    .expect("stored thread JSON");

    assert_eq!(value["model"], "gpt-5.5");
    assert_eq!(value["reasoning_effort"], "xhigh");
}

#[test]
fn thread_history_mode_key_uses_canonical_lowercase_values() {
    assert_eq!(thread_history_mode_key(ThreadHistoryMode::Legacy), "legacy");
    assert_eq!(
        thread_history_mode_key(ThreadHistoryMode::Paginated),
        "paginated"
    );
}

#[test]
fn stored_thread_memory_mode_from_value_reads_known_values() {
    let value = serde_json::json!({ "memory_mode": "enabled" });

    assert_eq!(
        stored_thread_memory_mode_key_from_value(&value).expect("parse memory mode"),
        Some("enabled".to_string())
    );
}

#[test]
fn stored_thread_memory_mode_from_value_preserves_unknown_values() {
    let value = serde_json::json!({ "memory_mode": "polluted" });

    assert_eq!(
        stored_thread_memory_mode_key_from_value(&value).expect("preserve memory mode"),
        Some("polluted".to_string())
    );
}

#[test]
fn locking_disabled_migrator_preserves_configuration() {
    let migrator = locking_disabled_migrator(&MIGRATOR);

    assert!(!migrator.locking);
    assert_eq!(migrator.migrations, MIGRATOR.migrations);
    assert_eq!(migrator.ignore_missing, MIGRATOR.ignore_missing);
    assert_eq!(migrator.no_tx, MIGRATOR.no_tx);
    assert_eq!(migrator.table_name, MIGRATOR.table_name);
    assert_eq!(migrator.create_schemas, MIGRATOR.create_schemas);
}

#[tokio::test]
async fn run_migrations_with_explicit_lock_releases_lock_after_success() {
    let migrator = Migrator {
        locking: false,
        ..Migrator::DEFAULT
    };
    let mut conn = FakeMigrate::default();

    run_migrations_with_explicit_lock(&migrator, &mut conn)
        .await
        .expect("migration should succeed");

    assert_eq!(conn.lock_calls, 1);
    assert_eq!(conn.unlock_calls, 1);
}

#[tokio::test]
async fn run_migrations_with_explicit_lock_releases_lock_after_migration_error() {
    let migrator = Migrator {
        locking: false,
        ..Migrator::DEFAULT
    };
    let mut conn = FakeMigrate {
        dirty_version: Some(7),
        ..FakeMigrate::default()
    };

    let error = run_migrations_with_explicit_lock(&migrator, &mut conn)
        .await
        .expect_err("dirty migration should fail");

    assert!(matches!(
        error,
        ThreadStoreError::Internal { message }
            if message.contains("migration 7 is partially applied")
    ));
    assert_eq!(conn.lock_calls, 1);
    assert_eq!(conn.unlock_calls, 1);
}

#[tokio::test]
async fn unconfigured_store_rejects_remote_control_persistence_requests() {
    let store = PostgresThreadStore::unconfigured("missing database url".to_string());

    let error = store
        .get_remote_control_enrollment("wss://example.com", "account", None)
        .await
        .expect_err("unconfigured store should reject reads");
    assert!(matches!(
        error,
        ThreadStoreError::InvalidRequest { message } if message == "missing database url"
    ));

    let error = store
        .upsert_remote_control_enrollment(&RemoteControlEnrollmentRecord {
            websocket_url: "wss://example.com".to_string(),
            account_id: "account".to_string(),
            app_server_client_name: None,
            server_id: "server".to_string(),
            environment_id: "environment".to_string(),
            server_name: "server-name".to_string(),
            remote_control_enabled: Some(true),
        })
        .await
        .expect_err("unconfigured store should reject writes");
    assert!(matches!(
        error,
        ThreadStoreError::InvalidRequest { message } if message == "missing database url"
    ));
}

#[test]
fn postgres_store_defaults_to_paginated_history() {
    let store = PostgresThreadStore::unconfigured("missing database url".to_string());

    assert_eq!(
        ThreadStore::default_history_mode(&store),
        ThreadHistoryMode::Paginated
    );
}

#[derive(Default)]
struct FakeMigrate {
    dirty_version: Option<i64>,
    lock_calls: usize,
    unlock_calls: usize,
}

type TestFuture<'e, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'e>>;

impl Migrate for FakeMigrate {
    fn create_schema_if_not_exists<'e>(
        &'e mut self,
        _schema_name: &'e str,
    ) -> TestFuture<'e, Result<(), MigrateError>> {
        Box::pin(async { Ok(()) })
    }

    fn ensure_migrations_table<'e>(
        &'e mut self,
        _table_name: &'e str,
    ) -> TestFuture<'e, Result<(), MigrateError>> {
        Box::pin(async { Ok(()) })
    }

    fn dirty_version<'e>(
        &'e mut self,
        _table_name: &'e str,
    ) -> TestFuture<'e, Result<Option<i64>, MigrateError>> {
        Box::pin(async move { Ok(self.dirty_version) })
    }

    fn list_applied_migrations<'e>(
        &'e mut self,
        _table_name: &'e str,
    ) -> TestFuture<'e, Result<Vec<AppliedMigration>, MigrateError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn lock(&mut self) -> TestFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move {
            self.lock_calls += 1;
            Ok(())
        })
    }

    fn unlock(&mut self) -> TestFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move {
            self.unlock_calls += 1;
            Ok(())
        })
    }

    fn apply<'e>(
        &'e mut self,
        _table_name: &'e str,
        _migration: &'e Migration,
    ) -> TestFuture<'e, Result<Duration, MigrateError>> {
        Box::pin(async { panic!("apply should not run for empty migrator") })
    }

    fn revert<'e>(
        &'e mut self,
        _table_name: &'e str,
        _migration: &'e Migration,
    ) -> TestFuture<'e, Result<Duration, MigrateError>> {
        Box::pin(async { panic!("revert should not run for empty migrator") })
    }
}

fn sample_stored_thread() -> StoredThread {
    let now = Utc::now();
    StoredThread {
        originator: None,
        section: None,
        section_position: None,
        section_entered_at: None,
        project_id: None,
        daybreak_enabled: None,
        thread_id: ThreadId::default(),
        extra_config: None,
        rollout_path: None,
        forked_from_id: None,
        parent_thread_id: None,
        preview: "preview".to_string(),
        name: Some("name".to_string()),
        model_provider: "openai".to_string(),
        model: None,
        reasoning_effort: None,
        created_at: now,
        updated_at: now,
        recency_at: now,
        archived_at: None,
        cwd: std::path::PathBuf::from("/tmp"),
        cli_version: "test".to_string(),
        source: SessionSource::Cli,
        history_mode: ThreadHistoryMode::Legacy,
        thread_source: None,
        agent_nickname: None,
        agent_role: None,
        agent_path: None,
        git_info: None,
        approval_mode: AskForApproval::OnRequest,
        permission_profile: PermissionProfile::read_only(),
        token_usage: None,
        first_user_message: None,
        history: None,
    }
}

#[test]
fn metadata_patch_preserves_omissions_and_honors_cleared_reasoning() {
    let mut stored = sample_stored_thread();
    stored.reasoning_effort = Some(ReasoningEffort::XHigh);
    apply_metadata_patch(
        &mut stored,
        codex_thread_store::ThreadMetadataPatch::default(),
    );
    assert_eq!(stored.reasoning_effort, Some(ReasoningEffort::XHigh));
    apply_metadata_patch(
        &mut stored,
        codex_thread_store::ThreadMetadataPatch {
            reasoning_effort: Some(None),
            project_id: Some(Some("project".to_string())),
            daybreak_enabled: Some(false),
            ..Default::default()
        },
    );
    assert_eq!(stored.reasoning_effort, None);
    assert_eq!(stored.project_id.as_deref(), Some("project"));
    assert_eq!(stored.daybreak_enabled, Some(false));
}

#[tokio::test]
async fn versioned_store_clones_share_pending_metadata() {
    let store = PostgresThreadStore::unconfigured("test".to_string());
    let v2 = store.with_memory_version(codex_protocol::MemoryVersion::V2);
    let thread_id = ThreadId::default();
    store
        .stage_pending_thread_metadata(
            thread_id,
            codex_thread_store::ThreadMetadataPatch {
                name: Some(Some("pending".to_string())),
                ..Default::default()
            },
        )
        .await
        .expect("stage metadata");
    assert_eq!(
        v2.read_pending_thread_metadata(thread_id)
            .await
            .expect("read metadata")
            .expect("staged metadata")
            .name,
        Some(Some("pending".to_string()))
    );
    v2.remove_pending_thread_metadata(thread_id)
        .await
        .expect("remove metadata");
    assert!(
        store
            .read_pending_thread_metadata(thread_id)
            .await
            .expect("read removed metadata")
            .is_none()
    );
}

#[test]
fn historical_thread_metadata_and_rollout_remain_readable() {
    let mut legacy = serde_json::to_value(sample_stored_thread()).expect("serialize metadata");
    for field in [
        "originator",
        "section",
        "section_position",
        "section_entered_at",
        "project_id",
        "daybreak_enabled",
    ] {
        legacy
            .as_object_mut()
            .expect("metadata object")
            .remove(field);
    }
    let stored: StoredThread = serde_json::from_value(legacy).expect("read pre-versioned metadata");
    assert_eq!(stored.originator, None);
    assert_eq!(stored.project_id, None);
    let item: RolloutItem = serde_json::from_value(json!({
        "type": "response_item",
        "payload": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "historical input"}]}
    })).expect("read historical PostgreSQL rollout JSON");
    assert!(matches!(item, RolloutItem::ResponseItem(_)));
}

#[test]
fn canonical_session_meta_retains_creation_settings() {
    let thread_id = ThreadId::default();
    let params = CreateThreadParams {
        creator_user_id: Some("creator".to_string()),
        creator_account_id: Some("account".to_string()),
        session_id: thread_id.into(),
        thread_id,
        extra_config: None,
        forked_from_id: None,
        parent_thread_id: None,
        source: SessionSource::Exec,
        thread_source: None,
        originator: "test".to_string(),
        base_instructions: codex_protocol::models::BaseInstructions { text: "original instructions".to_string(), provenance: None },
        dynamic_tools: serde_json::from_value(json!([{"type":"function", "name":"test", "description":"test", "inputSchema":{"type":"object"}}])).expect("dynamic tool"),
        selected_capability_roots: serde_json::from_value(json!([{"id":"root", "location":{"type":"environment","environmentId":"local","path":"/workspace"}}])).expect("capability root"),
        multi_agent_version: Some(codex_protocol::protocol::MultiAgentVersion::V2),
        history_mode: ThreadHistoryMode::Paginated,
        history_base: None,
        subagent_history_start_ordinal: Some(7),
        initial_window_id: "initial-window".to_string(),
        runtime_workspace_roots: Some(serde_json::from_value(json!(["/workspace"])).expect("workspace root")),
        metadata: codex_thread_store::ThreadPersistenceMetadata {
            cwd: Some("/workspace".into()),
            model_provider: "test".to_string(),
            model: None,
            reasoning_effort: None,
            memory_mode: ThreadMemoryMode::Disabled,
        },
    };
    let metadata = session_meta_for_create(&params, Utc::now());
    assert_eq!(metadata.id, params.thread_id);
    assert_eq!(metadata.creator_user_id, params.creator_user_id);
    assert_eq!(metadata.creator_account_id, params.creator_account_id);
    assert_eq!(
        metadata
            .base_instructions
            .as_ref()
            .map(|value| value.text.as_str()),
        Some("original instructions")
    );
    assert_eq!(metadata.dynamic_tools, Some(params.dynamic_tools));
    assert_eq!(
        metadata.selected_capability_roots,
        params.selected_capability_roots
    );
    assert_eq!(
        metadata.runtime_workspace_roots,
        Some(vec![std::path::PathBuf::from("/workspace")])
    );
    assert_eq!(metadata.subagent_history_start_ordinal, Some(7));
    assert_eq!(
        metadata.multi_agent_version,
        Some(codex_protocol::protocol::MultiAgentVersion::V2)
    );
    assert_eq!(
        metadata.context_window.expect("context window").window_id,
        "initial-window"
    );
    assert_eq!(metadata.memory_mode.as_deref(), Some("disabled"));
}

#[test]
fn legacy_history_gets_own_identity_without_inheriting_parent_settings() {
    let stored = sample_stored_thread();
    let ancestor = RolloutItem::SessionMeta(SessionMetaLine {
        meta: SessionMeta {
            id: ThreadId::default(),
            base_instructions: Some(codex_protocol::models::BaseInstructions {
                text: "parent-only".to_string(),
                provenance: None,
            }),
            ..Default::default()
        },
        git: None,
    });
    let original = serde_json::to_value(&ancestor).expect("original ancestor");
    let mut items = vec![ancestor];
    ensure_canonical_session_meta(&stored, stored.thread_id.into(), "enabled", &mut items);
    let RolloutItem::SessionMeta(first) = &items[0] else {
        panic!("canonical metadata missing")
    };
    assert_eq!(first.meta.id, stored.thread_id);
    assert_eq!(first.meta.base_instructions, None);
    assert_eq!(first.meta.dynamic_tools, None);
    assert_eq!(first.meta.multi_agent_version, None);
    assert_eq!(
        serde_json::to_value(&items[1]).expect("ancestor retained"),
        original
    );
    ensure_canonical_session_meta(&stored, stored.thread_id.into(), "enabled", &mut items);
    assert_eq!(items.len(), 2);
}

#[test]
fn existing_own_metadata_is_preserved_when_ancestor_appears_first() {
    let stored = sample_stored_thread();
    let mut items = vec![
        RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta::default(),
            git: None,
        }),
        RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                id: stored.thread_id,
                originator: "original-child".to_string(),
                ..Default::default()
            },
            git: None,
        }),
    ];
    let original = serde_json::to_value(&items).expect("original history");
    ensure_canonical_session_meta(&stored, stored.thread_id.into(), "enabled", &mut items);
    assert_eq!(
        serde_json::to_value(&items[1..]).expect("retained history"),
        original
    );
    assert_eq!(
        serde_json::to_value(&items[0]).expect("canonical child"),
        original[1]
    );
}
