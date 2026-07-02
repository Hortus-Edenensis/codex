use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;

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
