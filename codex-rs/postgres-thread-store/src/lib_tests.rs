use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;

use super::*;

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
