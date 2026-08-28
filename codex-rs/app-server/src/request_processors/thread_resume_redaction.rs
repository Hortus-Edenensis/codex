use codex_app_server_protocol::McpToolCallResult;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::Turn;
use serde_json::Value as JsonValue;

// Temporary bandaid for remote clients: thread/resume can include large MCP and
// image-generation payloads. Keep this response-only so persisted rollout
// history, model resume history, and other APIs stay unchanged.
const REDACTED_PAYLOAD: &str = "[redacted]";
const MAX_INLINE_PERSISTED_IMAGE_RESULT_BYTES: usize = 64 * 1024;
const CHATGPT_REMOTE_CLIENT_NAMES: &[&str] =
    &["codex_chatgpt_android_remote", "codex_chatgpt_ios_remote"];

pub(super) fn should_redact_thread_resume_payloads(client_name: Option<&str>) -> bool {
    client_name.is_some_and(|client_name| CHATGPT_REMOTE_CLIENT_NAMES.contains(&client_name))
}

pub(super) fn redact_thread_resume_payloads(turns: &mut [Turn]) {
    for turn in turns {
        turn.items.retain_mut(|item| match item {
            ThreadItem::McpToolCall {
                arguments,
                result,
                error,
                ..
            } => {
                *arguments = JsonValue::String(REDACTED_PAYLOAD.to_string());
                if result.is_some() {
                    *result = Some(Box::new(redacted_mcp_tool_call_result()));
                }
                if let Some(error) = error {
                    error.message = REDACTED_PAYLOAD.to_string();
                }
                true
            }
            ThreadItem::ImageGeneration(_) => false,
            _ => true,
        });
    }
}

// Persisted image-generation items can contain multi-megabyte base64 payloads.
// Once the image has a durable path, keep the item metadata but avoid sending
// the duplicate inline bytes every time a client opens thread history.
pub(super) fn compact_thread_history_image_payloads(turns: &mut [Turn]) {
    for turn in turns {
        for item in &mut turn.items {
            let ThreadItem::ImageGeneration(image) = item else {
                continue;
            };
            if image.saved_path.is_some()
                && image.result.len() > MAX_INLINE_PERSISTED_IMAGE_RESULT_BYTES
            {
                image.result.clear();
            }
        }
    }
}

fn redacted_mcp_tool_call_result() -> McpToolCallResult {
    McpToolCallResult {
        content: vec![serde_json::json!({
            "type": "text",
            "text": REDACTED_PAYLOAD,
        })],
        structured_content: None,
        meta: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_app_server_protocol::ImageGenerationItem;
    use codex_app_server_protocol::McpToolCallAppContext;
    use codex_app_server_protocol::McpToolCallError;
    use codex_app_server_protocol::McpToolCallStatus;
    use codex_app_server_protocol::SessionSource;
    use codex_app_server_protocol::Thread;
    use codex_app_server_protocol::ThreadStatus;
    use codex_app_server_protocol::TurnItemsView;
    use codex_app_server_protocol::TurnStatus;
    use codex_utils_absolute_path::test_support::PathBufExt;
    use codex_utils_absolute_path::test_support::test_path_buf;
    use pretty_assertions::assert_eq;

    #[test]
    fn redacts_mcp_success_result_and_removes_image_generation() {
        let mut thread = test_thread(vec![
            ThreadItem::AgentMessage {
                id: "agent-1".to_string(),
                text: "kept".to_string(),
                phase: None,
                memory_citation: None,
                delivery: None,
            },
            ThreadItem::McpToolCall {
                id: "mcp-1".to_string(),
                server: "docs".to_string(),
                tool: "lookup".to_string(),
                status: McpToolCallStatus::Completed,
                arguments: serde_json::json!({"secret":"argument"}),
                app_context: Some(McpToolCallAppContext {
                    connector_id: "calendar".to_string(),
                    link_id: Some("link_calendar".to_string()),
                    resource_uri: Some("ui://widget/lookup.html".to_string()),
                    app_name: Some("Calendar".to_string()),
                    action_name: Some("lookup".to_string()),
                }),
                mcp_app_resource_uri: Some("ui://widget/lookup.html".to_string()),
                plugin_id: Some("sample@test".to_string()),
                read_only_hint: None,
                result: Some(Box::new(McpToolCallResult {
                    content: vec![serde_json::json!({
                        "type": "text",
                        "text": "secret result"
                    })],
                    structured_content: Some(serde_json::json!({"secret":"structured"})),
                    meta: Some(serde_json::json!({"secret":"meta"})),
                })),
                error: None,
                duration_ms: Some(8),
            },
            ThreadItem::ImageGeneration(ImageGenerationItem {
                id: "ig-1".to_string(),
                status: "completed".to_string(),
                revised_prompt: Some("revised".to_string()),
                result: "base64-result".to_string(),
                transparent_background: None,
                failure: None,
                saved_path: Some(test_path_buf("/tmp/ig-1.png").abs()),
                imagegen_request_id: None,
            }),
        ]);

        redact_thread_resume_payloads(&mut thread.turns);

        assert_eq!(thread.turns[0].items.len(), 2);
        assert_eq!(
            thread.turns[0].items[0],
            ThreadItem::AgentMessage {
                id: "agent-1".to_string(),
                text: "kept".to_string(),
                phase: None,
                memory_citation: None,
                delivery: None,
            }
        );
        assert_eq!(
            thread.turns[0].items[1],
            ThreadItem::McpToolCall {
                id: "mcp-1".to_string(),
                server: "docs".to_string(),
                tool: "lookup".to_string(),
                status: McpToolCallStatus::Completed,
                arguments: JsonValue::String(REDACTED_PAYLOAD.to_string()),
                app_context: Some(McpToolCallAppContext {
                    connector_id: "calendar".to_string(),
                    link_id: Some("link_calendar".to_string()),
                    resource_uri: Some("ui://widget/lookup.html".to_string()),
                    app_name: Some("Calendar".to_string()),
                    action_name: Some("lookup".to_string()),
                }),
                mcp_app_resource_uri: Some("ui://widget/lookup.html".to_string()),
                plugin_id: Some("sample@test".to_string()),
                read_only_hint: None,
                result: Some(Box::new(redacted_mcp_tool_call_result())),
                error: None,
                duration_ms: Some(8),
            }
        );
    }

    #[test]
    fn compacts_only_large_persisted_image_results() {
        let large_result = "x".repeat(MAX_INLINE_PERSISTED_IMAGE_RESULT_BYTES + 1);
        let small_result = "small-result".to_string();
        let unsaved_result = "y".repeat(MAX_INLINE_PERSISTED_IMAGE_RESULT_BYTES + 1);
        let persisted_path = test_path_buf("/tmp/generated.png").abs();
        let mut thread = test_thread(vec![
            ThreadItem::ImageGeneration(ImageGenerationItem {
                id: "large-persisted".to_string(),
                status: "completed".to_string(),
                revised_prompt: Some("large".to_string()),
                result: large_result,
                transparent_background: None,
                failure: None,
                saved_path: Some(persisted_path.clone()),
                imagegen_request_id: None,
            }),
            ThreadItem::ImageGeneration(ImageGenerationItem {
                id: "small-persisted".to_string(),
                status: "completed".to_string(),
                revised_prompt: Some("small".to_string()),
                result: small_result.clone(),
                transparent_background: None,
                failure: None,
                saved_path: Some(persisted_path),
                imagegen_request_id: None,
            }),
            ThreadItem::ImageGeneration(ImageGenerationItem {
                id: "large-unsaved".to_string(),
                status: "completed".to_string(),
                revised_prompt: Some("unsaved".to_string()),
                result: unsaved_result.clone(),
                transparent_background: None,
                failure: None,
                saved_path: None,
                imagegen_request_id: None,
            }),
        ]);

        compact_thread_history_image_payloads(&mut thread.turns);

        let expected = test_thread(vec![
            ThreadItem::ImageGeneration(ImageGenerationItem {
                id: "large-persisted".to_string(),
                status: "completed".to_string(),
                revised_prompt: Some("large".to_string()),
                result: String::new(),
                transparent_background: None,
                failure: None,
                saved_path: Some(test_path_buf("/tmp/generated.png").abs()),
                imagegen_request_id: None,
            }),
            ThreadItem::ImageGeneration(ImageGenerationItem {
                id: "small-persisted".to_string(),
                status: "completed".to_string(),
                revised_prompt: Some("small".to_string()),
                result: small_result,
                transparent_background: None,
                failure: None,
                saved_path: Some(test_path_buf("/tmp/generated.png").abs()),
                imagegen_request_id: None,
            }),
            ThreadItem::ImageGeneration(ImageGenerationItem {
                id: "large-unsaved".to_string(),
                status: "completed".to_string(),
                revised_prompt: Some("unsaved".to_string()),
                result: unsaved_result,
                transparent_background: None,
                failure: None,
                saved_path: None,
                imagegen_request_id: None,
            }),
        ]);
        assert_eq!(thread, expected);
    }

    #[test]
    fn redacts_mcp_error_message() {
        let mut thread = test_thread(vec![ThreadItem::McpToolCall {
            id: "mcp-1".to_string(),
            server: "docs".to_string(),
            tool: "lookup".to_string(),
            status: McpToolCallStatus::Failed,
            arguments: serde_json::json!({"secret":"argument"}),
            app_context: None,
            mcp_app_resource_uri: None,
            plugin_id: None,
            read_only_hint: None,
            result: None,
            error: Some(McpToolCallError {
                message: "secret error".to_string(),
            }),
            duration_ms: Some(8),
        }]);

        redact_thread_resume_payloads(&mut thread.turns);

        assert_eq!(
            thread.turns[0].items[0],
            ThreadItem::McpToolCall {
                id: "mcp-1".to_string(),
                server: "docs".to_string(),
                tool: "lookup".to_string(),
                status: McpToolCallStatus::Failed,
                arguments: JsonValue::String(REDACTED_PAYLOAD.to_string()),
                app_context: None,
                mcp_app_resource_uri: None,
                plugin_id: None,
                read_only_hint: None,
                result: None,
                error: Some(McpToolCallError {
                    message: REDACTED_PAYLOAD.to_string(),
                }),
                duration_ms: Some(8),
            }
        );
    }

    fn test_thread(items: Vec<ThreadItem>) -> Thread {
        Thread {
            id: "thread-1".to_string(),
            extra: None,
            session_id: "session-1".to_string(),
            forked_from_id: None,
            parent_thread_id: None,
            preview: "preview".to_string(),
            ephemeral: false,
            section: None,
            section_entered_at: None,
            project_id: None,
            history_mode: Default::default(),
            model_provider: "mock_provider".to_string(),
            created_at: 0,
            updated_at: 0,
            recency_at: Some(0),
            status: ThreadStatus::Idle,
            path: None,
            cwd: test_path_buf("/tmp").abs(),
            cli_version: "0.0.0".to_string(),
            source: SessionSource::Cli,
            can_accept_direct_input: None,
            thread_source: None,
            agent_nickname: None,
            agent_role: None,
            git_info: None,
            name: None,
            turns: vec![Turn {
                id: "turn-1".to_string(),
                items,
                items_view: TurnItemsView::Full,
                status: TurnStatus::Completed,
                error: None,
                started_at: None,
                completed_at: None,
                duration_ms: None,
            }],
        }
    }
}
