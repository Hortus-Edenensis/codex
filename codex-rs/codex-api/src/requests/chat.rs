use crate::error::ApiError;
use crate::provider::Provider;
use crate::requests::headers::build_session_headers;
use crate::requests::headers::insert_header;
use crate::requests::headers::subagent_header;
use codex_protocol::models::AgentMessageInputContent;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ReasoningItemContent;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::SessionSource;
use http::HeaderMap;
use serde_json::Value;
use serde_json::json;
use std::collections::HashMap;

/// Assembled request body plus headers for a Chat Completions streaming call.
pub struct ChatRequest {
    pub body: Value,
    pub headers: HeaderMap,
}

pub struct ChatRequestBuilder<'a> {
    model: &'a str,
    instructions: &'a str,
    input: &'a [ResponseItem],
    tools: &'a [Value],
    parallel_tool_calls: bool,
    reasoning_effort: Option<String>,
    output_schema: Option<&'a Value>,
    output_schema_strict: bool,
    session_id: Option<String>,
    thread_id: Option<String>,
    session_source: Option<SessionSource>,
}

impl<'a> ChatRequestBuilder<'a> {
    pub fn new(
        model: &'a str,
        instructions: &'a str,
        input: &'a [ResponseItem],
        tools: &'a [Value],
    ) -> Self {
        Self {
            model,
            instructions,
            input,
            tools,
            parallel_tool_calls: false,
            reasoning_effort: None,
            output_schema: None,
            output_schema_strict: true,
            session_id: None,
            thread_id: None,
            session_source: None,
        }
    }

    pub fn parallel_tool_calls(mut self, enabled: bool) -> Self {
        self.parallel_tool_calls = enabled;
        self
    }

    pub fn reasoning_effort(mut self, effort: Option<String>) -> Self {
        self.reasoning_effort = effort;
        self
    }

    pub fn output_schema(mut self, schema: Option<&'a Value>, strict: bool) -> Self {
        self.output_schema = schema;
        self.output_schema_strict = strict;
        self
    }

    pub fn session_id(mut self, id: Option<String>) -> Self {
        self.session_id = id;
        self
    }

    pub fn thread_id(mut self, id: Option<String>) -> Self {
        self.thread_id = id;
        self
    }

    pub fn session_source(mut self, source: Option<SessionSource>) -> Self {
        self.session_source = source;
        self
    }

    pub fn build(self, provider: &Provider) -> Result<ChatRequest, ApiError> {
        let mut messages = Vec::new();
        if !self.instructions.trim().is_empty() {
            messages.push(json!({"role": "system", "content": self.instructions}));
        }

        let reasoning_by_anchor_index = collect_reasoning_by_anchor(self.input);
        for (index, item) in self.input.iter().enumerate() {
            match item {
                ResponseItem::Message { role, content, .. } => {
                    // Chat Completions providers such as Kimi do not accept
                    // OpenAI's newer `developer` role. Preserve its instruction
                    // priority by sending it as another system message.
                    let chat_role = if role == "developer" { "system" } else { role };
                    let mut text = String::new();
                    let mut parts = Vec::new();
                    let mut has_image = false;
                    for content_item in content {
                        match content_item {
                            ContentItem::InputText { text: value }
                            | ContentItem::OutputText { text: value } => {
                                text.push_str(value);
                                parts.push(json!({"type": "text", "text": value}));
                            }
                            ContentItem::InputImage { image_url, .. } => {
                                has_image = true;
                                parts.push(json!({
                                    "type": "image_url",
                                    "image_url": {"url": image_url},
                                }));
                            }
                            ContentItem::InputAudio { .. } => {}
                        }
                    }

                    let content = if role == "assistant" || !has_image {
                        json!(text)
                    } else {
                        json!(parts)
                    };
                    let mut message = json!({"role": chat_role, "content": content});
                    attach_reasoning_content(
                        &mut message,
                        reasoning_by_anchor_index.get(&index).map(String::as_str),
                    );
                    messages.push(message);
                }
                ResponseItem::AgentMessage { content, .. } => {
                    let text = content
                        .iter()
                        .filter_map(|part| match part {
                            AgentMessageInputContent::InputText { text } => Some(text.as_str()),
                            AgentMessageInputContent::EncryptedContent { .. } => None,
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    if !text.is_empty() {
                        messages.push(json!({"role": "assistant", "content": text}));
                    }
                }
                ResponseItem::FunctionCall {
                    name,
                    namespace,
                    arguments,
                    call_id,
                    ..
                } => {
                    let name = namespace
                        .as_ref()
                        .map_or_else(|| name.clone(), |namespace| format!("{namespace}{name}"));
                    push_tool_call_message(
                        &mut messages,
                        json!({
                            "id": call_id,
                            "type": "function",
                            "function": {
                                "name": name,
                                "arguments": arguments,
                            },
                        }),
                        reasoning_by_anchor_index.get(&index).map(String::as_str),
                    );
                }
                ResponseItem::LocalShellCall {
                    id,
                    call_id,
                    action,
                    ..
                } => {
                    let arguments = serde_json::to_string(action)
                        .map_err(|error| ApiError::Stream(error.to_string()))?;
                    push_tool_call_message(
                        &mut messages,
                        json!({
                            "id": call_id.clone().or_else(|| id.as_ref().map(ToString::to_string)).unwrap_or_default(),
                            "type": "function",
                            "function": {
                                "name": "local_shell",
                                "arguments": arguments,
                            },
                        }),
                        reasoning_by_anchor_index.get(&index).map(String::as_str),
                    );
                }
                ResponseItem::FunctionCallOutput {
                    call_id, output, ..
                } => {
                    messages.push(json!({
                        "role": "tool",
                        "tool_call_id": call_id,
                        "content": output.body.to_text().unwrap_or_default(),
                    }));
                }
                ResponseItem::CustomToolCallOutput {
                    call_id, output, ..
                } => {
                    messages.push(json!({
                        "role": "tool",
                        "tool_call_id": call_id,
                        "content": output.body.to_text().unwrap_or_default(),
                    }));
                }
                ResponseItem::CustomToolCall {
                    call_id,
                    name,
                    input,
                    ..
                } => {
                    let arguments = if serde_json::from_str::<Value>(input).is_ok() {
                        input.clone()
                    } else {
                        json!({"input": input}).to_string()
                    };
                    push_tool_call_message(
                        &mut messages,
                        json!({
                            "id": call_id,
                            "type": "function",
                            "function": {
                                "name": name,
                                "arguments": arguments,
                            },
                        }),
                        reasoning_by_anchor_index.get(&index).map(String::as_str),
                    );
                }
                ResponseItem::ToolSearchOutput {
                    call_id: Some(call_id),
                    tools,
                    ..
                } => {
                    messages.push(json!({
                        "role": "tool",
                        "tool_call_id": call_id,
                        "content": serde_json::to_string(tools)
                            .map_err(|error| ApiError::Stream(error.to_string()))?,
                    }));
                }
                ResponseItem::AdditionalTools { .. }
                | ResponseItem::Reasoning { .. }
                | ResponseItem::ToolSearchCall { .. }
                | ResponseItem::ToolSearchOutput { call_id: None, .. }
                | ResponseItem::WebSearchCall { .. }
                | ResponseItem::ImageGenerationCall { .. }
                | ResponseItem::Compaction { .. }
                | ResponseItem::CompactionTrigger { .. }
                | ResponseItem::ContextCompaction { .. }
                | ResponseItem::Other => {}
            }
        }

        let mut payload = json!({
            "model": self.model,
            "messages": messages,
            "stream": true,
        });
        let reasoning_enabled = self.reasoning_effort.is_some();
        if let Some(reasoning_effort) = self.reasoning_effort {
            payload["reasoning_effort"] = json!(reasoning_effort);
        }
        if reasoning_enabled && uses_kimi_thinking_parameter(provider) {
            payload["thinking"] = json!({"type": "enabled"});
        }
        if let Some(output_schema) = self.output_schema {
            payload["response_format"] = json!({
                "type": "json_schema",
                "json_schema": {
                    "name": "codex_output_schema",
                    "schema": output_schema,
                    "strict": self.output_schema_strict,
                },
            });
        }
        if !self.tools.is_empty() {
            payload["tools"] = json!(self.tools);
            payload["tool_choice"] = json!("auto");
            payload["parallel_tool_calls"] = json!(self.parallel_tool_calls);
        }
        let mut headers = build_session_headers(self.session_id, self.thread_id);
        if let Some(subagent) = subagent_header(&self.session_source) {
            insert_header(&mut headers, "x-openai-subagent", &subagent);
        }

        Ok(ChatRequest {
            body: payload,
            headers,
        })
    }
}

fn uses_kimi_thinking_parameter(provider: &Provider) -> bool {
    provider.name.eq_ignore_ascii_case("kimi")
        || provider
            .base_url
            .to_ascii_lowercase()
            .contains("moonshot.cn")
}

fn collect_reasoning_by_anchor(input: &[ResponseItem]) -> HashMap<usize, String> {
    let mut reasoning_by_anchor = HashMap::<usize, String>::new();
    for (index, item) in input.iter().enumerate() {
        let ResponseItem::Reasoning {
            content: Some(content),
            ..
        } = item
        else {
            continue;
        };
        let reasoning = content
            .iter()
            .map(|part| match part {
                ReasoningItemContent::ReasoningText { text }
                | ReasoningItemContent::Text { text } => text.as_str(),
            })
            .collect::<String>();
        if reasoning.trim().is_empty() {
            continue;
        }

        let anchor =
            input
                .iter()
                .enumerate()
                .skip(index + 1)
                .find_map(|(candidate_index, candidate)| {
                    is_assistant_anchor(candidate).then_some(candidate_index)
                })
                .or_else(|| {
                    input[..index].iter().enumerate().rev().find_map(
                        |(candidate_index, candidate)| {
                            is_assistant_anchor(candidate).then_some(candidate_index)
                        },
                    )
                });
        if let Some(anchor) = anchor {
            reasoning_by_anchor
                .entry(anchor)
                .and_modify(|existing| existing.push_str(&reasoning))
                .or_insert(reasoning);
        }
    }
    reasoning_by_anchor
}

fn is_assistant_anchor(item: &ResponseItem) -> bool {
    matches!(
        item,
        ResponseItem::Message { role, .. } if role == "assistant"
    ) || matches!(
        item,
        ResponseItem::AgentMessage { .. }
            | ResponseItem::FunctionCall { .. }
            | ResponseItem::LocalShellCall { .. }
            | ResponseItem::CustomToolCall { .. }
    )
}

fn attach_reasoning_content(message: &mut Value, reasoning: Option<&str>) {
    if let Some(reasoning) = reasoning
        && let Some(object) = message.as_object_mut()
    {
        object.insert("reasoning_content".to_string(), json!(reasoning));
    }
}

fn push_tool_call_message(messages: &mut Vec<Value>, tool_call: Value, reasoning: Option<&str>) {
    if let Some(Value::Object(object)) = messages.last_mut()
        && object.get("role").and_then(Value::as_str) == Some("assistant")
        && object.get("content").is_some_and(Value::is_null)
        && let Some(tool_calls) = object.get_mut("tool_calls").and_then(Value::as_array_mut)
    {
        tool_calls.push(tool_call);
        if let Some(reasoning) = reasoning {
            object
                .entry("reasoning_content")
                .and_modify(|value| {
                    if let Some(existing) = value.as_str() {
                        *value = json!(format!("{existing}{reasoning}"));
                    }
                })
                .or_insert_with(|| json!(reasoning));
        }
        return;
    }

    let mut message = json!({
        "role": "assistant",
        "content": null,
        "tool_calls": [tool_call],
    });
    attach_reasoning_content(&mut message, reasoning);
    messages.push(message);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::RetryConfig;
    use codex_protocol::models::FunctionCallOutputPayload;
    use pretty_assertions::assert_eq;
    use std::time::Duration;

    fn provider() -> Provider {
        Provider {
            name: "Kimi".to_string(),
            base_url: "https://api.moonshot.cn/v1".to_string(),
            query_params: None,
            headers: HeaderMap::new(),
            retry: RetryConfig {
                max_attempts: 1,
                base_delay: Duration::from_millis(10),
                retry_429: false,
                retry_5xx: true,
                retry_transport: true,
            },
            stream_idle_timeout: Duration::from_secs(1),
        }
    }

    #[test]
    fn builds_kimi_tool_loop_with_preserved_reasoning() {
        let input = vec![
            ResponseItem::Message {
                id: None,
                role: "user".to_string(),
                content: vec![ContentItem::InputText {
                    text: "read it".to_string(),
                }],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            },
            ResponseItem::Reasoning {
                id: None,
                summary: Vec::new(),
                content: Some(vec![ReasoningItemContent::ReasoningText {
                    text: "need the file".to_string(),
                }]),
                encrypted_content: None,
                internal_chat_message_metadata_passthrough: None,
            },
            ResponseItem::FunctionCall {
                id: None,
                name: "read_file".to_string(),
                namespace: None,
                arguments: r#"{"path":"a.txt"}"#.to_string(),
                encrypted_function_args: None,
                call_id: "call-a".to_string(),
                internal_chat_message_metadata_passthrough: None,
            },
            ResponseItem::FunctionCallOutput {
                id: None,
                call_id: Some("call-a".to_string()),
                name: None,
                namespace: None,
                output: FunctionCallOutputPayload::from_text("A".to_string()),
                internal_chat_message_metadata_passthrough: None,
            },
        ];

        let request = ChatRequestBuilder::new("kimi-k2.7-code", "be useful", &input, &[])
            .reasoning_effort(Some("max".to_string()))
            .build(&provider())
            .expect("request");
        let messages = request.body["messages"].as_array().expect("messages");

        assert!(request.body.get("max_tokens").is_none());
        assert_eq!(request.body["reasoning_effort"], "max");
        assert_eq!(request.body["thinking"]["type"], "enabled");
        assert_eq!(messages[2]["reasoning_content"], "need the file");
        assert_eq!(messages[2]["tool_calls"][0]["id"], "call-a");
        assert_eq!(messages[3]["tool_call_id"], "call-a");
        assert_eq!(messages[3]["content"], "A");
    }

    #[test]
    fn maps_developer_messages_to_system_for_chat_providers() {
        let input = vec![ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: "follow this policy".to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        }];

        let request = ChatRequestBuilder::new("kimi-k3", "", &input, &[])
            .build(&provider())
            .expect("request");

        assert_eq!(request.body["messages"][0]["role"], "system");
        assert_eq!(request.body["messages"][0]["content"], "follow this policy");
    }

    #[test]
    fn maps_output_schema_to_chat_response_format() {
        let schema = json!({
            "type": "object",
            "properties": {"approved": {"type": "boolean"}},
            "required": ["approved"],
            "additionalProperties": false,
        });

        let request = ChatRequestBuilder::new("kimi-k3", "", &[], &[])
            .output_schema(Some(&schema), true)
            .build(&provider())
            .expect("request");

        assert_eq!(request.body["response_format"]["type"], "json_schema");
        assert_eq!(
            request.body["response_format"]["json_schema"]["name"],
            "codex_output_schema"
        );
        assert_eq!(
            request.body["response_format"]["json_schema"]["schema"],
            schema
        );
        assert_eq!(
            request.body["response_format"]["json_schema"]["strict"],
            true
        );
    }

    #[test]
    fn does_not_add_kimi_thinking_parameter_for_other_chat_providers() {
        let mut generic_provider = provider();
        generic_provider.name = "Compatible".to_string();
        generic_provider.base_url = "https://example.com/v1".to_string();

        let request = ChatRequestBuilder::new("reasoning-model", "", &[], &[])
            .reasoning_effort(Some("high".to_string()))
            .build(&generic_provider)
            .expect("request");

        assert_eq!(request.body["reasoning_effort"], "high");
        assert!(request.body.get("thinking").is_none());
    }
}
