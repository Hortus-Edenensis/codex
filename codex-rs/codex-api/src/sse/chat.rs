use crate::common::ResponseEvent;
use crate::common::ResponseStream;
use crate::error::ApiError;
use crate::telemetry::SseTelemetry;
use codex_client::StreamResponse;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ReasoningItemContent;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::TokenUsage;
use eventsource_stream::Eventsource;
use futures::Stream;
use futures::StreamExt;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio::time::timeout;
use tracing::debug;
use tracing::trace;

pub(crate) fn spawn_chat_stream(
    stream_response: StreamResponse,
    idle_timeout: Duration,
    telemetry: Option<Arc<dyn SseTelemetry>>,
    _turn_state: Option<Arc<OnceLock<String>>>,
) -> ResponseStream {
    let upstream_request_id = stream_response
        .headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let (tx_event, rx_event) = mpsc::channel(1600);
    tokio::spawn(async move {
        process_chat_sse(stream_response.bytes, tx_event, idle_timeout, telemetry).await;
    });
    ResponseStream {
        rx_event,
        upstream_request_id,
    }
}

#[derive(Default)]
struct ToolCallState {
    id: String,
    name: String,
    arguments: String,
}

struct ChatStreamState {
    assistant_item: Option<ResponseItem>,
    reasoning_item: Option<ResponseItem>,
    tool_calls: BTreeMap<usize, ToolCallState>,
    tool_call_index_by_id: HashMap<String, usize>,
    last_tool_call_index: Option<usize>,
    response_id: String,
    end_turn: Option<bool>,
    token_usage: Option<TokenUsage>,
    completed: bool,
}

impl ChatStreamState {
    fn new() -> Self {
        Self {
            assistant_item: None,
            reasoning_item: None,
            tool_calls: BTreeMap::new(),
            tool_call_index_by_id: HashMap::new(),
            last_tool_call_index: None,
            response_id: String::new(),
            end_turn: None,
            token_usage: None,
            completed: false,
        }
    }

    async fn append_assistant_text(
        &mut self,
        tx_event: &mpsc::Sender<Result<ResponseEvent, ApiError>>,
        text: &str,
    ) {
        if text.is_empty() {
            return;
        }
        self.finish_reasoning(tx_event).await;
        if self.assistant_item.is_none() {
            let item = ResponseItem::Message {
                id: None,
                role: "assistant".to_string(),
                content: Vec::new(),
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            };
            self.assistant_item = Some(item.clone());
            let _ = tx_event
                .send(Ok(ResponseEvent::OutputItemAdded(item)))
                .await;
        }
        if let Some(ResponseItem::Message { content, .. }) = &mut self.assistant_item {
            content.push(ContentItem::OutputText {
                text: text.to_string(),
            });
        }
        let _ = tx_event
            .send(Ok(ResponseEvent::OutputTextDelta(text.to_string())))
            .await;
    }

    async fn finish_reasoning(&mut self, tx_event: &mpsc::Sender<Result<ResponseEvent, ApiError>>) {
        if let Some(reasoning) = self.reasoning_item.take() {
            let _ = tx_event
                .send(Ok(ResponseEvent::OutputItemDone(reasoning)))
                .await;
        }
    }

    async fn finish_assistant(&mut self, tx_event: &mpsc::Sender<Result<ResponseEvent, ApiError>>) {
        if let Some(assistant) = self.assistant_item.take() {
            let _ = tx_event
                .send(Ok(ResponseEvent::OutputItemDone(assistant)))
                .await;
        }
    }

    async fn finish_text_items(
        &mut self,
        tx_event: &mpsc::Sender<Result<ResponseEvent, ApiError>>,
    ) {
        self.finish_reasoning(tx_event).await;
        self.finish_assistant(tx_event).await;
    }

    async fn append_reasoning_text(
        &mut self,
        tx_event: &mpsc::Sender<Result<ResponseEvent, ApiError>>,
        text: &str,
    ) {
        if text.is_empty() {
            return;
        }
        if self.reasoning_item.is_none() {
            let item = ResponseItem::Reasoning {
                id: None,
                summary: Vec::new(),
                content: Some(Vec::new()),
                encrypted_content: None,
                internal_chat_message_metadata_passthrough: None,
            };
            self.reasoning_item = Some(item.clone());
            let _ = tx_event
                .send(Ok(ResponseEvent::OutputItemAdded(item)))
                .await;
        }
        let mut content_index = 0;
        if let Some(ResponseItem::Reasoning {
            content: Some(content),
            ..
        }) = &mut self.reasoning_item
        {
            content_index = content.len() as i64;
            content.push(ReasoningItemContent::ReasoningText {
                text: text.to_string(),
            });
        }
        let _ = tx_event
            .send(Ok(ResponseEvent::ReasoningContentDelta {
                delta: text.to_string(),
                content_index,
            }))
            .await;
    }

    fn update_tool_call(&mut self, tool_call: &serde_json::Value) {
        let id = tool_call
            .get("id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        let mut index = tool_call
            .get("index")
            .and_then(serde_json::Value::as_u64)
            .map(|value| value as usize);
        if index.is_none()
            && let Some(id) = id.as_deref()
            && let Some(existing) = self.tool_call_index_by_id.get(id)
        {
            index = Some(*existing);
        }
        let index = index
            .or(self.last_tool_call_index)
            .unwrap_or(self.tool_calls.len());
        self.last_tool_call_index = Some(index);

        let state = self.tool_calls.entry(index).or_default();
        if let Some(id) = id {
            state.id.clone_from(&id);
            self.tool_call_index_by_id.insert(id, index);
        }
        if let Some(function) = tool_call.get("function") {
            if let Some(name) = function.get("name").and_then(serde_json::Value::as_str) {
                if state.name != name {
                    state.name.push_str(name);
                }
            }
            if let Some(arguments) = function
                .get("arguments")
                .and_then(serde_json::Value::as_str)
            {
                state.arguments.push_str(arguments);
            }
        }
    }

    async fn complete(&mut self, tx_event: &mpsc::Sender<Result<ResponseEvent, ApiError>>) {
        if self.completed {
            return;
        }
        self.completed = true;
        self.finish_text_items(tx_event).await;
        for (_, call) in std::mem::take(&mut self.tool_calls) {
            let item = ResponseItem::FunctionCall {
                id: None,
                name: call.name,
                namespace: None,
                arguments: call.arguments,
                encrypted_function_args: None,
                call_id: call.id,
                internal_chat_message_metadata_passthrough: None,
            };
            let _ = tx_event
                .send(Ok(ResponseEvent::OutputItemAdded(item.clone())))
                .await;
            let _ = tx_event.send(Ok(ResponseEvent::OutputItemDone(item))).await;
        }
        let _ = tx_event
            .send(Ok(ResponseEvent::Completed {
                response_id: std::mem::take(&mut self.response_id),
                token_usage: self.token_usage.take(),
                usage_metadata: None,
                end_turn: self.end_turn,
            }))
            .await;
    }
}

pub async fn process_chat_sse<S>(
    stream: S,
    tx_event: mpsc::Sender<Result<ResponseEvent, ApiError>>,
    idle_timeout: Duration,
    telemetry: Option<Arc<dyn SseTelemetry>>,
) where
    S: Stream<Item = Result<bytes::Bytes, codex_client::TransportError>> + Unpin,
{
    let mut stream = stream.eventsource();
    let mut state = ChatStreamState::new();

    loop {
        let start = Instant::now();
        let response = timeout(idle_timeout, stream.next()).await;
        if let Some(telemetry) = telemetry.as_ref() {
            telemetry.on_sse_poll(&response, start.elapsed());
        }
        let event = match response {
            Ok(Some(Ok(event))) => event,
            Ok(Some(Err(error))) => {
                let _ = tx_event
                    .send(Err(ApiError::Stream(error.to_string())))
                    .await;
                return;
            }
            Ok(None) => {
                let _ = tx_event
                    .send(Err(ApiError::Stream(
                        "Chat Completions stream ended before [DONE]".to_string(),
                    )))
                    .await;
                return;
            }
            Err(_) => {
                let _ = tx_event
                    .send(Err(ApiError::Stream(
                        "idle timeout waiting for Chat Completions SSE".to_string(),
                    )))
                    .await;
                return;
            }
        };

        trace!("Chat Completions SSE event: {}", event.data);
        let data = event.data.trim();
        if data.is_empty() {
            continue;
        }
        if data == "[DONE]" || data == "DONE" {
            state.complete(&tx_event).await;
            return;
        }

        let value: serde_json::Value = match serde_json::from_str(data) {
            Ok(value) => value,
            Err(error) => {
                debug!("failed to parse Chat Completions SSE event: {error}");
                continue;
            }
        };
        if state.response_id.is_empty()
            && let Some(id) = value.get("id").and_then(serde_json::Value::as_str)
        {
            state.response_id = id.to_string();
        }
        if let Some(usage) = value.get("usage") {
            state.token_usage = chat_token_usage(usage);
        }
        let Some(choices) = value.get("choices").and_then(serde_json::Value::as_array) else {
            continue;
        };

        for choice in choices {
            let message = choice.get("delta").or_else(|| choice.get("message"));
            if let Some(message) = message {
                for key in ["reasoning_content", "reasoning"] {
                    if let Some(reasoning) = message.get(key) {
                        if let Some(text) = reasoning.as_str() {
                            state.append_reasoning_text(&tx_event, text).await;
                        } else if let Some(text) = reasoning
                            .get("text")
                            .or_else(|| reasoning.get("content"))
                            .and_then(serde_json::Value::as_str)
                        {
                            state.append_reasoning_text(&tx_event, text).await;
                        }
                    }
                }
                if let Some(content) = message.get("content") {
                    if let Some(text) = content.as_str() {
                        state.append_assistant_text(&tx_event, text).await;
                    } else if let Some(parts) = content.as_array() {
                        for part in parts {
                            if let Some(text) = part.get("text").and_then(serde_json::Value::as_str)
                            {
                                state.append_assistant_text(&tx_event, text).await;
                            }
                        }
                    }
                }
                if let Some(tool_calls) = message
                    .get("tool_calls")
                    .and_then(serde_json::Value::as_array)
                {
                    if !tool_calls.is_empty() {
                        state.finish_text_items(&tx_event).await;
                    }
                    for tool_call in tool_calls {
                        state.update_tool_call(tool_call);
                    }
                }
            }

            if let Some(finish_reason) = choice
                .get("finish_reason")
                .and_then(serde_json::Value::as_str)
            {
                if finish_reason == "length" {
                    let _ = tx_event.send(Err(ApiError::ContextWindowExceeded)).await;
                    return;
                }
                state.end_turn = match finish_reason {
                    "stop" => Some(true),
                    "tool_calls" | "function_call" => Some(false),
                    _ => None,
                };
            }
        }
    }
}

fn chat_token_usage(usage: &serde_json::Value) -> Option<TokenUsage> {
    let input_tokens = usage.get("prompt_tokens")?.as_i64()?;
    let output_tokens = usage.get("completion_tokens")?.as_i64()?;
    let cached_input_tokens = usage
        .pointer("/prompt_tokens_details/cached_tokens")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);
    let reasoning_output_tokens = usage
        .pointer("/completion_tokens_details/reasoning_tokens")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);
    let total_tokens = usage
        .get("total_tokens")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(input_tokens + output_tokens);
    Some(TokenUsage {
        input_tokens,
        cached_input_tokens,
        cache_write_input_tokens: 0,
        output_tokens,
        reasoning_output_tokens,
        total_tokens,
        codex_rollout_budget_units: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use futures::stream;

    async fn collect_events(chunks: &[&str]) -> Vec<ResponseEvent> {
        let stream = stream::iter(
            chunks
                .iter()
                .map(|chunk| Ok(bytes::Bytes::copy_from_slice(chunk.as_bytes()))),
        );
        let (tx, mut rx) = mpsc::channel(32);
        process_chat_sse(stream, tx, Duration::from_secs(1), None).await;
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event.expect("event"));
        }
        events
    }

    #[tokio::test]
    async fn parses_kimi_reasoning_content_and_text() {
        let events = collect_events(&[
            "data: {\"id\":\"chat-1\",\"choices\":[{\"delta\":{\"reasoning_content\":\"think\"}}]}\n\n",
            "data: {\"id\":\"chat-1\",\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}]}\n\n",
            "data: {\"id\":\"chat-1\",\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":5,\"total_tokens\":17}}\n\n",
            "data: [DONE]\n\n",
        ])
        .await;

        assert_eq!(events.len(), 7);
        assert_matches!(
            &events[0],
            ResponseEvent::OutputItemAdded(ResponseItem::Reasoning { .. })
        );
        assert_matches!(
            &events[1],
            ResponseEvent::ReasoningContentDelta { delta, .. } if delta == "think"
        );
        assert_matches!(
            &events[2],
            ResponseEvent::OutputItemDone(ResponseItem::Reasoning { .. })
        );
        assert_matches!(
            &events[3],
            ResponseEvent::OutputItemAdded(ResponseItem::Message { .. })
        );
        assert_matches!(
            &events[4],
            ResponseEvent::OutputTextDelta(delta) if delta == "done"
        );
        assert_matches!(
            &events[5],
            ResponseEvent::OutputItemDone(ResponseItem::Message { .. })
        );
        assert_matches!(
            &events[6],
            ResponseEvent::Completed {
                response_id,
                token_usage: Some(usage),
                end_turn: Some(true),
                ..
            } if response_id == "chat-1" && usage.total_tokens == 17
        );
    }

    #[tokio::test]
    async fn merges_streamed_tool_call_arguments() {
        let events = collect_events(&[
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"read_file\",\"arguments\":\"{\\\"path\\\":\\\"\"}}]}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"a.txt\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: [DONE]\n\n",
        ])
        .await;

        assert!(events.iter().any(|event| matches!(
            event,
            ResponseEvent::OutputItemDone(ResponseItem::FunctionCall {
                name,
                arguments,
                call_id,
                ..
            }) if name == "read_file"
                && arguments == r#"{"path":"a.txt"}"#
                && call_id == "call-1"
        )));
        assert_matches!(
            events.last(),
            Some(ResponseEvent::Completed {
                end_turn: Some(false),
                ..
            })
        );
    }
}
