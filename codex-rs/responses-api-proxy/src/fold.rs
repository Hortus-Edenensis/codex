use std::collections::HashMap;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Cursor;
use std::io::Read;
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::SyncSender;
use std::thread;

use reqwest::Url;
use reqwest::blocking::Client;
use reqwest::blocking::Response;
use reqwest::header::HeaderMap;
use serde_json::Map;
use serde_json::Value;

use crate::continue_thinking::ContinueThinkingConfig;
use crate::continue_thinking::build_next_round_body;
use crate::continue_thinking::commentary_message;
use crate::continue_thinking::reasoning_tokens_from_usage;
use crate::continue_thinking::reconstruct_usage;
use crate::continue_thinking::should_continue;
use crate::continue_thinking::sum_usage;
use crate::sse::parse_sse_json;
use crate::sse::read_sse_message;
use crate::sse::serialize_json_event;

pub(crate) fn create_folded_body(
    first_response: Response,
    client: Client,
    upstream_url: Url,
    upstream_headers: HeaderMap,
    base_body: Value,
    config: ContinueThinkingConfig,
) -> Box<dyn Read + Send> {
    let (tx, rx) = mpsc::sync_channel::<Vec<u8>>(16);
    thread::spawn(move || {
        let base_object = match base_body.as_object() {
            Some(base_object) => base_object.clone(),
            None => return,
        };
        let mut round = UpstreamRound::from_response(first_response);
        let _ = fold_rounds(&mut round, &base_object, &config, &tx, &mut |next_body| {
            let response = client
                .post(upstream_url.clone())
                .headers(upstream_headers.clone())
                .body(serde_json::to_vec(next_body).map_err(invalid_data)?)
                .send()
                .map_err(other_io)?;
            let status = response.status();
            let is_sse = response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.contains("text/event-stream"));
            if !status.is_success() || !is_sse {
                return Err(io::Error::other(format!(
                    "continuation round returned non-SSE status {}",
                    status.as_u16()
                )));
            }
            Ok(UpstreamRound::from_response(response))
        });
    });
    Box::new(ChannelReader::new(rx))
}

fn fold_rounds(
    current_round: &mut UpstreamRound,
    base_body: &Map<String, Value>,
    config: &ContinueThinkingConfig,
    tx: &SyncSender<Vec<u8>>,
    open_next_round: &mut dyn FnMut(&Value) -> io::Result<UpstreamRound>,
) -> io::Result<()> {
    let mut round_number = 1usize;
    let mut replay_tail = Vec::new();
    let mut first_usage: Option<Value> = None;
    let mut total_usage = Map::new();
    let mut hidden_rounds_taken = 0usize;

    loop {
        let round_result = process_round(current_round, round_number, tx)?;
        if first_usage.is_none() {
            first_usage = round_result.usage.clone();
        }
        sum_usage(&mut total_usage, round_result.usage.as_ref());

        let has_encrypted_reasoning = round_result
            .reasoning_items
            .iter()
            .any(item_has_encrypted_reasoning);
        let should_hide_and_continue = should_continue(
            config,
            reasoning_tokens_from_usage(round_result.usage.as_ref()),
            round_number,
            has_encrypted_reasoning,
        );

        if should_hide_and_continue {
            let mut next_replay_tail = replay_tail.clone();
            next_replay_tail.extend(round_result.reasoning_items.iter().cloned());
            next_replay_tail.push(commentary_message(&config.message));

            if let Some(next_body) = build_next_round_body(base_body, &next_replay_tail)
                && let Ok(next_round) = open_next_round(&next_body)
            {
                *current_round = next_round;
                replay_tail = next_replay_tail;
                hidden_rounds_taken += 1;
                round_number += 1;
                continue;
            }
        }

        flush_buffered_events(tx, &round_result.buffered_events)?;
        if let Some(mut terminal) = round_result.terminal {
            if hidden_rounds_taken > 0
                && let Some(response) = terminal.get_mut("response").and_then(Value::as_object_mut)
            {
                response.insert(
                    "usage".to_string(),
                    reconstruct_usage(
                        first_usage.as_ref(),
                        &total_usage,
                        round_result.usage.as_ref(),
                    ),
                );
            }
            send_json_event(tx, &terminal)?;
        }
        break;
    }

    Ok(())
}

fn process_round(
    round: &mut UpstreamRound,
    round_number: usize,
    tx: &SyncSender<Vec<u8>>,
) -> io::Result<RoundResult> {
    let mut item_dispositions = ItemDispositions::default();
    let mut buffered_events = Vec::new();
    let mut reasoning_items = Vec::new();
    let mut terminal = None;
    let mut usage = None;

    while let Some(message) = read_sse_message(round.reader.as_mut())? {
        let Some(event) = parse_sse_json(&message) else {
            continue;
        };
        let event_type = event
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();

        match event_type {
            "response.created" | "response.in_progress" => {
                if round_number == 1 {
                    send_json_event(tx, &event)?;
                }
            }
            "response.completed" | "response.failed" | "response.incomplete" => {
                usage = event
                    .get("response")
                    .and_then(|response| response.get("usage"))
                    .cloned();
                terminal = Some(event);
                break;
            }
            "response.output_item.added" => {
                let item = event.get("item").cloned().unwrap_or(Value::Null);
                let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();
                if item_type == "reasoning" {
                    item_dispositions.insert(&event, ItemDisposition::Reasoning);
                    send_json_event(tx, &event)?;
                } else {
                    let index = buffered_events.len();
                    item_dispositions.insert(&event, ItemDisposition::Buffered(index));
                    buffered_events.push(vec![event]);
                }
            }
            _ => match item_dispositions.lookup(&event) {
                Some(ItemDisposition::Reasoning) => {
                    if event_type == "response.output_item.done"
                        && let Some(item) = event.get("item").cloned()
                    {
                        reasoning_items.push(item);
                    }
                    send_json_event(tx, &event)?;
                }
                Some(ItemDisposition::Buffered(index)) => {
                    if let Some(buffer) = buffered_events.get_mut(index) {
                        buffer.push(event);
                    }
                }
                None => send_json_event(tx, &event)?,
            },
        }
    }

    Ok(RoundResult {
        buffered_events,
        reasoning_items,
        terminal,
        usage,
    })
}

fn flush_buffered_events(
    tx: &SyncSender<Vec<u8>>,
    buffered_events: &[Vec<Value>],
) -> io::Result<()> {
    for events in buffered_events {
        for event in events {
            send_json_event(tx, event)?;
        }
    }
    Ok(())
}

fn send_json_event(tx: &SyncSender<Vec<u8>>, event: &Value) -> io::Result<()> {
    tx.send(serialize_json_event(event)?)
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "proxy consumer dropped"))
}

fn item_has_encrypted_reasoning(item: &Value) -> bool {
    item.get("encrypted_content").is_some_and(Value::is_string)
}

fn invalid_data(err: serde_json::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err.to_string())
}

fn other_io(err: impl std::fmt::Display) -> io::Error {
    io::Error::other(err.to_string())
}

#[derive(Default)]
struct ItemDispositions {
    by_output_index: HashMap<i64, ItemDisposition>,
    by_item_id: HashMap<String, ItemDisposition>,
}

impl ItemDispositions {
    fn insert(&mut self, event: &Value, disposition: ItemDisposition) {
        if let Some(output_index) = event.get("output_index").and_then(Value::as_i64) {
            self.by_output_index.insert(output_index, disposition);
        }
        if let Some(item_id) = event_item_id(event) {
            self.by_item_id.insert(item_id, disposition);
        }
    }

    fn lookup(&self, event: &Value) -> Option<ItemDisposition> {
        if let Some(output_index) = event.get("output_index").and_then(Value::as_i64)
            && let Some(disposition) = self.by_output_index.get(&output_index)
        {
            return Some(*disposition);
        }
        event_item_id(event).and_then(|item_id| self.by_item_id.get(&item_id).copied())
    }
}

#[derive(Clone, Copy)]
enum ItemDisposition {
    Reasoning,
    Buffered(usize),
}

struct RoundResult {
    buffered_events: Vec<Vec<Value>>,
    reasoning_items: Vec<Value>,
    terminal: Option<Value>,
    usage: Option<Value>,
}

struct UpstreamRound {
    reader: Box<dyn BufRead + Send>,
}

impl UpstreamRound {
    fn from_response(response: Response) -> Self {
        Self {
            reader: Box::new(BufReader::new(response)),
        }
    }

    #[cfg(test)]
    fn from_bytes(bytes: Vec<u8>) -> Self {
        Self {
            reader: Box::new(BufReader::new(Cursor::new(bytes))),
        }
    }
}

struct ChannelReader {
    rx: Receiver<Vec<u8>>,
    current: Cursor<Vec<u8>>,
    finished: bool,
}

impl ChannelReader {
    fn new(rx: Receiver<Vec<u8>>) -> Self {
        Self {
            rx,
            current: Cursor::new(Vec::new()),
            finished: false,
        }
    }
}

impl Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let bytes = self.current.read(buf)?;
            if bytes > 0 {
                return Ok(bytes);
            }
            if self.finished {
                return Ok(0);
            }
            match self.rx.recv() {
                Ok(next) => self.current = Cursor::new(next),
                Err(_) => {
                    self.finished = true;
                    return Ok(0);
                }
            }
        }
    }
}

fn event_item_id(event: &Value) -> Option<String> {
    event
        .get("item_id")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            event
                .get("item")
                .and_then(|item| item.get("id"))
                .and_then(Value::as_str)
                .map(str::to_string)
        })
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::mpsc;

    use pretty_assertions::assert_eq;
    use serde_json::Value;
    use serde_json::json;

    use super::ContinueThinkingConfig;
    use super::UpstreamRound;
    use super::fold_rounds;
    use crate::sse::parse_sse_json;
    use crate::sse::read_sse_message;

    fn sse_bytes(events: &[Value]) -> Vec<u8> {
        let mut out = Vec::new();
        for event in events {
            let kind = event.get("type").and_then(Value::as_str).unwrap();
            out.extend_from_slice(b"event: ");
            out.extend_from_slice(kind.as_bytes());
            out.extend_from_slice(b"\r\ndata: ");
            out.extend_from_slice(event.to_string().as_bytes());
            out.extend_from_slice(b"\r\n\r\n");
        }
        out
    }

    fn collect_output(rx: mpsc::Receiver<Vec<u8>>) -> Vec<Value> {
        let mut joined = Vec::new();
        while let Ok(chunk) = rx.recv() {
            joined.extend_from_slice(&chunk);
        }
        let mut reader = std::io::BufReader::new(Cursor::new(joined));
        let mut events = Vec::new();
        while let Some(message) = read_sse_message(&mut reader).unwrap() {
            if let Some(event) = parse_sse_json(&message) {
                events.push(event);
            }
        }
        events
    }

    #[test]
    fn folds_truncated_first_round_into_second_round() {
        let config = ContinueThinkingConfig {
            enabled: true,
            message: "Continue thinking.".to_string(),
            max_extra_rounds: 2,
        };
        let first_round = sse_bytes(&[
            json!({"type": "response.created", "response": {"id": "resp-1"}}),
            json!({"type": "response.output_item.added", "output_index": 0, "item": {"type": "reasoning", "id": "r1"}}),
            json!({"type": "response.output_item.done", "output_index": 0, "item": {"type": "reasoning", "id": "r1", "encrypted_content": "abc"}}),
            json!({"type": "response.output_item.added", "output_index": 1, "item": {"type": "message", "id": "m1", "role": "assistant", "content": [{"type":"output_text","text":""}]}}),
            json!({"type": "response.output_text.delta", "output_index": 1, "item_id": "m1", "delta": "bad answer"}),
            json!({"type": "response.output_item.done", "output_index": 1, "item": {"type": "message", "id": "m1", "role": "assistant", "content": [{"type":"output_text","text":"bad answer"}]}}),
            json!({"type": "response.completed", "response": {"id": "resp-1", "usage": {"input_tokens": 10, "input_tokens_details": {"cached_tokens": 1}, "output_tokens": 520, "output_tokens_details": {"reasoning_tokens": 516}, "total_tokens": 530}}}),
        ]);
        let second_round = sse_bytes(&[
            json!({"type": "response.output_item.added", "output_index": 0, "item": {"type": "reasoning", "id": "r2"}}),
            json!({"type": "response.output_item.done", "output_index": 0, "item": {"type": "reasoning", "id": "r2", "encrypted_content": "def"}}),
            json!({"type": "response.output_item.added", "output_index": 1, "item": {"type": "message", "id": "m2", "role": "assistant", "content": [{"type":"output_text","text":""}]}}),
            json!({"type": "response.output_text.delta", "output_index": 1, "item_id": "m2", "delta": "good answer"}),
            json!({"type": "response.output_item.done", "output_index": 1, "item": {"type": "message", "id": "m2", "role": "assistant", "content": [{"type":"output_text","text":"good answer"}]}}),
            json!({"type": "response.completed", "response": {"id": "resp-2", "usage": {"input_tokens": 12, "output_tokens": 40, "output_tokens_details": {"reasoning_tokens": 20}, "total_tokens": 52}}}),
        ]);
        let base_body = json!({
            "stream": true,
            "reasoning": {"effort": "high"},
            "input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "solve"}]}]
        });
        let mut current_round = UpstreamRound::from_bytes(first_round);
        let (tx, rx) = mpsc::sync_channel(16);
        let mut next_rounds = vec![UpstreamRound::from_bytes(second_round)];
        let mut captured_payloads = Vec::new();

        fold_rounds(
            &mut current_round,
            base_body.as_object().unwrap(),
            &config,
            &tx,
            &mut |next_body| {
                captured_payloads.push(next_body.clone());
                Ok(next_rounds.remove(0))
            },
        )
        .unwrap();
        drop(tx);

        let events = collect_output(rx);
        assert_eq!(events[0]["type"], json!("response.created"));
        assert_eq!(events[1]["type"], json!("response.output_item.added"));
        assert_eq!(events[1]["item"]["id"], json!("r1"));
        assert_eq!(events[2]["type"], json!("response.output_item.done"));
        assert_eq!(events[2]["item"]["id"], json!("r1"));
        assert_eq!(events[3]["type"], json!("response.output_item.added"));
        assert_eq!(events[3]["item"]["id"], json!("r2"));
        assert_eq!(events[4]["type"], json!("response.output_item.done"));
        assert_eq!(events[4]["item"]["id"], json!("r2"));
        assert_eq!(events[5]["type"], json!("response.output_item.added"));
        assert_eq!(events[5]["item"]["id"], json!("m2"));
        assert_eq!(
            events[8]["response"]["usage"]["output_tokens_details"]["reasoning_tokens"],
            json!(536)
        );
        assert_eq!(events[8]["response"]["usage"]["output_tokens"], json!(556));
        assert_eq!(
            events
                .iter()
                .filter(|event| event["item"]["id"] == json!("m1"))
                .count(),
            0
        );

        assert_eq!(captured_payloads.len(), 1);
        let next_input = captured_payloads[0]["input"].as_array().unwrap();
        assert_eq!(next_input.len(), 3);
        assert_eq!(next_input[1]["type"], json!("reasoning"));
        assert_eq!(next_input[2]["phase"], json!("commentary"));
    }

    #[test]
    fn keeps_first_round_output_when_reasoning_is_not_encrypted() {
        let config = ContinueThinkingConfig {
            enabled: true,
            message: "Continue thinking.".to_string(),
            max_extra_rounds: 2,
        };
        let first_round = sse_bytes(&[
            json!({"type": "response.output_item.added", "output_index": 0, "item": {"type": "message", "id": "m1", "role": "assistant", "content": [{"type":"output_text","text":""}]}}),
            json!({"type": "response.output_text.delta", "output_index": 0, "item_id": "m1", "delta": "keep me"}),
            json!({"type": "response.output_item.done", "output_index": 0, "item": {"type": "message", "id": "m1", "role": "assistant", "content": [{"type":"output_text","text":"keep me"}]}}),
            json!({"type": "response.completed", "response": {"id": "resp-1", "usage": {"output_tokens": 520, "output_tokens_details": {"reasoning_tokens": 516}}}}),
        ]);
        let base_body = json!({
            "stream": true,
            "reasoning": {"effort": "high"},
            "input": []
        });
        let mut current_round = UpstreamRound::from_bytes(first_round);
        let (tx, rx) = mpsc::sync_channel(16);

        fold_rounds(
            &mut current_round,
            base_body.as_object().unwrap(),
            &config,
            &tx,
            &mut |_next_body| unreachable!("should not continue without encrypted reasoning"),
        )
        .unwrap();
        drop(tx);

        let events = collect_output(rx);
        assert_eq!(events.len(), 4);
        assert_eq!(events[0]["type"], json!("response.output_item.added"));
        assert_eq!(events[0]["item"]["id"], json!("m1"));
        assert_eq!(events[2]["type"], json!("response.output_item.done"));
        assert_eq!(events[2]["item"]["id"], json!("m1"));
        assert_eq!(events[3]["type"], json!("response.completed"));
        assert_eq!(events[3]["response"]["id"], json!("resp-1"));
    }

    #[test]
    fn falls_back_to_first_round_when_continuation_open_fails() {
        let config = ContinueThinkingConfig {
            enabled: true,
            message: "Continue thinking.".to_string(),
            max_extra_rounds: 2,
        };
        let first_round = sse_bytes(&[
            json!({"type": "response.output_item.added", "output_index": 0, "item": {"type": "reasoning", "id": "r1"}}),
            json!({"type": "response.output_item.done", "output_index": 0, "item": {"type": "reasoning", "id": "r1", "encrypted_content": "abc"}}),
            json!({"type": "response.output_item.added", "output_index": 1, "item": {"type": "message", "id": "m1", "role": "assistant", "content": [{"type":"output_text","text":""}]}}),
            json!({"type": "response.output_text.delta", "output_index": 1, "item_id": "m1", "delta": "keep me"}),
            json!({"type": "response.output_item.done", "output_index": 1, "item": {"type": "message", "id": "m1", "role": "assistant", "content": [{"type":"output_text","text":"keep me"}]}}),
            json!({"type": "response.completed", "response": {"id": "resp-1", "usage": {"output_tokens": 520, "output_tokens_details": {"reasoning_tokens": 516}}}}),
        ]);
        let base_body = json!({
            "stream": true,
            "reasoning": {"effort": "high"},
            "input": []
        });
        let mut current_round = UpstreamRound::from_bytes(first_round);
        let (tx, rx) = mpsc::sync_channel(16);

        fold_rounds(
            &mut current_round,
            base_body.as_object().unwrap(),
            &config,
            &tx,
            &mut |_next_body| Err(std::io::Error::other("boom")),
        )
        .unwrap();
        drop(tx);

        let events = collect_output(rx);
        assert_eq!(events.len(), 6);
        assert_eq!(events[0]["type"], json!("response.output_item.added"));
        assert_eq!(events[0]["item"]["id"], json!("r1"));
        assert_eq!(events[1]["type"], json!("response.output_item.done"));
        assert_eq!(events[1]["item"]["id"], json!("r1"));
        assert_eq!(events[2]["type"], json!("response.output_item.added"));
        assert_eq!(events[2]["item"]["id"], json!("m1"));
        assert_eq!(events[4]["type"], json!("response.output_item.done"));
        assert_eq!(events[4]["item"]["id"], json!("m1"));
        assert_eq!(events[5]["type"], json!("response.completed"));
        assert_eq!(events[5]["response"]["id"], json!("resp-1"));
    }
}
