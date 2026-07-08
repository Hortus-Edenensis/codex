use std::io;
use std::io::BufRead;

use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SseMessage {
    pub(crate) event: Option<String>,
    pub(crate) data: String,
}

pub(crate) fn read_sse_message(reader: &mut dyn BufRead) -> io::Result<Option<SseMessage>> {
    let mut event = None;
    let mut data_lines = Vec::new();
    let mut saw_any_line = false;

    loop {
        let mut line = String::new();
        let bytes_read = reader.read_line(&mut line)?;
        if bytes_read == 0 {
            if !saw_any_line {
                return Ok(None);
            }
            break;
        }
        saw_any_line = true;

        if line == "\n" || line == "\r\n" {
            break;
        }

        let line = line.trim_end_matches(['\r', '\n']);
        if let Some(value) = line.strip_prefix("event:") {
            event = Some(value.trim_start().to_string());
            continue;
        }
        if let Some(value) = line.strip_prefix("data:") {
            data_lines.push(value.trim_start().to_string());
            continue;
        }
    }

    Ok(Some(SseMessage {
        event,
        data: data_lines.join("\n"),
    }))
}

pub(crate) fn parse_sse_json(message: &SseMessage) -> Option<Value> {
    if message.data.is_empty() {
        return None;
    }
    serde_json::from_str(&message.data).ok()
}

pub(crate) fn serialize_json_event(value: &Value) -> io::Result<Vec<u8>> {
    let event_name = value
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "SSE event missing type"))?;
    let body = serde_json::to_vec(value)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;

    let mut bytes = Vec::with_capacity(event_name.len() + body.len() + 32);
    bytes.extend_from_slice(b"event: ");
    bytes.extend_from_slice(event_name.as_bytes());
    bytes.extend_from_slice(b"\r\ndata: ");
    bytes.extend_from_slice(&body);
    bytes.extend_from_slice(b"\r\n\r\n");
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::io::BufReader;
    use std::io::Cursor;

    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::SseMessage;
    use super::parse_sse_json;
    use super::read_sse_message;
    use super::serialize_json_event;

    #[test]
    fn reads_event_and_data_lines() {
        let bytes = b"event: response.created\r\ndata: {\"type\":\"response.created\"}\r\n\r\n";
        let mut reader = BufReader::new(Cursor::new(bytes));

        let message = read_sse_message(&mut reader).unwrap().unwrap();
        assert_eq!(
            message,
            SseMessage {
                event: Some("response.created".to_string()),
                data: "{\"type\":\"response.created\"}".to_string(),
            }
        );
        assert_eq!(
            parse_sse_json(&message),
            Some(json!({"type": "response.created"}))
        );
    }

    #[test]
    fn serializes_json_event_to_sse_bytes() {
        let event = json!({
            "type": "response.completed",
            "response": {"id": "resp-1"}
        });

        let bytes = serialize_json_event(&event).unwrap();
        assert_eq!(
            String::from_utf8(bytes).unwrap(),
            "event: response.completed\r\ndata: {\"response\":{\"id\":\"resp-1\"},\"type\":\"response.completed\"}\r\n\r\n"
        );
    }
}
