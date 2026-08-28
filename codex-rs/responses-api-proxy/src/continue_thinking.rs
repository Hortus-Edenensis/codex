use serde_json::Map;
use serde_json::Value;

pub(crate) const ENCRYPTED_REASONING_INCLUDE: &str = "reasoning.encrypted_content";
const DEFAULT_TRUNCATION_STEP: i64 = 518;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ContinueThinkingConfig {
    pub(crate) enabled: bool,
    pub(crate) message: String,
    pub(crate) max_extra_rounds: usize,
}

impl ContinueThinkingConfig {
    pub(crate) fn is_candidate_request(&self, body: &Value) -> bool {
        self.enabled
            && body.is_object()
            && body.get("stream").and_then(Value::as_bool) == Some(true)
            && body.get("reasoning") != Some(&Value::Bool(false))
            && body.get("input").is_some_and(Value::is_array)
    }
}

pub(crate) fn should_continue(
    config: &ContinueThinkingConfig,
    reasoning_tokens: Option<i64>,
    round_number: usize,
    has_encrypted_reasoning: bool,
) -> bool {
    config.enabled
        && round_number <= config.max_extra_rounds
        && has_encrypted_reasoning
        && is_truncation_pattern(reasoning_tokens)
}

pub(crate) fn is_truncation_pattern(reasoning_tokens: Option<i64>) -> bool {
    let Some(reasoning_tokens) = reasoning_tokens else {
        return false;
    };

    reasoning_tokens >= DEFAULT_TRUNCATION_STEP - 2
        && (reasoning_tokens + 2) % DEFAULT_TRUNCATION_STEP == 0
}

pub(crate) fn reasoning_tokens_from_usage(usage: Option<&Value>) -> Option<i64> {
    usage.and_then(|usage| {
        usage
            .get("output_tokens_details")
            .and_then(|details| details.get("reasoning_tokens"))
            .and_then(Value::as_i64)
    })
}

pub(crate) fn merge_include_with_encrypted_reasoning(body: &Map<String, Value>) -> Value {
    let mut include = body
        .get("include")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let already_present = include
        .iter()
        .any(|value| value.as_str() == Some(ENCRYPTED_REASONING_INCLUDE));
    if !already_present {
        include.push(Value::String(ENCRYPTED_REASONING_INCLUDE.to_string()));
    }
    Value::Array(include)
}

pub(crate) fn commentary_message(text: &str) -> Value {
    serde_json::json!({
        "type": "message",
        "role": "assistant",
        "phase": "commentary",
        "content": [
            {
                "type": "output_text",
                "text": text,
            }
        ],
    })
}

pub(crate) fn build_next_round_body(
    base_body: &Map<String, Value>,
    replay_tail: &[Value],
) -> Option<Value> {
    let input = base_body.get("input")?.as_array()?;
    let mut next_body = base_body.clone();
    let mut next_input = input.clone();
    next_input.extend(replay_tail.iter().cloned());
    next_body.insert("input".to_string(), Value::Array(next_input));
    next_body.insert(
        "include".to_string(),
        merge_include_with_encrypted_reasoning(base_body),
    );
    next_body.remove("previous_response_id");
    Some(Value::Object(next_body))
}

pub(crate) fn sum_usage(acc: &mut Map<String, Value>, usage: Option<&Value>) {
    let Some(usage) = usage.and_then(Value::as_object) else {
        return;
    };

    add_top_level_usage_field(acc, usage, "input_tokens");
    add_top_level_usage_field(acc, usage, "output_tokens");
    add_top_level_usage_field(acc, usage, "total_tokens");

    if let Some(cached_tokens) = usage
        .get("input_tokens_details")
        .and_then(Value::as_object)
        .and_then(|details| details.get("cached_tokens"))
        .and_then(Value::as_i64)
    {
        add_nested_usage_field(acc, "input_tokens_details", "cached_tokens", cached_tokens);
    }

    if let Some(reasoning_tokens) = usage
        .get("output_tokens_details")
        .and_then(Value::as_object)
        .and_then(|details| details.get("reasoning_tokens"))
        .and_then(Value::as_i64)
    {
        add_nested_usage_field(
            acc,
            "output_tokens_details",
            "reasoning_tokens",
            reasoning_tokens,
        );
    }
}

pub(crate) fn reconstruct_usage(
    first_usage: Option<&Value>,
    total_usage: &Map<String, Value>,
    final_usage: Option<&Value>,
) -> Value {
    let input_tokens = first_usage
        .and_then(|usage| usage.get("input_tokens"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let cached_tokens = first_usage
        .and_then(|usage| usage.get("input_tokens_details"))
        .and_then(|details| details.get("cached_tokens"))
        .and_then(Value::as_i64);
    let reasoning_tokens = total_usage
        .get("output_tokens_details")
        .and_then(|details| details.get("reasoning_tokens"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let final_output_tokens = final_usage
        .and_then(|usage| usage.get("output_tokens"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let final_reasoning_tokens = reasoning_tokens_from_usage(final_usage).unwrap_or(0);
    let final_non_reasoning_tokens = (final_output_tokens - final_reasoning_tokens).max(0);
    let output_tokens = reasoning_tokens + final_non_reasoning_tokens;
    let total_tokens = input_tokens + output_tokens;

    let mut usage = serde_json::json!({
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "output_tokens_details": {
            "reasoning_tokens": reasoning_tokens,
        },
    });
    if let Some(cached_tokens) = cached_tokens {
        usage["input_tokens_details"] = serde_json::json!({
            "cached_tokens": cached_tokens,
        });
    }
    usage
}

fn add_top_level_usage_field(acc: &mut Map<String, Value>, usage: &Map<String, Value>, key: &str) {
    let Some(delta) = usage.get(key).and_then(Value::as_i64) else {
        return;
    };
    let current = acc.get(key).and_then(Value::as_i64).unwrap_or(0);
    acc.insert(key.to_string(), Value::from(current + delta));
}

fn add_nested_usage_field(
    acc: &mut Map<String, Value>,
    outer_key: &str,
    inner_key: &str,
    delta: i64,
) {
    let outer = acc
        .entry(outer_key.to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    let Value::Object(outer) = outer else {
        return;
    };
    let current = outer.get(inner_key).and_then(Value::as_i64).unwrap_or(0);
    outer.insert(inner_key.to_string(), Value::from(current + delta));
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::Map;
    use serde_json::json;

    use super::ContinueThinkingConfig;
    use super::build_next_round_body;
    use super::commentary_message;
    use super::is_truncation_pattern;
    use super::merge_include_with_encrypted_reasoning;
    use super::reasoning_tokens_from_usage;
    use super::reconstruct_usage;
    use super::should_continue;
    use super::sum_usage;

    #[test]
    fn detects_truncation_pattern() {
        assert!(is_truncation_pattern(Some(516)));
        assert!(is_truncation_pattern(Some(1034)));
        assert!(!is_truncation_pattern(Some(515)));
        assert!(!is_truncation_pattern(None));
    }

    #[test]
    fn continue_requires_encrypted_reasoning_and_budget() {
        let config = ContinueThinkingConfig {
            enabled: true,
            message: "Continue thinking.".to_string(),
            max_extra_rounds: 2,
        };

        assert!(should_continue(
            &config,
            Some(516),
            /*round_number*/ 1,
            /*has_encrypted*/ true
        ));
        assert!(!should_continue(
            &config,
            Some(516),
            /*round_number*/ 3,
            /*has_encrypted*/ true,
        ));
        assert!(!should_continue(
            &config,
            Some(516),
            /*round_number*/ 1,
            /*has_encrypted*/ false,
        ));
    }

    #[test]
    fn candidate_request_requires_stream_reasoning_and_array_input() {
        let config = ContinueThinkingConfig {
            enabled: true,
            message: "Continue thinking.".to_string(),
            max_extra_rounds: 2,
        };

        assert!(config.is_candidate_request(&json!({
            "stream": true,
            "reasoning": {"effort": "high"},
            "input": []
        })));
        assert!(!config.is_candidate_request(&json!({
            "stream": false,
            "reasoning": {"effort": "high"},
            "input": []
        })));
        assert!(!config.is_candidate_request(&json!({
            "stream": true,
            "reasoning": false,
            "input": []
        })));
        assert!(!config.is_candidate_request(&json!({
            "stream": true,
            "reasoning": {"effort": "high"},
            "input": "plain text"
        })));
    }

    #[test]
    fn merges_include_without_duplicates() {
        let body = json!({
            "include": ["foo", "reasoning.encrypted_content"]
        });

        let include = merge_include_with_encrypted_reasoning(body.as_object().unwrap());
        assert_eq!(include, json!(["foo", "reasoning.encrypted_content"]));
    }

    #[test]
    fn builds_next_round_body_from_replay_tail() {
        let body = json!({
            "input": [{"type": "message", "role": "user"}],
            "include": ["foo"],
            "previous_response_id": "resp-1"
        });
        let replay_tail = vec![commentary_message("Continue thinking.")];

        let next = build_next_round_body(body.as_object().unwrap(), &replay_tail).unwrap();
        assert_eq!(
            next,
            json!({
                "input": [
                    {"type": "message", "role": "user"},
                    {
                        "type": "message",
                        "role": "assistant",
                        "phase": "commentary",
                        "content": [{"type": "output_text", "text": "Continue thinking."}]
                    }
                ],
                "include": ["foo", "reasoning.encrypted_content"]
            })
        );
    }

    #[test]
    fn reconstructs_usage_as_one_logical_response() {
        let first_usage = json!({
            "input_tokens": 10,
            "input_tokens_details": {"cached_tokens": 3},
            "output_tokens": 520,
            "output_tokens_details": {"reasoning_tokens": 516},
            "total_tokens": 530
        });
        let second_usage = json!({
            "input_tokens": 12,
            "output_tokens": 40,
            "output_tokens_details": {"reasoning_tokens": 20},
            "total_tokens": 52
        });
        let mut total = Map::new();
        sum_usage(&mut total, Some(&first_usage));
        sum_usage(&mut total, Some(&second_usage));

        let reconstructed = reconstruct_usage(Some(&first_usage), &total, Some(&second_usage));
        assert_eq!(
            reconstructed,
            json!({
                "input_tokens": 10,
                "input_tokens_details": {"cached_tokens": 3},
                "output_tokens": 556,
                "output_tokens_details": {"reasoning_tokens": 536},
                "total_tokens": 566
            })
        );
        assert_eq!(reasoning_tokens_from_usage(Some(&second_usage)), Some(20));
    }
}
