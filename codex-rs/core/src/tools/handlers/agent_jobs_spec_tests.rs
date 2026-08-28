use super::*;
use codex_tools::ToolSpec;
use pretty_assertions::assert_eq;

fn function_spec(tool: ToolSpec) -> codex_tools::ResponsesApiTool {
    match tool {
        ToolSpec::Function(spec) => spec,
        other => panic!("expected function tool spec, got {other:?}"),
    }
}

#[test]
fn spawn_agents_on_csv_spec_requires_csv_path_and_instruction() {
    let spec = function_spec(create_spawn_agents_on_csv_tool());
    let params = spec.parameters;
    let required = params.required.expect("required fields");
    let properties = params.properties.expect("properties");

    assert_eq!(spec.name, "spawn_agents_on_csv");
    assert_eq!(
        required,
        vec!["csv_path".to_string(), "instruction".to_string()]
    );
    assert!(properties.contains_key("id_column"));
    assert!(properties.contains_key("max_concurrency"));
    assert!(properties.contains_key("max_workers"));
    assert!(properties.contains_key("max_runtime_seconds"));
    assert!(properties.contains_key("output_schema"));
    assert!(properties.contains_key("output_csv_path"));
}

#[test]
fn report_agent_job_result_spec_requires_job_item_and_result() {
    let spec = function_spec(create_report_agent_job_result_tool());
    let params = spec.parameters;
    let required = params.required.expect("required fields");
    let properties = params.properties.expect("properties");

    assert_eq!(spec.name, "report_agent_job_result");
    assert_eq!(
        required,
        vec![
            "job_id".to_string(),
            "item_id".to_string(),
            "result".to_string(),
        ]
    );
    assert!(properties.contains_key("job_id"));
    assert!(properties.contains_key("item_id"));
    assert!(properties.contains_key("result"));
    assert!(properties.contains_key("stop"));
}
