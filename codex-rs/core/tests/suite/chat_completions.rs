use codex_model_provider_info::ModelProviderInfo;
use codex_model_provider_info::WireApi;
use codex_protocol::openai_models::ReasoningEffort;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::test_codex;
use pretty_assertions::assert_eq;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;
use wiremock::matchers::path;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chat_provider_uses_native_endpoint_and_kimi_k3_effort() {
    skip_if_no_network!();

    let server = MockServer::start().await;
    let response = concat!(
        "data: {\"id\":\"chat-1\",\"choices\":[{\"delta\":{\"reasoning_content\":\"think\"}}]}\n\n",
        "data: {\"id\":\"chat-1\",\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}]}\n\n",
        "data: {\"id\":\"chat-1\",\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":7,\"total_tokens\":18}}\n\n",
        "data: [DONE]\n\n",
    );
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(response, "text/event-stream"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let provider = ModelProviderInfo {
        name: "Kimi".to_string(),
        base_url: Some(format!("{}/v1", server.uri())),
        env_key: Some("PATH".to_string()),
        env_key_instructions: None,
        experimental_bearer_token: None,
        auth: None,
        aws: None,
        wire_api: WireApi::Chat,
        query_params: None,
        http_headers: None,
        env_http_headers: None,
        request_max_retries: Some(0),
        stream_max_retries: Some(0),
        stream_idle_timeout_ms: Some(2_000),
        websocket_connect_timeout_ms: None,
        requires_openai_auth: false,
        supports_websockets: false,
        supports_standalone_web_search: false,
    };

    let test = test_codex()
        .with_model("kimi-k3")
        .with_config(move |config| {
            config.model_provider_id = "kimi".to_string();
            config.model_provider = provider;
            config.model_reasoning_effort = Some(ReasoningEffort::XHigh);
        })
        .build(&server)
        .await
        .expect("build test Codex");

    test.submit_turn("hello kimi").await.expect("submit turn");

    let requests = server.received_requests().await.expect("requests");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].url.path(), "/v1/chat/completions");
    let body: serde_json::Value = serde_json::from_slice(&requests[0].body).expect("request JSON");
    assert_eq!(body["model"], "kimi-k3");
    assert_eq!(body["reasoning_effort"], "max");
    assert_eq!(body["thinking"]["type"], "enabled");
    assert!(body["messages"].as_array().is_some_and(|messages| {
        messages
            .iter()
            .any(|message| message["content"] == "hello kimi")
    }));
}
