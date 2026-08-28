use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use codex_api::AgentIdentityTelemetry;
use codex_api::CompatibleModelInfo;
use codex_api::ModelsClient;
use codex_api::RequestTelemetry;
use codex_api::ReqwestTransport;
use codex_api::TransportError;
use codex_api::auth_header_telemetry;
use codex_api::map_api_error;
use codex_feedback::FeedbackRequestTags;
use codex_feedback::emit_feedback_request_tags_with_auth_env;
use codex_http_client::ClientRouteClass;
use codex_http_client::HttpClientFactory;
use codex_login::AuthEnvTelemetry;
use codex_login::AuthManager;
use codex_login::CodexAuth;
use codex_login::collect_auth_env_telemetry;
use codex_login::default_client::create_client_for_route_async;
use codex_model_provider_info::ModelProviderInfo;
use codex_model_provider_info::WireApi;
use codex_models_manager::manager::ModelsEndpointClient;
use codex_models_manager::manager::ModelsEndpointFuture;
use codex_otel::TelemetryAuthMode;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CoreResult;
use codex_protocol::openai_models::InputModality;
use codex_protocol::openai_models::ModelInfo;
use codex_protocol::openai_models::ModelVisibility;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::openai_models::ReasoningEffortPreset;
use codex_response_debug_context::extract_response_debug_context;
use codex_response_debug_context::telemetry_transport_error_message;
use http::HeaderMap;
use tokio::time::timeout;

use crate::auth::agent_identity_telemetry;
use crate::auth::resolve_provider_auth;
use crate::provider::enforce_managed_residency;

const MODELS_REFRESH_TIMEOUT: Duration = Duration::from_secs(5);
const COMPATIBLE_MODELS_REFRESH_TIMEOUT: Duration = Duration::from_secs(20);
const MODELS_ENDPOINT: &str = "/models";
const KIMI_CODEX_BEHAVIOR_PROFILE: &str = "gpt-5.5";

/// Provider-owned OpenAI-compatible `/models` endpoint.
#[derive(Debug)]
pub(crate) struct OpenAiModelsEndpoint {
    provider_info: ModelProviderInfo,
    auth_manager: Option<Arc<AuthManager>>,
    transport_builder: Arc<dyn ModelsTransportBuilder>,
}

impl OpenAiModelsEndpoint {
    pub(crate) fn new(
        provider_info: ModelProviderInfo,
        auth_manager: Option<Arc<AuthManager>>,
    ) -> Self {
        Self {
            provider_info,
            auth_manager,
            transport_builder: Arc::new(RouteAwareModelsTransportBuilder),
        }
    }

    async fn auth(&self) -> Option<CodexAuth> {
        match self.auth_manager.as_ref() {
            Some(auth_manager) => auth_manager.auth().await,
            None => None,
        }
    }

    async fn uses_codex_backend(&self) -> bool {
        self.auth()
            .await
            .as_ref()
            .is_some_and(CodexAuth::uses_codex_backend)
    }

    async fn list_models(
        &self,
        client_version: &str,
        http_client_factory: HttpClientFactory,
    ) -> CoreResult<(Vec<ModelInfo>, Option<String>)> {
        let _timer =
            codex_otel::start_global_timer("codex.remote_models.fetch_update.duration_ms", &[]);
        let auth = self.auth().await;
        let auth_mode = auth.as_ref().map(CodexAuth::auth_mode);
        let mut api_provider = self.provider_info.to_api_provider(auth_mode)?;
        enforce_managed_residency(&mut api_provider);
        let api_auth = resolve_provider_auth(auth.as_ref(), &self.provider_info)?;
        let request_url =
            ModelsClient::<ReqwestTransport>::request_url(&api_provider, client_version);
        let auth_telemetry = auth_header_telemetry(api_auth.as_ref());
        let agent_identity_telemetry = if let Some(CodexAuth::AgentIdentity(auth)) = auth.as_ref() {
            Some(agent_identity_telemetry(auth))
        } else {
            None
        };
        let request_telemetry: Arc<dyn RequestTelemetry> = Arc::new(ModelsRequestTelemetry {
            auth_mode: auth_mode.map(|mode| TelemetryAuthMode::from(mode).to_string()),
            auth_header_attached: auth_telemetry.attached,
            auth_header_name: auth_telemetry.name,
            agent_identity_telemetry,
            auth_env: self.auth_env(),
        });
        timeout(models_refresh_timeout(&self.provider_info), async {
            let transport = self
                .transport_builder
                .build(http_client_factory, request_url.clone())
                .await?;
            let client = ModelsClient::new(transport, api_provider, api_auth)
                .with_telemetry(Some(request_telemetry));
            if self.provider_info.wire_api == WireApi::Chat {
                let behavior_profile = kimi_codex_behavior_profile(&self.provider_info);
                let (models, etag) = client
                    .list_compatible_models(request_url, HeaderMap::new())
                    .await
                    .map_err(map_api_error)?;
                Ok((
                    models
                        .into_iter()
                        .enumerate()
                        .map(|(priority, model)| {
                            compatible_model_info(model, priority, behavior_profile.as_ref())
                        })
                        .collect(),
                    etag,
                ))
            } else {
                client
                    .list_models(request_url, HeaderMap::new())
                    .await
                    .map_err(map_api_error)
            }
        })
        .await
        .map_err(|_| CodexErr::Timeout)?
    }

    fn auth_env(&self) -> AuthEnvTelemetry {
        let codex_api_key_env_enabled = self
            .auth_manager
            .as_ref()
            .is_some_and(|auth_manager| auth_manager.codex_api_key_env_enabled());
        collect_auth_env_telemetry(&self.provider_info, codex_api_key_env_enabled)
    }
}

fn models_refresh_timeout(provider_info: &ModelProviderInfo) -> Duration {
    if provider_info.wire_api == WireApi::Chat {
        COMPATIBLE_MODELS_REFRESH_TIMEOUT
    } else {
        MODELS_REFRESH_TIMEOUT
    }
}

fn compatible_model_info(
    model: CompatibleModelInfo,
    priority: usize,
    behavior_profile: Option<&ModelInfo>,
) -> ModelInfo {
    let model_id = model.id.clone();
    let mut info = codex_models_manager::model_info::compatible_model_info_from_slug(&model.id);
    if let Some(behavior_profile) = behavior_profile {
        info.model_messages
            .clone_from(&behavior_profile.model_messages);
        info.include_skills_usage_instructions = behavior_profile.include_skills_usage_instructions;
        info.include_plugin_usage_instructions = behavior_profile.include_plugin_usage_instructions;
        info.include_apps_usage_instructions = behavior_profile.include_apps_usage_instructions;
        info.tool_mode = behavior_profile.tool_mode;
        info.multi_agent_version = behavior_profile.multi_agent_version;
        info.multi_agent_reasoning_effort
            .clone_from(&behavior_profile.multi_agent_reasoning_effort);
    }
    info.visibility = ModelVisibility::List;
    info.priority = i32::try_from(priority).unwrap_or(i32::MAX);
    info.context_window = model.context_length;
    info.max_context_window = model.context_length;
    info.auto_review_model_override = Some(model_id);
    info.input_modalities = if model.supports_image_in {
        vec![InputModality::Text, InputModality::Image]
    } else {
        vec![InputModality::Text]
    };
    info.default_reasoning_level = None;
    info.supported_reasoning_levels.clear();
    if model.supports_reasoning
        && let Some(efforts) = model.reasoning_efforts
        && efforts.support
    {
        info.default_reasoning_level = efforts.default_effort;
        info.supported_reasoning_levels = efforts
            .valid_efforts
            .into_iter()
            .map(|effort| ReasoningEffortPreset {
                description: effort.to_string(),
                effort,
            })
            .collect();
    } else if model.supports_reasoning {
        info.default_reasoning_level = Some(ReasoningEffort::High);
        info.supported_reasoning_levels = [
            ReasoningEffort::Low,
            ReasoningEffort::High,
            ReasoningEffort::Max,
        ]
        .into_iter()
        .map(|effort| ReasoningEffortPreset {
            description: effort.to_string(),
            effort,
        })
        .collect();
    }
    info
}

fn kimi_codex_behavior_profile(provider_info: &ModelProviderInfo) -> Option<ModelInfo> {
    if !is_kimi_provider(provider_info) {
        return None;
    }
    codex_models_manager::bundled_models_response()
        .ok()?
        .models
        .into_iter()
        .find(|model| model.slug == KIMI_CODEX_BEHAVIOR_PROFILE)
}

fn is_kimi_provider(provider_info: &ModelProviderInfo) -> bool {
    provider_info.name.eq_ignore_ascii_case("kimi")
        || provider_info.base_url.as_deref().is_some_and(|base_url| {
            let base_url = base_url.to_ascii_lowercase();
            base_url.contains("moonshot.cn") || base_url.contains("moonshot.ai")
        })
}

impl ModelsEndpointClient for OpenAiModelsEndpoint {
    fn has_command_auth(&self) -> bool {
        self.provider_info.has_command_auth()
    }

    fn uses_codex_backend(&self) -> ModelsEndpointFuture<'_, bool> {
        Box::pin(OpenAiModelsEndpoint::uses_codex_backend(self))
    }

    fn list_models<'a>(
        &'a self,
        client_version: &'a str,
        http_client_factory: HttpClientFactory,
    ) -> ModelsEndpointFuture<'a, CoreResult<(Vec<ModelInfo>, Option<String>)>> {
        Box::pin(OpenAiModelsEndpoint::list_models(
            self,
            client_version,
            http_client_factory,
        ))
    }
}

type ModelsTransportFuture<'a> =
    Pin<Box<dyn Future<Output = std::io::Result<ReqwestTransport>> + Send + 'a>>;

/// Builds the concrete transport selected for one models request.
///
/// Implementations must honor the supplied request-time client factory and exact request URL.
trait ModelsTransportBuilder: fmt::Debug + Send + Sync {
    fn build(
        &self,
        http_client_factory: HttpClientFactory,
        request_url: String,
    ) -> ModelsTransportFuture<'_>;
}

#[derive(Debug)]
struct RouteAwareModelsTransportBuilder;

impl ModelsTransportBuilder for RouteAwareModelsTransportBuilder {
    fn build(
        &self,
        http_client_factory: HttpClientFactory,
        request_url: String,
    ) -> ModelsTransportFuture<'_> {
        Box::pin(async move {
            create_client_for_route_async(http_client_factory, request_url, ClientRouteClass::Api)
                .await
                .map(ReqwestTransport::from_http_client)
        })
    }
}

#[derive(Clone)]
struct ModelsRequestTelemetry {
    auth_mode: Option<String>,
    auth_header_attached: bool,
    auth_header_name: Option<&'static str>,
    agent_identity_telemetry: Option<AgentIdentityTelemetry>,
    auth_env: AuthEnvTelemetry,
}

impl RequestTelemetry for ModelsRequestTelemetry {
    fn on_request(
        &self,
        attempt: u64,
        status: Option<http::StatusCode>,
        error: Option<&TransportError>,
        duration: Duration,
    ) {
        let success = status.is_some_and(|code| code.is_success()) && error.is_none();
        let error_message = error.map(telemetry_transport_error_message);
        let response_debug = error
            .map(extract_response_debug_context)
            .unwrap_or_default();
        let status = status.map(|status| status.as_u16());
        tracing::event!(
            target: "codex_otel.log_only",
            tracing::Level::INFO,
            event.name = "codex.api_request",
            duration_ms = %duration.as_millis(),
            http.response.status_code = status,
            success = success,
            error.message = error_message.as_deref(),
            attempt = attempt,
            endpoint = MODELS_ENDPOINT,
            auth.header_attached = self.auth_header_attached,
            auth.header_name = self.auth_header_name,
            auth.env_openai_api_key_present = self.auth_env.openai_api_key_env_present,
            auth.env_codex_api_key_present = self.auth_env.codex_api_key_env_present,
            auth.env_codex_api_key_enabled = self.auth_env.codex_api_key_env_enabled,
            auth.env_provider_key_name = self.auth_env.provider_env_key_name.as_deref(),
            auth.env_provider_key_present = self.auth_env.provider_env_key_present,
            auth.env_refresh_token_url_override_present = self.auth_env.refresh_token_url_override_present,
            auth.request_id = response_debug.request_id.as_deref(),
            auth.cf_ray = response_debug.cf_ray.as_deref(),
            auth.error = response_debug.auth_error.as_deref(),
            auth.error_code = response_debug.auth_error_code.as_deref(),
            auth.mode = self.auth_mode.as_deref(),
            auth.agent_id = self.agent_identity_telemetry.as_ref().map(|metadata| metadata.agent_id.as_str()),
            auth.task_id = self.agent_identity_telemetry.as_ref().map(|metadata| metadata.task_id.as_str()),
        );
        tracing::event!(
            target: "codex_otel.trace_safe",
            tracing::Level::INFO,
            event.name = "codex.api_request",
            duration_ms = %duration.as_millis(),
            http.response.status_code = status,
            success = success,
            error.message = error_message.as_deref(),
            attempt = attempt,
            endpoint = MODELS_ENDPOINT,
            auth.header_attached = self.auth_header_attached,
            auth.header_name = self.auth_header_name,
            auth.env_openai_api_key_present = self.auth_env.openai_api_key_env_present,
            auth.env_codex_api_key_present = self.auth_env.codex_api_key_env_present,
            auth.env_codex_api_key_enabled = self.auth_env.codex_api_key_env_enabled,
            auth.env_provider_key_name = self.auth_env.provider_env_key_name.as_deref(),
            auth.env_provider_key_present = self.auth_env.provider_env_key_present,
            auth.env_refresh_token_url_override_present = self.auth_env.refresh_token_url_override_present,
            auth.request_id = response_debug.request_id.as_deref(),
            auth.cf_ray = response_debug.cf_ray.as_deref(),
            auth.error = response_debug.auth_error.as_deref(),
            auth.error_code = response_debug.auth_error_code.as_deref(),
            auth.mode = self.auth_mode.as_deref(),
            auth.agent_id = self.agent_identity_telemetry.as_ref().map(|metadata| metadata.agent_id.as_str()),
            auth.task_id = self.agent_identity_telemetry.as_ref().map(|metadata| metadata.task_id.as_str()),
        );
        emit_feedback_request_tags_with_auth_env(
            &FeedbackRequestTags {
                endpoint: MODELS_ENDPOINT,
                auth_header_attached: self.auth_header_attached,
                auth_header_name: self.auth_header_name,
                auth_mode: self.auth_mode.as_deref(),
                auth_retry_after_unauthorized: None,
                auth_recovery_mode: None,
                auth_recovery_phase: None,
                auth_connection_reused: None,
                auth_request_id: response_debug.request_id.as_deref(),
                auth_cf_ray: response_debug.cf_ray.as_deref(),
                auth_error: response_debug.auth_error.as_deref(),
                auth_error_code: response_debug.auth_error_code.as_deref(),
                auth_recovery_followup_success: None,
                auth_recovery_followup_status: None,
            },
            &self.auth_env,
        );
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::Mutex;

    use super::*;
    use codex_http_client::OutboundProxyPolicy;
    use codex_login::default_client::RESIDENCY_HEADER_NAME;
    use codex_login::default_client::ResidencyRequirement;
    use codex_login::default_client::create_client;
    use codex_login::default_client::set_default_client_residency_requirement;
    use codex_protocol::config_types::ModelProviderAuthInfo;
    use codex_protocol::openai_models::ModelsResponse;
    use pretty_assertions::assert_eq;
    use wiremock::Mock;
    use wiremock::MockServer;
    use wiremock::ResponseTemplate;
    use wiremock::matchers::header;
    use wiremock::matchers::method;
    use wiremock::matchers::path;
    use wiremock::matchers::query_param;

    #[derive(Debug)]
    struct RecordingTransportBuilder {
        observed_request: Arc<Mutex<Option<(OutboundProxyPolicy, String)>>>,
    }

    impl ModelsTransportBuilder for RecordingTransportBuilder {
        fn build(
            &self,
            http_client_factory: HttpClientFactory,
            request_url: String,
        ) -> ModelsTransportFuture<'_> {
            let observed_request = Arc::clone(&self.observed_request);
            Box::pin(async move {
                *observed_request
                    .lock()
                    .expect("observed request lock should not be poisoned") =
                    Some((http_client_factory.outbound_proxy_policy(), request_url));
                Ok(ReqwestTransport::from_http_client(create_client()))
            })
        }
    }

    fn provider_info_with_command_auth() -> ModelProviderInfo {
        ModelProviderInfo {
            auth: Some(ModelProviderAuthInfo {
                command: "print-token".to_string(),
                args: Vec::new(),
                timeout_ms: NonZeroU64::new(5_000).expect("timeout should be non-zero"),
                refresh_interval_ms: 300_000,
                cwd: std::env::current_dir()
                    .expect("current dir should be available")
                    .try_into()
                    .expect("current dir should be absolute"),
            }),
            requires_openai_auth: false,
            ..ModelProviderInfo::create_openai_provider(/*base_url*/ None)
        }
    }

    #[test]
    fn command_auth_provider_reports_command_auth_without_cached_auth() {
        let endpoint = OpenAiModelsEndpoint::new(
            provider_info_with_command_auth(),
            /*auth_manager*/ None,
        );

        assert!(endpoint.has_command_auth());
    }

    #[test]
    fn provider_without_command_auth_reports_no_command_auth() {
        let endpoint = OpenAiModelsEndpoint::new(
            ModelProviderInfo::create_openai_provider(/*base_url*/ None),
            /*auth_manager*/ None,
        );

        assert!(!endpoint.has_command_auth());
    }

    #[tokio::test]
    async fn model_request_uses_request_time_proxy_policy_and_exact_url() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/models"))
            .and(query_param("client_version", "0.0.0"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(ModelsResponse { models: Vec::new() }),
            )
            .expect(1)
            .mount(&server)
            .await;

        let observed_request = Arc::new(Mutex::new(None));
        let endpoint = OpenAiModelsEndpoint {
            provider_info: ModelProviderInfo::create_openai_provider(Some(server.uri())),
            auth_manager: None,
            transport_builder: Arc::new(RecordingTransportBuilder {
                observed_request: Arc::clone(&observed_request),
            }),
        };

        endpoint
            .list_models(
                "0.0.0",
                HttpClientFactory::new(OutboundProxyPolicy::RespectSystemProxy),
            )
            .await
            .expect("models request should succeed");

        assert_eq!(
            *observed_request
                .lock()
                .expect("observed request lock should not be poisoned"),
            Some((
                OutboundProxyPolicy::RespectSystemProxy,
                format!("{}/models?client_version=0.0.0", server.uri()),
            ))
        );
    }

    #[test]
    fn compatible_model_metadata_preserves_kimi_capabilities() {
        let model = compatible_model_info(
            CompatibleModelInfo {
                id: "kimi-k3".to_string(),
                context_length: Some(1_048_576),
                supports_image_in: true,
                supports_reasoning: true,
                supports_dynamic_tools: true,
                reasoning_efforts: Some(codex_api::CompatibleReasoningEfforts {
                    support: true,
                    valid_efforts: vec![
                        ReasoningEffort::Low,
                        ReasoningEffort::High,
                        ReasoningEffort::Max,
                    ],
                    default_effort: Some(ReasoningEffort::Max),
                }),
            },
            0,
            None,
        );

        assert_eq!(model.slug, "kimi-k3");
        assert_eq!(model.visibility, ModelVisibility::List);
        assert_eq!(model.context_window, Some(1_048_576));
        assert_eq!(model.default_reasoning_level, Some(ReasoningEffort::Max));
        assert_eq!(model.supported_reasoning_levels.len(), 3);
        assert_eq!(
            model.input_modalities,
            vec![InputModality::Text, InputModality::Image]
        );
        assert!(!model.used_fallback_model_metadata);
    }

    #[test]
    fn kimi_models_inherit_native_codex_behavior_without_tool_metadata() {
        let provider_info = ModelProviderInfo {
            name: "Kimi".to_string(),
            base_url: Some("https://api.moonshot.cn/v1".to_string()),
            wire_api: WireApi::Chat,
            ..ModelProviderInfo::create_openai_provider(None)
        };
        let behavior_profile =
            kimi_codex_behavior_profile(&provider_info).expect("Kimi behavior profile");
        let model = compatible_model_info(
            CompatibleModelInfo {
                id: "kimi-k3".to_string(),
                context_length: Some(1_048_576),
                supports_image_in: false,
                supports_reasoning: true,
                supports_dynamic_tools: true,
                reasoning_efforts: None,
            },
            0,
            Some(&behavior_profile),
        );

        assert_eq!(model.model_messages, behavior_profile.model_messages);
        assert_eq!(model.apply_patch_tool_type, None);
        assert_eq!(model.context_window, Some(1_048_576));
        assert_eq!(model.tool_mode, behavior_profile.tool_mode);
        assert_eq!(
            model.multi_agent_version,
            behavior_profile.multi_agent_version
        );
    }

    #[test]
    fn compatible_model_refresh_allows_provider_startup_latency() {
        let mut provider_info = ModelProviderInfo::create_openai_provider(None);
        assert_eq!(
            models_refresh_timeout(&provider_info),
            MODELS_REFRESH_TIMEOUT
        );

        provider_info.wire_api = WireApi::Chat;
        assert_eq!(
            models_refresh_timeout(&provider_info),
            COMPATIBLE_MODELS_REFRESH_TIMEOUT
        );
    }

    #[test]
    fn kimi_reasoning_models_without_effort_metadata_default_to_thinking() {
        let model = compatible_model_info(
            CompatibleModelInfo {
                id: "kimi-k2.7-code".to_string(),
                context_length: Some(262_144),
                supports_image_in: false,
                supports_reasoning: true,
                supports_dynamic_tools: false,
                reasoning_efforts: None,
            },
            0,
            None,
        );

        assert_eq!(model.default_reasoning_level, Some(ReasoningEffort::High));
        assert_eq!(
            model
                .supported_reasoning_levels
                .iter()
                .map(|preset| preset.effort.clone())
                .collect::<Vec<_>>(),
            vec![
                ReasoningEffort::Low,
                ReasoningEffort::High,
                ReasoningEffort::Max
            ]
        );
        assert!(model.supports_reasoning_summary_parameter);
    }

    #[tokio::test]
    async fn model_discovery_enforces_managed_residency_over_provider_headers() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/models"))
            .and(header(RESIDENCY_HEADER_NAME, "us"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(ModelsResponse { models: Vec::new() }),
            )
            .expect(1)
            .mount(&server)
            .await;

        let mut provider_info = ModelProviderInfo::create_openai_provider(Some(server.uri()));
        provider_info.http_headers = Some(std::collections::HashMap::from([(
            RESIDENCY_HEADER_NAME.to_string(),
            "eu".into(),
        )]));
        let endpoint = OpenAiModelsEndpoint {
            provider_info,
            auth_manager: None,
            transport_builder: Arc::new(RecordingTransportBuilder {
                observed_request: Arc::new(Mutex::new(None)),
            }),
        };

        set_default_client_residency_requirement(Some(ResidencyRequirement::Us));
        endpoint
            .list_models(
                "0.0.0",
                HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault),
            )
            .await
            .expect("managed residency model discovery should succeed");
        set_default_client_residency_requirement(/*enforce_residency*/ None);

        assert_eq!(
            endpoint
                .provider_info
                .http_headers
                .as_ref()
                .and_then(|headers| headers.get(RESIDENCY_HEADER_NAME)),
            Some(&"eu".into())
        );
    }
}
