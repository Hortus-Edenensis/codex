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
use codex_login::GatewayAuthManager;
use codex_login::collect_auth_env_telemetry;
use codex_login::default_client::ClientRedirectPolicy;
use codex_login::default_client::create_client_for_route_async;
use codex_model_provider_info::CHATGPT_CODEX_BASE_URL;
use codex_model_provider_info::ModelProviderInfo;
use codex_model_provider_info::WireApi;
use codex_models_manager::manager::ModelsEndpointClient;
use codex_models_manager::manager::ModelsEndpointFuture;
use codex_models_manager::manager::ModelsEndpointResponse;
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

use crate::auth::ResolvedProviderAuth;
use crate::auth::agent_identity_telemetry;
use crate::auth::resolve_provider_auth;
use crate::combined_auth::compose_auth;

const MODELS_REFRESH_TIMEOUT: Duration = Duration::from_secs(5);
const COMPATIBLE_MODELS_REFRESH_TIMEOUT: Duration = Duration::from_secs(20);
const MODELS_ENDPOINT: &str = "/models";
// Bound downloads from explicitly configured catalogs before decoding or caching them.
const MAX_MODEL_CATALOG_BYTES: usize = 1024 * 1024;
const KIMI_CODEX_BEHAVIOR_PROFILE: &str = "gpt-5.5";

/// Provider-owned OpenAI-compatible `/models` endpoint.
#[derive(Debug)]
pub(crate) struct OpenAiModelsEndpoint {
    provider_info: ModelProviderInfo,
    auth_manager: Option<Arc<AuthManager>>,
    gateway_auth_manager: Option<Result<Arc<GatewayAuthManager>, String>>,
    transport_builder: Arc<dyn ModelsTransportBuilder>,
}

impl OpenAiModelsEndpoint {
    pub(crate) fn new(
        provider_info: ModelProviderInfo,
        auth_manager: Option<Arc<AuthManager>>,
        gateway_auth_manager: Option<Result<Arc<GatewayAuthManager>, String>>,
    ) -> Self {
        let redirect_policy = if provider_info.model_catalog_url.is_some() {
            ClientRedirectPolicy::Reject
        } else {
            ClientRedirectPolicy::Default
        };
        Self {
            provider_info,
            auth_manager,
            gateway_auth_manager,
            transport_builder: Arc::new(RouteAwareModelsTransportBuilder { redirect_policy }),
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
    ) -> CoreResult<ModelsEndpointResponse> {
        let auth = self.auth().await;
        let metric_auth_mode = if self.has_provider_api_key()
            || auth.as_ref().is_some_and(CodexAuth::is_api_key_auth)
        {
            "api_key"
        } else if auth.is_some() {
            "chatgpt"
        } else {
            "none"
        };
        let _timer = codex_otel::start_global_timer(
            "codex.remote_models.fetch_update.duration_ms",
            &[("auth_mode", metric_auth_mode)],
        );
        let identity = crate::models_identity::identity(&self.provider_info, auth.as_ref())?;
        let auth_mode = auth.as_ref().map(CodexAuth::auth_mode);
        let mut api_provider = self.provider_info.to_api_provider(auth_mode)?;
        if (auth.as_ref().is_some_and(CodexAuth::is_api_key_auth) || self.has_provider_api_key())
            && self.supports_api_key_models()
            && self.provider_info.base_url.is_none()
            && self.provider_info.model_catalog_url.is_none()
        {
            // Codex metadata is served by the Codex backend, not the public /v1/models API.
            api_provider.base_url = CHATGPT_CODEX_BASE_URL.to_string();
        }
        let resolved = compose_auth(
            &self.provider_info,
            self.gateway_auth_manager.as_ref(),
            ResolvedProviderAuth::new(resolve_provider_auth(auth.as_ref(), &self.provider_info)?),
        )
        .await?;
        let api_auth = resolved.auth;
        let request_url = match self.provider_info.model_catalog_url.as_deref() {
            Some(catalog_url) => ModelsClient::<ReqwestTransport>::catalog_request_url(
                &api_provider,
                catalog_url,
                client_version,
            )
            .map_err(map_api_error)?,
            None => ModelsClient::<ReqwestTransport>::request_url(&api_provider, client_version),
        };
        let auth_telemetry = auth_header_telemetry(api_auth.as_ref());
        let agent_identity_telemetry = if let Some(CodexAuth::AgentIdentity(auth)) = auth.as_ref() {
            Some(agent_identity_telemetry(auth))
        } else {
            None
        };
        let request_telemetry: Arc<dyn RequestTelemetry> = Arc::new(ModelsRequestTelemetry {
            include_response_debug: self.provider_info.model_catalog_url.is_none(),
            auth_mode: auth_mode.map(|mode| TelemetryAuthMode::from(mode).to_string()),
            auth_header_attached: auth_telemetry.attached,
            auth_header_name: auth_telemetry.name,
            agent_identity_telemetry,
            auth_env: self.auth_env(),
        });
        let (models, etag) = timeout(models_refresh_timeout(&self.provider_info), async {
            let transport = self
                .transport_builder
                .build(http_client_factory, request_url.clone())
                .await?;
            let client = ModelsClient::new(transport, api_provider, api_auth)
                .with_telemetry(Some(request_telemetry));
            if self.provider_info.wire_api == WireApi::Chat
                && self.provider_info.model_catalog_url.is_none()
            {
                let behavior_profile = kimi_codex_behavior_profile(&self.provider_info);
                let (models, etag) = client
                    .list_compatible_models(request_url, HeaderMap::new())
                    .await
                    .map_err(map_api_error)?;
                return Ok((
                    models
                        .into_iter()
                        .enumerate()
                        .map(|(priority, model)| {
                            compatible_model_info(model, priority, behavior_profile.as_ref())
                        })
                        .collect(),
                    etag,
                ));
            }
            let response_body_limit_bytes = self
                .provider_info
                .model_catalog_url
                .as_ref()
                .map(|_| MAX_MODEL_CATALOG_BYTES);
            client
                .list_models(request_url, HeaderMap::new(), response_body_limit_bytes)
                .await
                .map_err(|mut error| {
                    if self.provider_info.model_catalog_url.is_some()
                        && let codex_api::ApiError::Transport(TransportError::Http {
                            url,
                            headers,
                            body,
                            ..
                        }) = &mut error
                    {
                        // Provider diagnostics may echo URL credentials or other secrets.
                        *url = None;
                        *headers = None;
                        *body = None;
                    }
                    map_api_error(error)
                })
        })
        .await
        .map_err(|_| CodexErr::RequestTimeout)??;
        Ok(ModelsEndpointResponse {
            models,
            etag,
            identity,
        })
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
    }
    info.visibility = ModelVisibility::List;
    info.priority = i32::try_from(priority).unwrap_or(i32::MAX);
    info.context_window = model.context_length;
    info.max_context_window = model.context_length;
    info.auto_review_model_override = Some(model_id);
    if model.supports_image_in {
        info.input_modalities = vec![InputModality::Text, InputModality::Image];
    } else {
        info.input_modalities = vec![InputModality::Text];
    }
    info.default_reasoning_level = None;
    info.supported_reasoning_levels.clear();
    info.supports_reasoning_summary_parameter = false;
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
        || provider_info
            .base_url
            .as_deref()
            .is_some_and(|base_url| base_url.to_ascii_lowercase().contains("moonshot.cn"))
}

impl ModelsEndpointClient for OpenAiModelsEndpoint {
    fn supports_compatible_models(&self) -> bool {
        self.provider_info.wire_api == WireApi::Chat
    }

    fn supports_api_key_models(&self) -> bool {
        self.provider_info.model_catalog_url.is_some()
            || (self.provider_info.is_openai() && self.provider_info.base_url.is_none())
    }

    fn has_provider_api_key(&self) -> bool {
        self.provider_info.env_key.is_some()
            || self.provider_info.experimental_bearer_token.is_some()
    }

    fn identity(&self) -> Option<String> {
        let auth = self
            .auth_manager
            .as_ref()
            .and_then(|manager| manager.auth_cached());
        crate::models_identity::identity(&self.provider_info, auth.as_ref()).ok()
    }

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
    ) -> ModelsEndpointFuture<'a, CoreResult<ModelsEndpointResponse>> {
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
struct RouteAwareModelsTransportBuilder {
    redirect_policy: ClientRedirectPolicy,
}

impl ModelsTransportBuilder for RouteAwareModelsTransportBuilder {
    fn build(
        &self,
        http_client_factory: HttpClientFactory,
        request_url: String,
    ) -> ModelsTransportFuture<'_> {
        let redirect_policy = self.redirect_policy;
        Box::pin(async move {
            let client = create_client_for_route_async(
                http_client_factory,
                request_url,
                ClientRouteClass::Api,
                redirect_policy,
            )
            .await?;
            let client = match redirect_policy {
                ClientRedirectPolicy::Default => client,
                ClientRedirectPolicy::Reject => client.without_request_logging(),
            };
            Ok(ReqwestTransport::from_http_client(client))
        })
    }
}

#[derive(Clone)]
struct ModelsRequestTelemetry {
    include_response_debug: bool,
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
            .filter(|_| self.include_response_debug)
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
#[path = "models_endpoint_timeout_tests.rs"]
mod timeout_tests;

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
    use codex_models_manager::manager::ModelsManager;
    use codex_models_manager::manager::OpenAiModelsManager;
    use codex_models_manager::manager::RefreshStrategy;
    use codex_protocol::auth::AuthMode;
    use codex_protocol::config_types::ModelProviderAuthInfo;
    use codex_protocol::error::CodexErrorDetails;
    use codex_protocol::openai_models::ModelVisibility;
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

    #[derive(Debug)]
    struct CaptureModelsUrl(Mutex<Option<String>>);

    impl ModelsTransportBuilder for CaptureModelsUrl {
        fn build(
            &self,
            _http_client_factory: HttpClientFactory,
            request_url: String,
        ) -> ModelsTransportFuture<'_> {
            *self.0.lock().unwrap() = Some(request_url);
            Box::pin(async { Err(std::io::Error::other("transport intentionally unavailable")) })
        }
    }

    #[tokio::test]
    async fn api_key_discovery_respects_provider_routing() {
        let client_version = codex_models_manager::client_version_to_whole();
        for (name, base_url, models_url, inference_url) in [
            (
                "OpenAI",
                None,
                Some("https://chatgpt.com/backend-api/codex/models"),
                "https://api.openai.com/v1",
            ),
            (
                "OpenAI",
                Some("https://example.com/codex"),
                None,
                "https://example.com/codex",
            ),
            (
                "Azure",
                Some("https://example.openai.azure.com/openai/v1"),
                None,
                "https://example.openai.azure.com/openai/v1",
            ),
        ] {
            let capture = Arc::new(CaptureModelsUrl(Mutex::new(/*t*/ None)));
            let auth = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("test-api-key"));
            let endpoint = Arc::new(OpenAiModelsEndpoint {
                provider_info: ModelProviderInfo {
                    name: name.to_string(),
                    ..ModelProviderInfo::create_openai_provider(base_url.map(str::to_string))
                },
                auth_manager: Some(auth.clone()),
                gateway_auth_manager: None,
                transport_builder: capture.clone(),
            });
            let manager = OpenAiModelsManager::new_without_cache(endpoint.clone(), Some(auth));
            manager.set_api_key_model_discovery_enabled(/*enabled*/ true);
            manager
                .raw_model_catalog(
                    RefreshStrategy::Online,
                    HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault),
                )
                .await;
            assert_eq!(
                *capture.0.lock().unwrap(),
                models_url.map(|url| format!("{url}?client_version={client_version}"))
            );
            assert_eq!(
                endpoint
                    .provider_info
                    .to_api_provider(Some(AuthMode::ApiKey))
                    .unwrap()
                    .base_url,
                inference_url
            );
        }
    }

    #[tokio::test]
    async fn provider_api_key_without_endpoint_overrides_uses_codex_backend() {
        for auth in [
            None,
            Some(CodexAuth::create_dummy_chatgpt_auth_for_testing()),
        ] {
            let capture = Arc::new(CaptureModelsUrl(Mutex::new(/*t*/ None)));
            let auth_manager = auth.map(AuthManager::from_auth_for_testing);
            let endpoint = Arc::new(OpenAiModelsEndpoint {
                provider_info: ModelProviderInfo {
                    experimental_bearer_token: Some("provider-key".into()),
                    ..ModelProviderInfo::create_openai_provider(/*base_url*/ None)
                },
                auth_manager: auth_manager.clone(),
                gateway_auth_manager: None,
                transport_builder: capture.clone(),
            });
            let manager = OpenAiModelsManager::new_without_cache(endpoint, auth_manager);
            manager.set_api_key_model_discovery_enabled(/*enabled*/ true);
            manager
                .raw_model_catalog(
                    RefreshStrategy::Online,
                    HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault),
                )
                .await;
            assert_eq!(
                *capture.0.lock().unwrap(),
                Some(format!(
                    "{CHATGPT_CODEX_BASE_URL}/models?client_version={}",
                    codex_models_manager::client_version_to_whole(),
                ))
            );
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
            /*gateway_auth_manager*/ None,
        );

        assert!(endpoint.has_command_auth());
    }

    #[test]
    fn provider_without_command_auth_reports_no_command_auth() {
        let endpoint = OpenAiModelsEndpoint::new(
            ModelProviderInfo::create_openai_provider(/*base_url*/ None),
            /*auth_manager*/ None,
            /*gateway_auth_manager*/ None,
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
            gateway_auth_manager: None,
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
            gateway_auth_manager: None,
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

    #[derive(Debug)]
    struct RotatingAuth(std::sync::atomic::AtomicUsize);

    impl codex_login::ExternalAuth for RotatingAuth {
        fn resolve(&self) -> codex_login::ExternalAuthFuture<'_, CodexAuth> {
            Box::pin(async move {
                let generation = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(CodexAuth::from_api_key(&format!("token-{generation}")))
            })
        }

        fn refresh(
            &self,
            _context: codex_login::ExternalAuthRefreshContext,
        ) -> codex_login::ExternalAuthFuture<'_, CodexAuth> {
            self.resolve()
        }
    }

    #[tokio::test]
    async fn command_auth_refresh_fetches_a_catalog_for_the_current_credentials() {
        use codex_models_manager::manager::ModelsManager;
        use codex_models_manager::manager::OpenAiModelsManager;
        use codex_models_manager::manager::RefreshStrategy;

        let server = MockServer::start().await;
        let auth = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("initial"));
        auth.set_external_auth(Arc::new(RotatingAuth(std::sync::atomic::AtomicUsize::new(
            0,
        ))))
        .await
        .unwrap();
        let model = codex_protocol::openai_models::ModelInfo {
            used_fallback_model_metadata: false,
            ..codex_models_manager::model_info::model_info_from_slug("command-auth-model")
        };
        Mock::given(method("GET"))
            .and(path("/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(ModelsResponse {
                models: vec![model.clone()],
            }))
            .expect(/*r*/ 3)
            .mount(&server)
            .await;
        let mut provider = provider_info_with_command_auth();
        provider.base_url = Some(server.uri());
        // Keep this test independent of the residency override exercised in parallel.
        provider.http_headers = Some(std::collections::HashMap::from([(
            RESIDENCY_HEADER_NAME.to_string(),
            "us".into(),
        )]));
        let manager = OpenAiModelsManager::new_without_cache(
            Arc::new(OpenAiModelsEndpoint::new(
                provider,
                Some(auth.clone()),
                /*gateway_auth_manager*/ None,
            )),
            Some(auth.clone()),
        );
        let catalog = manager
            .raw_model_catalog(
                RefreshStrategy::OnlineIfUncached,
                HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault),
            )
            .await;
        assert_eq!(
            catalog
                .models
                .iter()
                .find(|candidate| candidate.slug == model.slug),
            Some(&model)
        );
        // The cached identity still matches here; resolving command auth must
        // detect the next token before deciding whether a refresh is needed.
        manager
            .refresh_after_auth_change(HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault))
            .await;
        assert_eq!(manager.get_remote_models().await, catalog.models);
        auth.auth().await;
        let bundled = codex_models_manager::bundled_models_response().unwrap();
        assert_eq!(manager.get_remote_models().await, bundled.models);
        assert_eq!(manager.try_get_remote_models().unwrap(), bundled.models);
        assert_eq!(
            manager
                .raw_model_catalog(
                    RefreshStrategy::Offline,
                    HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault),
                )
                .await,
            bundled
        );
        assert_eq!(
            manager
                .raw_model_catalog(
                    RefreshStrategy::OnlineIfUncached,
                    HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault),
                )
                .await,
            catalog
        );
        let requests = server.received_requests().await.unwrap();
        assert_eq!(
            requests
                .iter()
                .map(|request| request.headers["authorization"].to_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["Bearer token-2", "Bearer token-5", "Bearer token-9"]
        );
    }

    #[tokio::test]
    async fn explicit_catalog_reuses_auth_headers_and_query_parameters() {
        for auth in [
            CodexAuth::create_dummy_chatgpt_auth_for_testing(),
            CodexAuth::from_api_key("test-key"),
        ] {
            let server = MockServer::start().await;
            let mut model =
                codex_models_manager::model_info::model_info_from_slug("provider-model");
            model.visibility = ModelVisibility::List;
            model.supported_in_api = true;
            model.used_fallback_model_metadata = false;
            let expected = ModelsResponse {
                models: vec![model],
            };
            Mock::given(method("GET"))
                .and(path("/codex/models"))
                .and(header(
                    "authorization",
                    format!("Bearer {}", auth.get_token().unwrap()),
                ))
                .and(header("x-provider-header", "preserved"))
                .and(query_param("deployment", "one"))
                .and(query_param("api-version", "2026-09"))
                .respond_with(ResponseTemplate::new(/*s*/ 200).set_body_json(&expected))
                .expect(1)
                .mount(&server)
                .await;
            let auth_manager = AuthManager::from_auth_for_testing(auth);
            // The parallel residency test must not change this catalog's cache identity.
            let provider = ModelProviderInfo {
                model_catalog_url: Some(
                    format!("{}/codex/models?deployment=one", server.uri()).into(),
                ),
                query_params: Some(std::collections::HashMap::from([(
                    "api-version".to_string(),
                    "2026-09".into(),
                )])),
                http_headers: Some(std::collections::HashMap::from([
                    ("x-provider-header".to_string(), "preserved".into()),
                    (
                        codex_login::default_client::RESIDENCY_HEADER_NAME.to_string(),
                        "us".into(),
                    ),
                ])),
                ..ModelProviderInfo::create_openai_provider(Some(format!("{}/v1", server.uri())))
            };
            let home = tempfile::tempdir().unwrap();
            let endpoint = Arc::new(OpenAiModelsEndpoint::new(
                provider,
                Some(auth_manager.clone()),
                /*gateway_auth_manager*/ None,
            ));
            let manager = OpenAiModelsManager::new(
                home.path().to_path_buf(),
                endpoint,
                Some(auth_manager.clone()),
            );
            // SIWC does not require the API-key rollout flag.
            manager.set_api_key_model_discovery_enabled(
                auth_manager.auth_mode() == Some(codex_protocol::auth::AuthMode::ApiKey),
            );
            for _ in 0..2 {
                assert_eq!(
                    manager
                        .raw_model_catalog(
                            RefreshStrategy::OnlineIfUncached,
                            HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault)
                        )
                        .await,
                    expected
                );
            }
        }
    }

    #[tokio::test]
    async fn explicit_catalog_rejects_oversized_response() {
        let server = MockServer::start().await;
        let mut body = br#"{"models":[]}"#.to_vec();
        body.resize(MAX_MODEL_CATALOG_BYTES + 1, b' ');
        Mock::given(method("GET"))
            .and(path("/codex/models"))
            .respond_with(ResponseTemplate::new(/*s*/ 200).set_body_bytes(body))
            .expect(1)
            .mount(&server)
            .await;
        let endpoint = OpenAiModelsEndpoint::new(
            ModelProviderInfo {
                model_catalog_url: Some(format!("{}/codex/models", server.uri()).into()),
                ..ModelProviderInfo::default()
            },
            /*auth_manager*/ None,
            /*gateway_auth_manager*/ None,
        );
        let error = endpoint
            .list_models(
                "1.2.3",
                HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault),
            )
            .await
            .expect_err("oversized catalog should be rejected");
        let CodexErrorDetails::InvalidRequest(message) = error.details() else {
            panic!("expected a response-size error, got {error:?}");
        };
        assert_eq!(
            message,
            &format!("response body exceeds the {MAX_MODEL_CATALOG_BYTES} byte limit")
        );
    }

    #[tokio::test]
    async fn explicit_catalog_rejects_redirects_without_forwarding_provider_credentials() {
        let server = MockServer::start().await;
        let destination = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/codex/models"))
            .respond_with(
                ResponseTemplate::new(/*s*/ 302)
                    .insert_header("location", format!("{}/codex/models", destination.uri()))
                    .set_body_string("catalog-secret"),
            )
            .expect(1)
            .mount(&server)
            .await;
        let endpoint = OpenAiModelsEndpoint::new(
            ModelProviderInfo {
                base_url: Some(server.uri()),
                model_catalog_url: Some(
                    format!("{}/codex/models?token=catalog-secret", server.uri()).into(),
                ),
                experimental_bearer_token: Some("provider-key".into()),
                ..ModelProviderInfo::default()
            },
            /*auth_manager*/ None,
            /*gateway_auth_manager*/ None,
        );
        let error = endpoint
            .list_models(
                "1.2.3",
                HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault),
            )
            .await
            .expect_err("redirect should be rejected");
        assert!(!error.to_string().contains("catalog-secret"));
        assert!(!format!("{error:?}").contains("catalog-secret"));
        assert_eq!(destination.received_requests().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn chat_catalog_discovery_preserves_native_metadata_with_api_key_auth() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [{"id": "kimi-k3", "context_length": 1048576, "supports_reasoning": true}]
            })))
            .expect(1)
            .mount(&server)
            .await;
        let provider = ModelProviderInfo {
            name: "Kimi".to_string(),
            base_url: Some(server.uri()),
            wire_api: WireApi::Chat,
            experimental_bearer_token: Some("test-key".into()),
            ..ModelProviderInfo::default()
        };
        let behavior_profile = kimi_codex_behavior_profile(&provider).unwrap();
        let manager = OpenAiModelsManager::new_without_cache(
            Arc::new(OpenAiModelsEndpoint::new(provider, None, None)),
            None,
        );
        let catalog = manager
            .raw_model_catalog(
                RefreshStrategy::Online,
                HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault),
            )
            .await;
        let model = catalog
            .models
            .iter()
            .find(|model| model.slug == "kimi-k3")
            .unwrap();
        assert_eq!(model.context_window, Some(1048576));
        assert_eq!(model.model_messages, behavior_profile.model_messages);
        assert!(!model.used_fallback_model_metadata);
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
                        codex_protocol::openai_models::ReasoningEffort::Low,
                        codex_protocol::openai_models::ReasoningEffort::High,
                        codex_protocol::openai_models::ReasoningEffort::Max,
                    ],
                    default_effort: Some(codex_protocol::openai_models::ReasoningEffort::Max),
                }),
            },
            0,
            None,
        );

        assert_eq!(model.slug, "kimi-k3");
        assert_eq!(model.visibility, ModelVisibility::List);
        assert_eq!(model.context_window, Some(1_048_576));
        assert_eq!(
            model.default_reasoning_level,
            Some(codex_protocol::openai_models::ReasoningEffort::Max)
        );
        assert_eq!(model.supported_reasoning_levels.len(), 3);
        assert!(!model.used_fallback_model_metadata);
    }

    #[test]
    fn kimi_models_inherit_native_codex_behavior_without_tool_metadata() {
        let provider_info = ModelProviderInfo {
            name: "Kimi".to_string(),
            base_url: Some("https://api.moonshot.cn/v1".to_string()),
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
        assert!(!model.supports_reasoning_summary_parameter);
    }
}
