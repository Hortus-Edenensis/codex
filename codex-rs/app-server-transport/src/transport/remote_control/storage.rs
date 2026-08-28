use codex_state::RemoteControlEnrollmentRecord as StateRemoteControlEnrollmentRecord;
use codex_state::StateRuntime;
use futures::future::BoxFuture;
use std::io;

/// Persisted remote-control server enrollment, including the lookup key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedRemoteControlEnrollment {
    pub websocket_url: String,
    pub account_id: String,
    pub app_server_client_name: Option<String>,
    pub server_id: String,
    pub environment_id: String,
    pub server_name: String,
    pub remote_control_enabled: Option<bool>,
}

impl From<StateRemoteControlEnrollmentRecord> for PersistedRemoteControlEnrollment {
    fn from(value: StateRemoteControlEnrollmentRecord) -> Self {
        Self {
            websocket_url: value.websocket_url,
            account_id: value.account_id,
            app_server_client_name: value.app_server_client_name,
            server_id: value.server_id,
            environment_id: value.environment_id,
            server_name: value.server_name,
            remote_control_enabled: value.remote_control_enabled,
        }
    }
}

impl From<&PersistedRemoteControlEnrollment> for StateRemoteControlEnrollmentRecord {
    fn from(value: &PersistedRemoteControlEnrollment) -> Self {
        Self {
            websocket_url: value.websocket_url.clone(),
            account_id: value.account_id.clone(),
            app_server_client_name: value.app_server_client_name.clone(),
            server_id: value.server_id.clone(),
            environment_id: value.environment_id.clone(),
            server_name: value.server_name.clone(),
            remote_control_enabled: value.remote_control_enabled,
        }
    }
}

/// Backend-agnostic remote-control persistence surface.
///
/// Implementations should map transport persistence requests onto the concrete storage engine
/// while preserving the current enrollment selection semantics keyed by websocket URL, account,
/// and optional app-server client name.
pub trait RemoteControlStateStore: Send + Sync {
    fn get_remote_control_enrollment<'a>(
        &'a self,
        websocket_url: &'a str,
        account_id: &'a str,
        app_server_client_name: Option<&'a str>,
    ) -> BoxFuture<'a, io::Result<Option<PersistedRemoteControlEnrollment>>>;

    fn upsert_remote_control_enrollment<'a>(
        &'a self,
        enrollment: &'a PersistedRemoteControlEnrollment,
    ) -> BoxFuture<'a, io::Result<()>>;

    fn set_remote_control_enabled<'a>(
        &'a self,
        websocket_url: &'a str,
        account_id: &'a str,
        app_server_client_name: Option<&'a str>,
        remote_control_enabled: bool,
    ) -> BoxFuture<'a, io::Result<u64>>;

    fn delete_remote_control_enrollment<'a>(
        &'a self,
        websocket_url: &'a str,
        account_id: &'a str,
        app_server_client_name: Option<&'a str>,
    ) -> BoxFuture<'a, io::Result<u64>>;
}

impl RemoteControlStateStore for StateRuntime {
    fn get_remote_control_enrollment<'a>(
        &'a self,
        websocket_url: &'a str,
        account_id: &'a str,
        app_server_client_name: Option<&'a str>,
    ) -> BoxFuture<'a, io::Result<Option<PersistedRemoteControlEnrollment>>> {
        Box::pin(async move {
            StateRuntime::get_remote_control_enrollment(
                self,
                websocket_url,
                account_id,
                app_server_client_name,
            )
            .await
            .map(|enrollment| enrollment.map(Into::into))
            .map_err(io::Error::other)
        })
    }

    fn upsert_remote_control_enrollment<'a>(
        &'a self,
        enrollment: &'a PersistedRemoteControlEnrollment,
    ) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async move {
            let state_enrollment = StateRemoteControlEnrollmentRecord::from(enrollment);
            StateRuntime::upsert_remote_control_enrollment(self, &state_enrollment)
                .await
                .map_err(io::Error::other)
        })
    }

    fn set_remote_control_enabled<'a>(
        &'a self,
        websocket_url: &'a str,
        account_id: &'a str,
        app_server_client_name: Option<&'a str>,
        remote_control_enabled: bool,
    ) -> BoxFuture<'a, io::Result<u64>> {
        Box::pin(async move {
            StateRuntime::set_remote_control_enabled(
                self,
                websocket_url,
                account_id,
                app_server_client_name,
                remote_control_enabled,
            )
            .await
            .map_err(io::Error::other)
        })
    }

    fn delete_remote_control_enrollment<'a>(
        &'a self,
        websocket_url: &'a str,
        account_id: &'a str,
        app_server_client_name: Option<&'a str>,
    ) -> BoxFuture<'a, io::Result<u64>> {
        Box::pin(async move {
            StateRuntime::delete_remote_control_enrollment(
                self,
                websocket_url,
                account_id,
                app_server_client_name,
            )
            .await
            .map_err(io::Error::other)
        })
    }
}
