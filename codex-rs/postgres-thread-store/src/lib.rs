//! PostgreSQL-backed thread persistence for remote Codex workspaces.
//!
//! This crate is the remote-SQL replacement for the local JSONL/SQLite thread
//! store. It keeps the existing `ThreadStore` contract as the integration seam
//! while making PostgreSQL the canonical history and metadata store.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::OnceLock;

use chrono::DateTime;
use chrono::Utc;
use codex_agent_graph_store::AgentGraphStore;
use codex_agent_graph_store::AgentGraphStoreFuture;
use codex_agent_graph_store::ThreadSpawnEdgeStatus;
use codex_protocol::ThreadId;
use codex_protocol::models::PermissionProfile;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::GitInfo;
use codex_protocol::protocol::GitSha;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_rollout::RolloutItem;
use codex_state::ThreadGoalStore as _;
use codex_thread_store::AppendThreadItemsParams;
use codex_thread_store::ArchiveThreadParams;
use codex_thread_store::CreateThreadParams;
use codex_thread_store::DeleteThreadParams;
use codex_thread_store::ItemPage;
use codex_thread_store::ListItemsParams;
use codex_thread_store::ListThreadsParams;
use codex_thread_store::ListTurnsParams;
use codex_thread_store::LoadThreadHistoryParams;
use codex_thread_store::PersistContext;
use codex_thread_store::ReadThreadByRolloutPathParams;
use codex_thread_store::ReadThreadParams;
use codex_thread_store::ResumeThreadParams;
use codex_thread_store::SearchThreadsParams;
use codex_thread_store::SortDirection;
use codex_thread_store::StoredThread;
use codex_thread_store::StoredThreadHistory;
use codex_thread_store::ThreadPage;
use codex_thread_store::ThreadSearchPage;
use codex_thread_store::ThreadSortKey;
use codex_thread_store::ThreadStore;
use codex_thread_store::ThreadStoreError;
use codex_thread_store::ThreadStoreFuture;
use codex_thread_store::ThreadStoreResult;
use codex_thread_store::TurnPage;
use codex_thread_store::UpdateThreadMetadataParams;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use sqlx::PgPool;
use sqlx::Postgres;
use sqlx::QueryBuilder;
use sqlx::Row;
use sqlx::migrate::Migrate;
use sqlx::migrate::Migrator;
use sqlx::postgres::PgConnectOptions;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::Notify;

mod agent_jobs;
mod generated_memories;

pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

pub const DEFAULT_DATABASE_URL_ENV: &str = "CODEX_REMOTE_SQL_URL";
pub const DEFAULT_WORKSPACE_ID: &str = "default";
const DEFAULT_THREAD_MEMORY_MODE: ThreadMemoryMode = ThreadMemoryMode::Enabled;
const REMOTE_CONTROL_APP_SERVER_CLIENT_NAME_NONE: &str = "";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostgresThreadStoreConfig {
    pub database_url_env: String,
    pub default_workspace_id: String,
    pub redis_url_env: Option<String>,
}

impl Default for PostgresThreadStoreConfig {
    fn default() -> Self {
        Self {
            database_url_env: DEFAULT_DATABASE_URL_ENV.to_string(),
            default_workspace_id: DEFAULT_WORKSPACE_ID.to_string(),
            redis_url_env: Some("CODEX_REDIS_URL".to_string()),
        }
    }
}

/// Persisted remote-control server enrollment for a remote SQL workspace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteControlEnrollmentRecord {
    pub websocket_url: String,
    pub account_id: String,
    pub app_server_client_name: Option<String>,
    pub server_id: String,
    pub environment_id: String,
    pub server_name: String,
    pub remote_control_enabled: Option<bool>,
}

#[derive(Clone)]
pub struct PostgresThreadStore {
    inner: Arc<PostgresThreadStoreInner>,
}

enum PostgresThreadStoreInner {
    Pool {
        pool: PgPool,
        workspace_id: String,
        redis_url_env: Option<String>,
        migration_scope_key: u64,
    },
    Unconfigured {
        message: String,
    },
}

#[derive(Debug)]
struct MigrationGate {
    state: StdMutex<MigrationGateState>,
    notify: Notify,
}

#[derive(Debug)]
enum MigrationGateState {
    Pending,
    Running,
    Failed(String),
    Succeeded,
}

impl Default for MigrationGate {
    fn default() -> Self {
        Self {
            state: StdMutex::new(MigrationGateState::Pending),
            notify: Notify::new(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct OffsetCursor {
    offset: usize,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct KeysetCursor {
    version: u8,
    sort_key: ThreadSortKey,
    sort_direction: SortDirection,
    value: KeysetCursorValue,
    thread_id: ThreadId,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
enum KeysetCursorValue {
    Timestamp(DateTime<Utc>),
    SectionPosition(i64),
}

#[derive(Clone, Debug, PartialEq)]
enum ThreadListCursor {
    Start,
    LegacyOffset(usize),
    Keyset(KeysetCursor),
}

impl std::fmt::Debug for PostgresThreadStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.inner.as_ref() {
            PostgresThreadStoreInner::Pool {
                workspace_id,
                redis_url_env,
                ..
            } => f
                .debug_struct("PostgresThreadStore")
                .field("workspace_id", workspace_id)
                .field("redis_url_env", redis_url_env)
                .finish_non_exhaustive(),
            PostgresThreadStoreInner::Unconfigured { message } => f
                .debug_struct("PostgresThreadStore")
                .field("unconfigured", message)
                .finish(),
        }
    }
}

impl PostgresThreadStore {
    pub fn from_env_or_unconfigured(config: PostgresThreadStoreConfig) -> Self {
        let database_url = match std::env::var(config.database_url_env.as_str()) {
            Ok(value) if !value.trim().is_empty() => value,
            _ => {
                return Self::unconfigured(format!(
                    "remote SQL thread store requires ${}",
                    config.database_url_env
                ));
            }
        };
        let options = match PgConnectOptions::from_str(database_url.as_str()) {
            Ok(options) => options,
            Err(err) => {
                return Self::unconfigured(format!("invalid ${}: {err}", config.database_url_env));
            }
        };
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect_lazy_with(options);
        Self {
            inner: Arc::new(PostgresThreadStoreInner::Pool {
                pool,
                workspace_id: config.default_workspace_id,
                redis_url_env: config.redis_url_env,
                migration_scope_key: migration_scope_key(&database_url),
            }),
        }
    }

    pub async fn migrate(&self) -> ThreadStoreResult<()> {
        self.ensure_migrated().await
    }

    pub async fn get_remote_control_enrollment(
        &self,
        websocket_url: &str,
        account_id: &str,
        app_server_client_name: Option<&str>,
    ) -> ThreadStoreResult<Option<RemoteControlEnrollmentRecord>> {
        self.ensure_migrated().await?;
        let (pool, workspace_id) = self.pool_and_workspace()?;
        let row = sqlx::query(
            r#"
SELECT websocket_url, account_id, app_server_client_name, server_id, environment_id, server_name,
    remote_control_enabled
FROM remote_control_enrollments
WHERE workspace_id = $1 AND websocket_url = $2 AND account_id = $3 AND app_server_client_name = $4
            "#,
        )
        .bind(workspace_id)
        .bind(websocket_url)
        .bind(account_id)
        .bind(remote_control_app_server_client_name_key(
            app_server_client_name,
        ))
        .fetch_optional(pool)
        .await
        .map_err(internal_error)?;

        row.map(remote_control_enrollment_from_row).transpose()
    }

    pub async fn upsert_remote_control_enrollment(
        &self,
        enrollment: &RemoteControlEnrollmentRecord,
    ) -> ThreadStoreResult<()> {
        self.ensure_migrated().await?;
        let (pool, workspace_id) = self.pool_and_workspace()?;
        let mut tx = pool.begin().await.map_err(internal_error)?;
        sqlx::query(
            "INSERT INTO workspaces (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
        )
        .bind(workspace_id)
        .bind("Default workspace")
        .execute(&mut *tx)
        .await
        .map_err(internal_error)?;
        sqlx::query(
            r#"
INSERT INTO remote_control_enrollments (
    workspace_id,
    websocket_url,
    account_id,
    app_server_client_name,
    server_id,
    environment_id,
    server_name,
    remote_control_enabled,
    updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT(workspace_id, websocket_url, account_id, app_server_client_name) DO UPDATE SET
    server_id = EXCLUDED.server_id,
    environment_id = EXCLUDED.environment_id,
    server_name = EXCLUDED.server_name,
    updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(workspace_id)
        .bind(&enrollment.websocket_url)
        .bind(&enrollment.account_id)
        .bind(remote_control_app_server_client_name_key(
            enrollment.app_server_client_name.as_deref(),
        ))
        .bind(&enrollment.server_id)
        .bind(&enrollment.environment_id)
        .bind(&enrollment.server_name)
        .bind(enrollment.remote_control_enabled)
        .bind(Utc::now())
        .execute(&mut *tx)
        .await
        .map_err(internal_error)?;
        tx.commit().await.map_err(internal_error)?;
        Ok(())
    }

    pub async fn set_remote_control_enabled(
        &self,
        websocket_url: &str,
        account_id: &str,
        app_server_client_name: Option<&str>,
        remote_control_enabled: bool,
    ) -> ThreadStoreResult<u64> {
        self.ensure_migrated().await?;
        let (pool, workspace_id) = self.pool_and_workspace()?;
        let result = sqlx::query(
            r#"
UPDATE remote_control_enrollments
SET remote_control_enabled = $5, updated_at = $6
WHERE workspace_id = $1 AND websocket_url = $2 AND account_id = $3 AND app_server_client_name = $4
            "#,
        )
        .bind(workspace_id)
        .bind(websocket_url)
        .bind(account_id)
        .bind(remote_control_app_server_client_name_key(
            app_server_client_name,
        ))
        .bind(remote_control_enabled)
        .bind(Utc::now())
        .execute(pool)
        .await
        .map_err(internal_error)?;
        Ok(result.rows_affected())
    }

    pub async fn delete_remote_control_enrollment(
        &self,
        websocket_url: &str,
        account_id: &str,
        app_server_client_name: Option<&str>,
    ) -> ThreadStoreResult<u64> {
        self.ensure_migrated().await?;
        let (pool, workspace_id) = self.pool_and_workspace()?;
        let result = sqlx::query(
            r#"
DELETE FROM remote_control_enrollments
WHERE workspace_id = $1 AND websocket_url = $2 AND account_id = $3 AND app_server_client_name = $4
            "#,
        )
        .bind(workspace_id)
        .bind(websocket_url)
        .bind(account_id)
        .bind(remote_control_app_server_client_name_key(
            app_server_client_name,
        ))
        .execute(pool)
        .await
        .map_err(internal_error)?;
        Ok(result.rows_affected())
    }

    fn unconfigured(message: String) -> Self {
        Self {
            inner: Arc::new(PostgresThreadStoreInner::Unconfigured { message }),
        }
    }

    fn pool_and_workspace(&self) -> ThreadStoreResult<(&PgPool, &str)> {
        match self.inner.as_ref() {
            PostgresThreadStoreInner::Pool {
                pool, workspace_id, ..
            } => Ok((pool, workspace_id.as_str())),
            PostgresThreadStoreInner::Unconfigured { message } => {
                Err(ThreadStoreError::InvalidRequest {
                    message: message.clone(),
                })
            }
        }
    }

    async fn ensure_migrated(&self) -> ThreadStoreResult<()> {
        match self.inner.as_ref() {
            PostgresThreadStoreInner::Pool {
                pool,
                migration_scope_key,
                ..
            } => ensure_shared_migrations(pool.clone(), *migration_scope_key).await,
            PostgresThreadStoreInner::Unconfigured { message } => {
                Err(ThreadStoreError::InvalidRequest {
                    message: message.clone(),
                })
            }
        }
    }

    async fn read_stored_thread(
        &self,
        thread_id: ThreadId,
        include_archived: bool,
        include_history: bool,
    ) -> ThreadStoreResult<StoredThread> {
        self.ensure_migrated().await?;
        let (pool, workspace_id) = self.pool_and_workspace()?;
        let row = sqlx::query(
            r#"
SELECT stored_thread_json, archived_at
FROM threads
WHERE workspace_id = $1 AND id = $2
            "#,
        )
        .bind(workspace_id)
        .bind(thread_id.to_string())
        .fetch_optional(pool)
        .await
        .map_err(internal_error)?
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;

        let archived_at: Option<DateTime<Utc>> =
            row.try_get("archived_at").map_err(internal_error)?;
        if archived_at.is_some() && !include_archived {
            return Err(ThreadStoreError::InvalidRequest {
                message: format!("thread {thread_id} is archived"),
            });
        }

        let mut stored = stored_thread_from_row(&row)?;
        if include_history {
            stored.history = Some(self.load_history_inner(thread_id, include_archived).await?);
        }
        Ok(stored)
    }

    async fn read_thread_memory_mode_key(&self, thread_id: ThreadId) -> ThreadStoreResult<String> {
        self.ensure_migrated().await?;
        let (pool, workspace_id) = self.pool_and_workspace()?;
        let row = sqlx::query(
            "SELECT memory_mode, stored_thread_json FROM threads WHERE workspace_id = $1 AND id = $2",
        )
        .bind(workspace_id)
        .bind(thread_id.to_string())
        .fetch_optional(pool)
        .await
        .map_err(internal_error)?
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
        stored_thread_memory_mode_key_from_row(&row)
    }

    async fn load_history_inner(
        &self,
        thread_id: ThreadId,
        include_archived: bool,
    ) -> ThreadStoreResult<StoredThreadHistory> {
        self.ensure_migrated().await?;
        let (pool, workspace_id) = self.pool_and_workspace()?;
        let archived_at = sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
            "SELECT archived_at FROM threads WHERE workspace_id = $1 AND id = $2",
        )
        .bind(workspace_id)
        .bind(thread_id.to_string())
        .fetch_optional(pool)
        .await
        .map_err(internal_error)?
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
        if archived_at.is_some() && !include_archived {
            return Err(ThreadStoreError::InvalidRequest {
                message: format!("thread {thread_id} is archived"),
            });
        }

        let rows = sqlx::query(
            r#"
SELECT item_json
FROM thread_items
WHERE thread_id = $1
ORDER BY seq ASC
            "#,
        )
        .bind(thread_id.to_string())
        .fetch_all(pool)
        .await
        .map_err(internal_error)?;
        let items = rows
            .into_iter()
            .map(|row| {
                let value: serde_json::Value = row.try_get("item_json").map_err(internal_error)?;
                serde_json::from_value::<RolloutItem>(value).map_err(internal_error)
            })
            .collect::<ThreadStoreResult<Vec<_>>>()?;
        Ok(StoredThreadHistory { thread_id, items })
    }
}

impl ThreadStore for PostgresThreadStore {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn default_history_mode(&self) -> ThreadHistoryMode {
        // PostgreSQL persists rollout items but does not implement the paginated turn/item lists.
        ThreadHistoryMode::Legacy
    }

    fn create_thread(&self, params: CreateThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            self.ensure_migrated().await?;
            let (pool, workspace_id) = self.pool_and_workspace()?;
            let now = Utc::now();
            let stored = StoredThread {
                thread_id: params.thread_id,
                extra_config: params.extra_config,
                rollout_path: None,
                forked_from_id: params.forked_from_id,
                parent_thread_id: params.parent_thread_id,
                preview: String::new(),
                name: None,
                model_provider: params.metadata.model_provider,
                model: params.metadata.model,
                reasoning_effort: params.metadata.reasoning_effort,
                created_at: now,
                updated_at: now,
                recency_at: now,
                archived_at: None,
                section: None,
                section_position: None,
                section_entered_at: None,
                project_id: None,
                cwd: params.metadata.cwd.unwrap_or_default(),
                cli_version: String::new(),
                source: params.source,
                history_mode: params.history_mode,
                thread_source: params.thread_source,
                agent_nickname: None,
                agent_role: None,
                agent_path: None,
                git_info: None,
                approval_mode: AskForApproval::OnRequest,
                permission_profile: PermissionProfile::read_only(),
                token_usage: None,
                first_user_message: None,
                history: None,
            };
            let source_key = canonical_session_source_key(&stored.source)?;
            let thread_source_key = stored.thread_source.as_ref().map(ToString::to_string);
            let memory_mode = params.metadata.memory_mode;
            let stored_json = stored_thread_json_with_memory_mode_key(
                &stored,
                thread_memory_mode_key(memory_mode),
            )?;
            let mut tx = pool.begin().await.map_err(internal_error)?;
            sqlx::query(
                "INSERT INTO workspaces (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
            )
            .bind(workspace_id)
            .bind("Default workspace")
            .execute(&mut *tx)
            .await
            .map_err(internal_error)?;
            sqlx::query(
                r#"
INSERT INTO sessions (id, workspace_id, root_thread_id)
VALUES ($1, $2, $3)
ON CONFLICT (id) DO NOTHING
                "#,
            )
            .bind(params.session_id.to_string())
            .bind(workspace_id)
            .bind(params.thread_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(internal_error)?;
            sqlx::query(
                r#"
INSERT INTO threads (
    id, workspace_id, session_id, forked_from_thread_id, parent_thread_id,
    history_mode, source, thread_source, model_provider, model, reasoning_effort,
    memory_mode, cwd, title, preview, created_at, updated_at, recency_at,
    stored_thread_json
) VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9, $10, $11,
    $12, $13, $14, $15, $16, $17, $18,
    $19
)
                "#,
            )
            .bind(params.thread_id.to_string())
            .bind(workspace_id)
            .bind(params.session_id.to_string())
            .bind(stored.forked_from_id.map(|id| id.to_string()))
            .bind(stored.parent_thread_id.map(|id| id.to_string()))
            .bind(thread_history_mode_key(stored.history_mode))
            .bind(source_key)
            .bind(thread_source_key)
            .bind(stored.model_provider.as_str())
            .bind(stored.model.as_deref())
            .bind(
                stored
                    .reasoning_effort
                    .as_ref()
                    .map(std::string::ToString::to_string),
            )
            .bind(thread_memory_mode_key(memory_mode))
            .bind(stored.cwd.to_string_lossy().to_string())
            .bind(stored.name.as_deref())
            .bind(stored.preview.as_str())
            .bind(now)
            .bind(now)
            .bind(now)
            .bind(stored_json)
            .execute(&mut *tx)
            .await
            .map_err(internal_error)?;
            tx.commit().await.map_err(internal_error)
        })
    }

    fn resume_thread(&self, params: ResumeThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            self.read_stored_thread(params.thread_id, params.include_archived, false)
                .await
                .map(|_| ())
        })
    }

    fn append_items(&self, params: AppendThreadItemsParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            if params.items.is_empty() {
                return Ok(());
            }
            self.ensure_migrated().await?;
            let (pool, workspace_id) = self.pool_and_workspace()?;
            let mut tx = pool.begin().await.map_err(internal_error)?;
            let row = sqlx::query(
                r#"
SELECT latest_seq, stored_thread_json
     , memory_mode
FROM threads
WHERE workspace_id = $1 AND id = $2
FOR UPDATE
                "#,
            )
            .bind(workspace_id)
            .bind(params.thread_id.to_string())
            .fetch_optional(&mut *tx)
            .await
            .map_err(internal_error)?
            .ok_or(ThreadStoreError::ThreadNotFound {
                thread_id: params.thread_id,
            })?;
            let latest_seq: i64 = row.try_get("latest_seq").map_err(internal_error)?;
            let mut stored = stored_thread_from_row(&row)?;
            let now = Utc::now();
            stored.updated_at = now;
            stored.recency_at = now;
            let mut next_seq = latest_seq;
            for item in params.items {
                next_seq += 1;
                let item_json = serde_json::to_value(&item).map_err(internal_error)?;
                sqlx::query(
                    r#"
INSERT INTO thread_items (thread_id, seq, item_ordinal, item_json)
VALUES ($1, $2, $3, $4)
                    "#,
                )
                .bind(params.thread_id.to_string())
                .bind(next_seq)
                .bind(next_seq)
                .bind(item_json.clone())
                .execute(&mut *tx)
                .await
                .map_err(internal_error)?;
                sqlx::query(
                    r#"
INSERT INTO thread_events (workspace_id, thread_id, seq, event_type, payload)
VALUES ($1, $2, $3, $4, $5)
                    "#,
                )
                .bind(workspace_id)
                .bind(params.thread_id.to_string())
                .bind(next_seq)
                .bind("thread_item_appended")
                .bind(item_json)
                .execute(&mut *tx)
                .await
                .map_err(internal_error)?;
            }
            let memory_mode = stored_thread_memory_mode_key_from_row(&row)?;
            let stored_json = stored_thread_json_with_memory_mode_key(&stored, &memory_mode)?;
            sqlx::query(
                r#"
UPDATE threads
SET latest_seq = $3,
    revision = revision + 1,
    updated_at = $4,
    recency_at = $4,
    stored_thread_json = $5
WHERE workspace_id = $1 AND id = $2
                "#,
            )
            .bind(workspace_id)
            .bind(params.thread_id.to_string())
            .bind(next_seq)
            .bind(now)
            .bind(stored_json)
            .execute(&mut *tx)
            .await
            .map_err(internal_error)?;
            sqlx::query(
                r#"
INSERT INTO outbox (workspace_id, thread_id, event_type, payload)
VALUES ($1, $2, $3, $4)
                "#,
            )
            .bind(workspace_id)
            .bind(params.thread_id.to_string())
            .bind("thread_items_appended")
            .bind(json!({
                "threadId": params.thread_id.to_string(),
                "fromSeq": latest_seq + 1,
                "toSeq": next_seq,
            }))
            .execute(&mut *tx)
            .await
            .map_err(internal_error)?;
            tx.commit().await.map_err(internal_error)
        })
    }

    fn persist_thread(
        &self,
        _thread_id: ThreadId,
        _context: PersistContext,
    ) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn flush_thread(&self, _thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn shutdown_thread(&self, _thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn discard_thread(&self, _thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn load_history(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredThreadHistory> {
        Box::pin(async move {
            self.load_history_inner(params.thread_id, params.include_archived)
                .await
        })
    }

    fn read_thread(&self, params: ReadThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(async move {
            self.read_stored_thread(
                params.thread_id,
                params.include_archived,
                params.include_history,
            )
            .await
        })
    }

    fn read_thread_by_rollout_path(
        &self,
        _params: ReadThreadByRolloutPathParams,
    ) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(async {
            Err(ThreadStoreError::Unsupported {
                operation: "read_thread_by_rollout_path",
            })
        })
    }

    fn list_threads(&self, params: ListThreadsParams) -> ThreadStoreFuture<'_, ThreadPage> {
        Box::pin(async move {
            self.ensure_migrated().await?;
            let (pool, workspace_id) = self.pool_and_workspace()?;
            let cursor = decode_thread_list_cursor(params.cursor.as_deref())?;
            if let ThreadListCursor::Keyset(cursor) = &cursor {
                validate_keyset_cursor(cursor, params.sort_key, params.sort_direction)?;
            }
            if matches!(params.cwd_filters.as_ref(), Some(filters) if filters.is_empty()) {
                return Ok(ThreadPage {
                    items: Vec::new(),
                    next_cursor: None,
                });
            }

            let page_size = params.page_size.max(1);
            let sort_column = match params.sort_key {
                ThreadSortKey::CreatedAt => "created_at",
                ThreadSortKey::UpdatedAt => "updated_at",
                ThreadSortKey::RecencyAt => "recency_at",
                ThreadSortKey::SectionPosition => {
                    "((stored_thread_json ->> 'section_position')::bigint)"
                }
            };
            let sort_direction = match params.sort_direction {
                SortDirection::Asc => "ASC",
                SortDirection::Desc => "DESC",
            };
            let mut builder = QueryBuilder::<Postgres>::new("SELECT stored_thread_json, ");
            builder.push(sort_column);
            builder.push(" AS cursor_sort_value FROM threads WHERE workspace_id = ");
            builder.push_bind(workspace_id);
            if params.archived {
                builder.push(" AND archived_at IS NOT NULL");
            } else {
                builder.push(" AND archived_at IS NULL");
                push_exclude_empty_shell_threads(&mut builder);
            }
            if !params.allowed_sources.is_empty() {
                let allowed_sources = params
                    .allowed_sources
                    .iter()
                    .map(session_source_filter_keys)
                    .collect::<ThreadStoreResult<Vec<_>>>()?
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>();
                builder.push(" AND source = ANY(");
                builder.push_bind(allowed_sources);
                builder.push(")");
            }
            if let Some(model_providers) = params.model_providers
                && !model_providers.is_empty()
            {
                builder.push(" AND model_provider = ANY(");
                builder.push_bind(model_providers);
                builder.push(")");
            }
            if let Some(cwd_filters) = params.cwd_filters {
                let cwd_filters = cwd_filters
                    .into_iter()
                    .map(|cwd| cwd.to_string_lossy().to_string())
                    .collect::<Vec<_>>();
                builder.push(" AND cwd = ANY(");
                builder.push_bind(cwd_filters);
                builder.push(")");
            }
            if let Some(section) = params.section {
                match section {
                    Some(section_id) => {
                        builder.push(" AND stored_thread_json #>> '{section,id}' = ");
                        builder.push_bind(section_id);
                    }
                    None => {
                        builder.push(" AND stored_thread_json #>> '{section,id}' IS NULL");
                    }
                }
            }
            if let Some(project_id) = params.project_id {
                match project_id {
                    Some(project_id) => {
                        builder.push(" AND stored_thread_json ->> 'project_id' = ");
                        builder.push_bind(project_id);
                    }
                    None => {
                        builder.push(" AND stored_thread_json ->> 'project_id' IS NULL");
                    }
                }
            }
            if let Some(search_term) = params.search_term
                && !search_term.is_empty()
            {
                let pattern = format!("%{search_term}%");
                builder.push(" AND (preview ILIKE ");
                builder.push_bind(pattern.clone());
                builder.push(" OR title ILIKE ");
                builder.push_bind(pattern.clone());
                builder.push(" OR EXISTS (SELECT 1 FROM thread_items WHERE thread_items.thread_id = threads.id AND item_json::text ILIKE ");
                builder.push_bind(pattern);
                builder.push("))");
            }
            if let Some(relation_filter) = params.relation_filter {
                match relation_filter {
                    codex_thread_store::ThreadRelationFilter::DirectChildrenOf(
                        parent_thread_id,
                    ) => {
                        builder.push(" AND parent_thread_id = ");
                        builder.push_bind(parent_thread_id.to_string());
                    }
                    codex_thread_store::ThreadRelationFilter::DescendantsOf(root_thread_id) => {
                        builder.push(" AND id IN (WITH RECURSIVE subtree(child_thread_id) AS (");
                        builder.push(
                            "SELECT child_thread_id FROM thread_spawn_edges WHERE parent_thread_id = ",
                        );
                        builder.push_bind(root_thread_id.to_string());
                        builder.push(
                            " UNION ALL SELECT edge.child_thread_id FROM thread_spawn_edges AS edge JOIN subtree ON edge.parent_thread_id = subtree.child_thread_id",
                        );
                        builder.push(") SELECT child_thread_id FROM subtree)");
                    }
                }
            }
            if let ThreadListCursor::Keyset(cursor) = &cursor {
                push_keyset_cursor_filter(&mut builder, cursor, sort_column, params.sort_direction);
            }
            builder.push(" ORDER BY ");
            builder.push(sort_column);
            builder.push(" ");
            builder.push(sort_direction);
            builder.push(", id ");
            builder.push(sort_direction);
            if let ThreadListCursor::LegacyOffset(offset) = cursor {
                builder.push(" OFFSET ");
                builder.push_bind(offset as i64);
            }
            builder.push(" LIMIT ");
            builder.push_bind((page_size + 1) as i64);
            let rows = builder
                .build()
                .fetch_all(pool)
                .await
                .map_err(internal_error)?;
            let has_next_page = rows.len() > page_size;
            let items = rows
                .iter()
                .take(page_size)
                .map(stored_thread_from_row)
                .collect::<ThreadStoreResult<Vec<_>>>()?;
            let next_cursor = if has_next_page {
                match (rows.get(page_size - 1), items.last()) {
                    (Some(row), Some(thread)) => Some(encode_keyset_cursor(
                        keyset_cursor_value_from_row(row, params.sort_key)?,
                        thread.thread_id.clone(),
                        params.sort_key,
                        params.sort_direction,
                    )?),
                    _ => None,
                }
            } else {
                None
            };
            Ok(ThreadPage { items, next_cursor })
        })
    }

    fn search_threads(
        &self,
        params: SearchThreadsParams,
    ) -> ThreadStoreFuture<'_, ThreadSearchPage> {
        Box::pin(async move {
            if params.search_term.is_empty() {
                return Err(ThreadStoreError::InvalidRequest {
                    message: "thread/search requires search_term".to_string(),
                });
            }
            let page = self
                .list_threads(ListThreadsParams {
                    page_size: params.page_size,
                    cursor: params.cursor,
                    sort_key: params.sort_key,
                    sort_direction: params.sort_direction,
                    allowed_sources: params.allowed_sources,
                    model_providers: None,
                    cwd_filters: None,
                    section: None,
                    project_id: None,
                    archived: params.archived,
                    search_term: Some(params.search_term.clone()),
                    relation_filter: None,
                    use_state_db_only: true,
                })
                .await?;
            let needle = params.search_term.to_lowercase();
            let items = page
                .items
                .into_iter()
                .filter(|thread| {
                    thread.preview.to_lowercase().contains(&needle)
                        || thread
                            .name
                            .as_deref()
                            .is_some_and(|name| name.to_lowercase().contains(&needle))
                })
                .map(|thread| codex_thread_store::StoredThreadSearchResult {
                    snippet: thread.preview.clone(),
                    thread,
                })
                .collect();
            Ok(ThreadSearchPage {
                items,
                next_cursor: None,
            })
        })
    }

    fn list_turns(&self, _params: ListTurnsParams) -> ThreadStoreFuture<'_, TurnPage> {
        Box::pin(async {
            Err(ThreadStoreError::Unsupported {
                operation: "remote_sql_list_turns",
            })
        })
    }

    fn list_items(&self, _params: ListItemsParams) -> ThreadStoreFuture<'_, ItemPage> {
        Box::pin(async {
            Err(ThreadStoreError::Unsupported {
                operation: "remote_sql_list_items",
            })
        })
    }

    fn update_thread_metadata(
        &self,
        params: UpdateThreadMetadataParams,
    ) -> ThreadStoreFuture<'_, Option<StoredThread>> {
        Box::pin(async move {
            self.ensure_migrated().await?;
            let (pool, workspace_id) = self.pool_and_workspace()?;
            let mut tx = pool.begin().await.map_err(internal_error)?;
            let row = sqlx::query(
                r#"
SELECT stored_thread_json, memory_mode
FROM threads
WHERE workspace_id = $1 AND id = $2
FOR UPDATE
                "#,
            )
            .bind(workspace_id)
            .bind(params.thread_id.to_string())
            .fetch_optional(&mut *tx)
            .await
            .map_err(internal_error)?
            .ok_or(ThreadStoreError::ThreadNotFound {
                thread_id: params.thread_id,
            })?;
            let mut stored = stored_thread_from_row(&row)?;
            let memory_mode = params
                .patch
                .memory_mode
                .map(thread_memory_mode_key)
                .map(str::to_string)
                .unwrap_or(stored_thread_memory_mode_key_from_row(&row)?);
            apply_metadata_patch(&mut stored, params.patch);
            let archived_at = stored.archived_at;
            if archived_at.is_some() && !params.include_archived {
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!("thread {} is archived", params.thread_id),
                });
            }
            let source_key = canonical_session_source_key(&stored.source)?;
            let thread_source_key = stored.thread_source.as_ref().map(ToString::to_string);
            let stored_json = stored_thread_json_with_memory_mode_key(&stored, &memory_mode)?;
            sqlx::query(
                r#"
UPDATE threads
SET revision = revision + 1,
    model_provider = $3,
    model = $4,
    reasoning_effort = $5,
    cwd = $6,
    title = $7,
    preview = $8,
    archived_at = $9,
    updated_at = $10,
    recency_at = $11,
    source = $12,
    thread_source = $13,
    memory_mode = $14,
    stored_thread_json = $15
WHERE workspace_id = $1 AND id = $2
                "#,
            )
            .bind(workspace_id)
            .bind(params.thread_id.to_string())
            .bind(stored.model_provider.as_str())
            .bind(stored.model.as_deref())
            .bind(
                stored
                    .reasoning_effort
                    .as_ref()
                    .map(std::string::ToString::to_string),
            )
            .bind(stored.cwd.to_string_lossy().to_string())
            .bind(stored.name.as_deref())
            .bind(stored.preview.as_str())
            .bind(stored.archived_at)
            .bind(stored.updated_at)
            .bind(stored.recency_at)
            .bind(source_key)
            .bind(thread_source_key)
            .bind(memory_mode)
            .bind(stored_json)
            .execute(&mut *tx)
            .await
            .map_err(internal_error)?;
            tx.commit().await.map_err(internal_error)?;
            Ok(Some(stored))
        })
    }

    fn archive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            let mut stored = self
                .read_stored_thread(params.thread_id, /*include_archived*/ true, false)
                .await?;
            stored.archived_at = Some(Utc::now());
            self.update_thread_metadata(UpdateThreadMetadataParams {
                thread_id: params.thread_id,
                patch: codex_thread_store::ThreadMetadataPatch {
                    updated_at: Some(stored.updated_at),
                    ..Default::default()
                },
                include_archived: true,
            })
            .await?;
            let (pool, workspace_id) = self.pool_and_workspace()?;
            let memory_mode = self
                .read_thread_memory_mode_key(params.thread_id)
                .await
                .unwrap_or_else(|_| thread_memory_mode_key(DEFAULT_THREAD_MEMORY_MODE).to_string());
            let stored_json = stored_thread_json_with_memory_mode_key(&stored, &memory_mode)?;
            sqlx::query(
                "UPDATE threads SET archived_at = $3, stored_thread_json = $4 WHERE workspace_id = $1 AND id = $2",
            )
            .bind(workspace_id)
            .bind(params.thread_id.to_string())
            .bind(stored.archived_at)
            .bind(stored_json)
            .execute(pool)
            .await
            .map_err(internal_error)?;
            Ok(())
        })
    }

    fn unarchive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(async move {
            let (pool, workspace_id) = self.pool_and_workspace()?;
            let mut stored = self
                .read_stored_thread(params.thread_id, /*include_archived*/ true, false)
                .await?;
            stored.archived_at = None;
            stored.updated_at = Utc::now();
            let memory_mode = self
                .read_thread_memory_mode_key(params.thread_id)
                .await
                .unwrap_or_else(|_| thread_memory_mode_key(DEFAULT_THREAD_MEMORY_MODE).to_string());
            let stored_json = stored_thread_json_with_memory_mode_key(&stored, &memory_mode)?;
            sqlx::query(
                "UPDATE threads SET archived_at = NULL, updated_at = $3, stored_thread_json = $4 WHERE workspace_id = $1 AND id = $2",
            )
            .bind(workspace_id)
            .bind(params.thread_id.to_string())
            .bind(stored.updated_at)
            .bind(stored_json)
            .execute(pool)
            .await
            .map_err(internal_error)?;
            Ok(stored)
        })
    }

    fn delete_thread(&self, params: DeleteThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            self.ensure_migrated().await?;
            let (pool, workspace_id) = self.pool_and_workspace()?;
            let result = sqlx::query("DELETE FROM threads WHERE workspace_id = $1 AND id = $2")
                .bind(workspace_id)
                .bind(params.thread_id.to_string())
                .execute(pool)
                .await
                .map_err(internal_error)?;
            if result.rows_affected() == 0 {
                return Err(ThreadStoreError::ThreadNotFound {
                    thread_id: params.thread_id,
                });
            }
            Ok(())
        })
    }
}

impl codex_state::ThreadGoalStore for PostgresThreadStore {
    fn get_thread_goal(
        &self,
        thread_id: ThreadId,
    ) -> codex_state::ThreadGoalStoreFuture<'_, Option<codex_state::ThreadGoal>> {
        Box::pin(async move {
            self.ensure_migrated().await.map_err(anyhow_from_store)?;
            let (pool, _) = self.pool_and_workspace().map_err(anyhow_from_store)?;
            let row = sqlx::query(
                r#"
SELECT
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
FROM thread_goals
WHERE thread_id = $1
                "#,
            )
            .bind(thread_id.to_string())
            .fetch_optional(pool)
            .await?;

            row.map(|row| thread_goal_from_pg_row(&row)).transpose()
        })
    }

    fn replace_thread_goal_snapshot(
        &self,
        goal: codex_state::ThreadGoal,
    ) -> codex_state::ThreadGoalStoreFuture<'_, ()> {
        Box::pin(async move {
            self.ensure_migrated().await.map_err(anyhow_from_store)?;
            let (pool, _) = self.pool_and_workspace().map_err(anyhow_from_store)?;
            let mut transaction = pool.begin().await?;
            let created_at_ms = datetime_to_epoch_millis(goal.created_at);
            let updated_at_ms = datetime_to_epoch_millis(goal.updated_at);
            sqlx::query(
                r#"
INSERT INTO thread_goals (
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms,
    updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT(thread_id) DO UPDATE SET
    goal_id = EXCLUDED.goal_id,
    objective = EXCLUDED.objective,
    status = EXCLUDED.status,
    token_budget = EXCLUDED.token_budget,
    tokens_used = EXCLUDED.tokens_used,
    time_used_seconds = EXCLUDED.time_used_seconds,
    created_at_ms = EXCLUDED.created_at_ms,
    updated_at_ms = EXCLUDED.updated_at_ms,
    updated_at = EXCLUDED.updated_at
                "#,
            )
            .bind(goal.thread_id.to_string())
            .bind(goal.goal_id.as_str())
            .bind(goal.objective.as_str())
            .bind(goal.status.as_str())
            .bind(goal.token_budget)
            .bind(goal.tokens_used)
            .bind(goal.time_used_seconds)
            .bind(created_at_ms)
            .bind(updated_at_ms)
            .bind(goal.updated_at)
            .execute(&mut *transaction)
            .await?;

            sqlx::query(
                r#"
INSERT INTO thread_goal_continuation_deferrals (thread_id)
VALUES ($1)
ON CONFLICT(thread_id) DO NOTHING
                "#,
            )
            .bind(goal.thread_id.to_string())
            .execute(&mut *transaction)
            .await?;

            transaction.commit().await?;
            Ok(())
        })
    }

    fn replace_thread_goal(
        &self,
        thread_id: ThreadId,
        objective: String,
        status: codex_state::ThreadGoalStatus,
        token_budget: Option<i64>,
    ) -> codex_state::ThreadGoalStoreFuture<'_, codex_state::ThreadGoal> {
        Box::pin(async move {
            self.ensure_migrated().await.map_err(anyhow_from_store)?;
            let (pool, _) = self.pool_and_workspace().map_err(anyhow_from_store)?;
            let now = Utc::now();
            let now_ms = datetime_to_epoch_millis(now);
            let goal_id = new_goal_id(thread_id, now);
            let status = status_after_budget_limit(status, /*tokens_used*/ 0, token_budget);
            let row = sqlx::query(
                r#"
INSERT INTO thread_goals (
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms,
    updated_at
) VALUES ($1, $2, $3, $4, $5, 0, 0, $6, $7, $8)
ON CONFLICT(thread_id) DO UPDATE SET
    goal_id = EXCLUDED.goal_id,
    objective = EXCLUDED.objective,
    status = EXCLUDED.status,
    token_budget = EXCLUDED.token_budget,
    tokens_used = 0,
    time_used_seconds = 0,
    created_at_ms = EXCLUDED.created_at_ms,
    updated_at_ms = EXCLUDED.updated_at_ms,
    updated_at = EXCLUDED.updated_at
RETURNING
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
                "#,
            )
            .bind(thread_id.to_string())
            .bind(goal_id)
            .bind(objective.as_str())
            .bind(status.as_str())
            .bind(token_budget)
            .bind(now_ms)
            .bind(now_ms)
            .bind(now)
            .fetch_one(pool)
            .await?;

            thread_goal_from_pg_row(&row)
        })
    }

    fn insert_thread_goal(
        &self,
        thread_id: ThreadId,
        objective: String,
        status: codex_state::ThreadGoalStatus,
        token_budget: Option<i64>,
    ) -> codex_state::ThreadGoalStoreFuture<'_, Option<codex_state::ThreadGoal>> {
        Box::pin(async move {
            self.ensure_migrated().await.map_err(anyhow_from_store)?;
            let (pool, _) = self.pool_and_workspace().map_err(anyhow_from_store)?;
            let now = Utc::now();
            let now_ms = datetime_to_epoch_millis(now);
            let goal_id = new_goal_id(thread_id, now);
            let status = status_after_budget_limit(status, /*tokens_used*/ 0, token_budget);
            let row = sqlx::query(
                r#"
INSERT INTO thread_goals (
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms,
    updated_at
) VALUES ($1, $2, $3, $4, $5, 0, 0, $6, $7, $8)
ON CONFLICT(thread_id) DO UPDATE SET
    goal_id = EXCLUDED.goal_id,
    objective = EXCLUDED.objective,
    status = EXCLUDED.status,
    token_budget = EXCLUDED.token_budget,
    tokens_used = 0,
    time_used_seconds = 0,
    created_at_ms = EXCLUDED.created_at_ms,
    updated_at_ms = EXCLUDED.updated_at_ms,
    updated_at = EXCLUDED.updated_at
WHERE thread_goals.status = 'complete'
RETURNING
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
                "#,
            )
            .bind(thread_id.to_string())
            .bind(goal_id)
            .bind(objective.as_str())
            .bind(status.as_str())
            .bind(token_budget)
            .bind(now_ms)
            .bind(now_ms)
            .bind(now)
            .fetch_optional(pool)
            .await?;

            row.map(|row| thread_goal_from_pg_row(&row)).transpose()
        })
    }

    fn update_thread_goal(
        &self,
        thread_id: ThreadId,
        update: codex_state::GoalUpdate,
    ) -> codex_state::ThreadGoalStoreFuture<'_, Option<codex_state::ThreadGoal>> {
        Box::pin(async move {
            self.ensure_migrated().await.map_err(anyhow_from_store)?;
            let (pool, _) = self.pool_and_workspace().map_err(anyhow_from_store)?;
            let codex_state::GoalUpdate {
                objective,
                status,
                token_budget,
                expected_goal_id,
            } = update;
            let objective = objective.as_deref();
            let expected_goal_id = expected_goal_id.as_deref();
            let now = Utc::now();
            let now_ms = datetime_to_epoch_millis(now);
            let result = match (status, token_budget) {
                (Some(status), Some(token_budget)) => {
                    sqlx::query(
                        r#"
UPDATE thread_goals
SET
    objective = COALESCE($1, objective),
    status = CASE
        WHEN status = $2 AND $3 IN ($4, $5) THEN status
        WHEN $6 = 'active' AND $7::BIGINT IS NOT NULL AND tokens_used >= $8 THEN $9
        ELSE $10
    END,
    token_budget = $11,
    updated_at = $12,
    updated_at_ms = $13
WHERE thread_id = $14
  AND ($15::TEXT IS NULL OR goal_id = $16)
                        "#,
                    )
                    .bind(objective)
                    .bind(codex_state::ThreadGoalStatus::BudgetLimited.as_str())
                    .bind(status.as_str())
                    .bind(codex_state::ThreadGoalStatus::Paused.as_str())
                    .bind(codex_state::ThreadGoalStatus::Blocked.as_str())
                    .bind(status.as_str())
                    .bind(token_budget)
                    .bind(token_budget)
                    .bind(codex_state::ThreadGoalStatus::BudgetLimited.as_str())
                    .bind(status.as_str())
                    .bind(token_budget)
                    .bind(now)
                    .bind(now_ms)
                    .bind(thread_id.to_string())
                    .bind(expected_goal_id)
                    .bind(expected_goal_id)
                    .execute(pool)
                    .await?
                }
                (Some(status), None) => {
                    sqlx::query(
                        r#"
UPDATE thread_goals
SET
    objective = COALESCE($1, objective),
    status = CASE
        WHEN status = $2 AND $3 IN ($4, $5) THEN status
        WHEN $6 = 'active' AND token_budget IS NOT NULL AND tokens_used >= token_budget THEN $7
        ELSE $8
    END,
    updated_at = $9,
    updated_at_ms = $10
WHERE thread_id = $11
  AND ($12::TEXT IS NULL OR goal_id = $13)
                        "#,
                    )
                    .bind(objective)
                    .bind(codex_state::ThreadGoalStatus::BudgetLimited.as_str())
                    .bind(status.as_str())
                    .bind(codex_state::ThreadGoalStatus::Paused.as_str())
                    .bind(codex_state::ThreadGoalStatus::Blocked.as_str())
                    .bind(status.as_str())
                    .bind(codex_state::ThreadGoalStatus::BudgetLimited.as_str())
                    .bind(status.as_str())
                    .bind(now)
                    .bind(now_ms)
                    .bind(thread_id.to_string())
                    .bind(expected_goal_id)
                    .bind(expected_goal_id)
                    .execute(pool)
                    .await?
                }
                (None, Some(token_budget)) => {
                    sqlx::query(
                        r#"
UPDATE thread_goals
SET
    objective = COALESCE($1, objective),
    token_budget = $2,
    status = CASE
        WHEN status = 'active' AND $3::BIGINT IS NOT NULL AND tokens_used >= $4 THEN $5
        ELSE status
    END,
    updated_at = $6,
    updated_at_ms = $7
WHERE thread_id = $8
  AND ($9::TEXT IS NULL OR goal_id = $10)
                        "#,
                    )
                    .bind(objective)
                    .bind(token_budget)
                    .bind(token_budget)
                    .bind(token_budget)
                    .bind(codex_state::ThreadGoalStatus::BudgetLimited.as_str())
                    .bind(now)
                    .bind(now_ms)
                    .bind(thread_id.to_string())
                    .bind(expected_goal_id)
                    .bind(expected_goal_id)
                    .execute(pool)
                    .await?
                }
                (None, None) => {
                    if let Some(objective) = objective {
                        sqlx::query(
                            r#"
UPDATE thread_goals
SET
    objective = $1,
    updated_at = $2,
    updated_at_ms = $3
WHERE thread_id = $4
  AND ($5::TEXT IS NULL OR goal_id = $6)
                            "#,
                        )
                        .bind(objective)
                        .bind(now)
                        .bind(now_ms)
                        .bind(thread_id.to_string())
                        .bind(expected_goal_id)
                        .bind(expected_goal_id)
                        .execute(pool)
                        .await?
                    } else {
                        let goal = self.get_thread_goal(thread_id).await?;
                        return Ok(match (goal, expected_goal_id) {
                            (Some(goal), Some(expected_goal_id))
                                if goal.goal_id != expected_goal_id =>
                            {
                                None
                            }
                            (goal, _) => goal,
                        });
                    }
                }
            };

            if result.rows_affected() == 0 {
                return Ok(None);
            }

            self.get_thread_goal(thread_id).await
        })
    }

    fn pause_active_thread_goal(
        &self,
        thread_id: ThreadId,
    ) -> codex_state::ThreadGoalStoreFuture<'_, Option<codex_state::ThreadGoal>> {
        update_active_thread_goal_status(self, thread_id, codex_state::ThreadGoalStatus::Paused)
    }

    fn usage_limit_active_thread_goal(
        &self,
        thread_id: ThreadId,
    ) -> codex_state::ThreadGoalStoreFuture<'_, Option<codex_state::ThreadGoal>> {
        update_active_thread_goal_status(
            self,
            thread_id,
            codex_state::ThreadGoalStatus::UsageLimited,
        )
    }

    fn delete_thread_goal(
        &self,
        thread_id: ThreadId,
    ) -> codex_state::ThreadGoalStoreFuture<'_, Option<codex_state::ThreadGoal>> {
        Box::pin(async move {
            self.ensure_migrated().await.map_err(anyhow_from_store)?;
            let (pool, _) = self.pool_and_workspace().map_err(anyhow_from_store)?;
            let row = sqlx::query(
                r#"
DELETE FROM thread_goals
WHERE thread_id = $1
RETURNING
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
                "#,
            )
            .bind(thread_id.to_string())
            .fetch_optional(pool)
            .await?;

            row.map(|row| thread_goal_from_pg_row(&row)).transpose()
        })
    }

    fn account_thread_goal_usage(
        &self,
        thread_id: ThreadId,
        time_delta_seconds: i64,
        token_delta: i64,
        mode: codex_state::GoalAccountingMode,
        expected_goal_id: Option<String>,
    ) -> codex_state::ThreadGoalStoreFuture<'_, codex_state::GoalAccountingOutcome> {
        Box::pin(async move {
            self.ensure_migrated().await.map_err(anyhow_from_store)?;
            let (pool, _) = self.pool_and_workspace().map_err(anyhow_from_store)?;
            let time_delta_seconds = time_delta_seconds.max(0);
            let token_delta = token_delta.max(0);
            if time_delta_seconds == 0 && token_delta == 0 {
                return Ok(codex_state::GoalAccountingOutcome::Unchanged(
                    self.get_thread_goal(thread_id).await?,
                ));
            }

            let now = Utc::now();
            let now_ms = datetime_to_epoch_millis(now);
            let active_or_stopped_status_filter =
                "status IN ('active', 'paused', 'blocked', 'usage_limited', 'budget_limited')";
            let status_filter = match mode {
                codex_state::GoalAccountingMode::ActiveStatusOnly => "status = 'active'",
                codex_state::GoalAccountingMode::ActiveOnly => {
                    "status IN ('active', 'budget_limited')"
                }
                codex_state::GoalAccountingMode::ActiveOrComplete => {
                    "status IN ('active', 'budget_limited', 'complete')"
                }
                codex_state::GoalAccountingMode::ActiveOrStopped => active_or_stopped_status_filter,
            };
            let budget_limit_status_filter = match mode {
                codex_state::GoalAccountingMode::ActiveStatusOnly
                | codex_state::GoalAccountingMode::ActiveOnly
                | codex_state::GoalAccountingMode::ActiveOrComplete => "status = 'active'",
                codex_state::GoalAccountingMode::ActiveOrStopped => active_or_stopped_status_filter,
            };
            let mut builder = QueryBuilder::<Postgres>::new(
                r#"
UPDATE thread_goals
SET
    time_used_seconds = time_used_seconds +
                "#,
            );
            builder.push_bind(time_delta_seconds);
            builder.push(
                r#",
    tokens_used = tokens_used +
                "#,
            );
            builder.push_bind(token_delta);
            builder.push(
                r#",
    status = CASE
        WHEN
                "#,
            );
            builder.push(budget_limit_status_filter);
            builder.push(
                r#"
            AND token_budget IS NOT NULL
            AND tokens_used +
                "#,
            );
            builder.push_bind(token_delta);
            builder.push(
                r#"
                >= token_budget
            THEN
                "#,
            );
            builder.push_bind(codex_state::ThreadGoalStatus::BudgetLimited.as_str());
            builder.push(
                r#"
        ELSE status
    END,
    updated_at =
                "#,
            );
            builder.push_bind(now);
            builder.push(
                r#",
    updated_at_ms =
                "#,
            );
            builder.push_bind(now_ms);
            builder.push(
                r#"
WHERE thread_id =
                "#,
            );
            builder.push_bind(thread_id.to_string());
            builder.push(" AND ");
            builder.push(status_filter);
            if let Some(expected_goal_id) = expected_goal_id {
                builder.push(" AND goal_id = ").push_bind(expected_goal_id);
            }
            builder.push(
                r#"
RETURNING
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
                "#,
            );

            let row = builder.build().fetch_optional(pool).await?;

            let Some(row) = row else {
                return Ok(codex_state::GoalAccountingOutcome::Unchanged(
                    self.get_thread_goal(thread_id).await?,
                ));
            };

            Ok(codex_state::GoalAccountingOutcome::Updated(
                thread_goal_from_pg_row(&row)?,
            ))
        })
    }
}

fn update_active_thread_goal_status(
    store: &PostgresThreadStore,
    thread_id: ThreadId,
    status: codex_state::ThreadGoalStatus,
) -> codex_state::ThreadGoalStoreFuture<'_, Option<codex_state::ThreadGoal>> {
    Box::pin(async move {
        store.ensure_migrated().await.map_err(anyhow_from_store)?;
        let (pool, _) = store.pool_and_workspace().map_err(anyhow_from_store)?;
        let now = Utc::now();
        let now_ms = datetime_to_epoch_millis(now);
        let result = sqlx::query(
            r#"
UPDATE thread_goals
SET
    status = $1,
    updated_at = $2,
    updated_at_ms = $3
WHERE thread_id = $4
  AND (
      status = 'active'
      OR (
          $5 = 'usage_limited'
          AND status = 'budget_limited'
      )
  )
            "#,
        )
        .bind(status.as_str())
        .bind(now)
        .bind(now_ms)
        .bind(thread_id.to_string())
        .bind(status.as_str())
        .execute(pool)
        .await?;

        if result.rows_affected() == 0 {
            return Ok(None);
        }

        store.get_thread_goal(thread_id).await
    })
}

impl AgentGraphStore for PostgresThreadStore {
    fn upsert_thread_spawn_edge(
        &self,
        parent_thread_id: ThreadId,
        child_thread_id: ThreadId,
        status: ThreadSpawnEdgeStatus,
    ) -> AgentGraphStoreFuture<'_, ()> {
        Box::pin(async move {
            self.ensure_migrated().await.map_err(agent_graph_error)?;
            let (pool, _) = self.pool_and_workspace().map_err(agent_graph_error)?;
            sqlx::query(
                r#"
INSERT INTO thread_spawn_edges (parent_thread_id, child_thread_id, status)
VALUES ($1, $2, $3)
ON CONFLICT (child_thread_id) DO UPDATE SET
    parent_thread_id = EXCLUDED.parent_thread_id,
    status = EXCLUDED.status
                "#,
            )
            .bind(parent_thread_id.to_string())
            .bind(child_thread_id.to_string())
            .bind(graph_status(status))
            .execute(pool)
            .await
            .map_err(agent_graph_error)?;
            Ok(())
        })
    }

    fn set_thread_spawn_edge_status(
        &self,
        child_thread_id: ThreadId,
        status: ThreadSpawnEdgeStatus,
    ) -> AgentGraphStoreFuture<'_, ()> {
        Box::pin(async move {
            self.ensure_migrated().await.map_err(agent_graph_error)?;
            let (pool, _) = self.pool_and_workspace().map_err(agent_graph_error)?;
            sqlx::query("UPDATE thread_spawn_edges SET status = $1 WHERE child_thread_id = $2")
                .bind(graph_status(status))
                .bind(child_thread_id.to_string())
                .execute(pool)
                .await
                .map_err(agent_graph_error)?;
            Ok(())
        })
    }

    fn list_thread_spawn_children(
        &self,
        parent_thread_id: ThreadId,
        status_filter: Option<ThreadSpawnEdgeStatus>,
    ) -> AgentGraphStoreFuture<'_, Vec<ThreadId>> {
        Box::pin(async move {
            self.ensure_migrated().await.map_err(agent_graph_error)?;
            let (pool, _) = self.pool_and_workspace().map_err(agent_graph_error)?;
            let rows = sqlx::query(
                r#"
SELECT child_thread_id
FROM thread_spawn_edges
WHERE parent_thread_id = $1
  AND ($2::text IS NULL OR status = $2)
ORDER BY child_thread_id
                "#,
            )
            .bind(parent_thread_id.to_string())
            .bind(status_filter.map(graph_status))
            .fetch_all(pool)
            .await
            .map_err(agent_graph_error)?;
            rows.into_iter()
                .map(|row| {
                    let id: String = row.try_get("child_thread_id").map_err(agent_graph_error)?;
                    ThreadId::from_string(id.as_str()).map_err(agent_graph_error)
                })
                .collect()
        })
    }

    fn list_thread_spawn_descendants(
        &self,
        root_thread_id: ThreadId,
        status_filter: Option<ThreadSpawnEdgeStatus>,
    ) -> AgentGraphStoreFuture<'_, Vec<ThreadId>> {
        Box::pin(async move {
            self.ensure_migrated().await.map_err(agent_graph_error)?;
            let (pool, _) = self.pool_and_workspace().map_err(agent_graph_error)?;
            let rows = sqlx::query(
                r#"
WITH RECURSIVE subtree(child_thread_id, depth) AS (
    SELECT child_thread_id, 1
    FROM thread_spawn_edges
    WHERE parent_thread_id = $1
      AND ($2::text IS NULL OR status = $2)
    UNION ALL
    SELECT edge.child_thread_id, subtree.depth + 1
    FROM thread_spawn_edges AS edge
    JOIN subtree ON edge.parent_thread_id = subtree.child_thread_id
    WHERE ($2::text IS NULL OR edge.status = $2)
)
SELECT child_thread_id
FROM subtree
ORDER BY depth, child_thread_id
                "#,
            )
            .bind(root_thread_id.to_string())
            .bind(status_filter.map(graph_status))
            .fetch_all(pool)
            .await
            .map_err(agent_graph_error)?;
            rows.into_iter()
                .map(|row| {
                    let id: String = row.try_get("child_thread_id").map_err(agent_graph_error)?;
                    ThreadId::from_string(id.as_str()).map_err(agent_graph_error)
                })
                .collect()
        })
    }
}

fn apply_metadata_patch(stored: &mut StoredThread, patch: codex_thread_store::ThreadMetadataPatch) {
    if let Some(name) = patch.name {
        stored.name = name;
    }
    if let Some(preview) = patch.preview {
        stored.preview = preview;
    }
    if let Some(model_provider) = patch.model_provider {
        stored.model_provider = model_provider;
    }
    if let Some(model) = patch.model {
        stored.model = Some(model);
    }
    if let Some(reasoning_effort) = patch.reasoning_effort {
        stored.reasoning_effort = reasoning_effort;
    }
    if let Some(created_at) = patch.created_at {
        stored.created_at = created_at;
    }
    if let Some(updated_at) = patch.updated_at {
        stored.updated_at = updated_at;
    }
    if let Some(recency_at) = patch.advance_recency_at
        && recency_at > stored.recency_at
    {
        stored.recency_at = recency_at;
    }
    if let Some(source) = patch.source {
        stored.source = source;
    }
    if let Some(thread_source) = patch.thread_source {
        stored.thread_source = thread_source;
    }
    if let Some(agent_nickname) = patch.agent_nickname {
        stored.agent_nickname = agent_nickname;
    }
    if let Some(agent_role) = patch.agent_role {
        stored.agent_role = agent_role;
    }
    if let Some(agent_path) = patch.agent_path {
        stored.agent_path = agent_path;
    }
    if let Some(cwd) = patch.cwd {
        stored.cwd = cwd;
    }
    if let Some(cli_version) = patch.cli_version {
        stored.cli_version = cli_version;
    }
    if let Some(approval_mode) = patch.approval_mode {
        stored.approval_mode = approval_mode;
    }
    if let Some(permission_profile) = patch.permission_profile {
        stored.permission_profile = permission_profile;
    }
    if let Some(token_usage) = patch.token_usage {
        stored.token_usage = Some(token_usage);
    }
    if let Some(first_user_message) = patch.first_user_message {
        stored.first_user_message = Some(first_user_message);
    }
    if let Some(project_id) = patch.project_id {
        stored.project_id = project_id;
    }
    if let Some(git_info) = patch.git_info {
        let stored_git_info = stored.git_info.get_or_insert(GitInfo {
            commit_hash: None,
            branch: None,
            repository_url: None,
        });
        if let Some(sha) = git_info.sha {
            stored_git_info.commit_hash = sha.map(|sha| GitSha::new(sha.as_str()));
        }
        if let Some(branch) = git_info.branch {
            stored_git_info.branch = branch;
        }
        if let Some(origin_url) = git_info.origin_url {
            stored_git_info.repository_url = origin_url;
        }
    }
}

fn stored_thread_json_with_memory_mode_key(
    stored: &StoredThread,
    memory_mode: &str,
) -> ThreadStoreResult<Value> {
    let mut value = serde_json::to_value(stored).map_err(internal_error)?;
    let Value::Object(object) = &mut value else {
        return Err(internal_error("stored thread JSON must be an object"));
    };
    object.insert(
        "memory_mode".to_string(),
        Value::String(memory_mode.to_string()),
    );
    Ok(value)
}

fn stored_thread_memory_mode_key_from_row(
    row: &sqlx::postgres::PgRow,
) -> ThreadStoreResult<String> {
    if let Ok(memory_mode) = row.try_get::<String, _>("memory_mode") {
        return Ok(memory_mode);
    }
    let value: Value = row.try_get("stored_thread_json").map_err(internal_error)?;
    Ok(stored_thread_memory_mode_key_from_value(&value)?
        .unwrap_or_else(|| thread_memory_mode_key(DEFAULT_THREAD_MEMORY_MODE).to_string()))
}

fn stored_thread_memory_mode_key_from_value(value: &Value) -> ThreadStoreResult<Option<String>> {
    value
        .get("memory_mode")
        .and_then(Value::as_str)
        .map(|memory_mode| Ok(memory_mode.to_string()))
        .transpose()
}

fn thread_memory_mode_key(memory_mode: ThreadMemoryMode) -> &'static str {
    match memory_mode {
        ThreadMemoryMode::Enabled => "enabled",
        ThreadMemoryMode::Disabled => "disabled",
    }
}

fn thread_history_mode_key(history_mode: ThreadHistoryMode) -> &'static str {
    history_mode.as_str()
}

fn stored_thread_from_row(row: &sqlx::postgres::PgRow) -> ThreadStoreResult<StoredThread> {
    let value: serde_json::Value = row.try_get("stored_thread_json").map_err(internal_error)?;
    stored_thread_from_value(value)
}

fn stored_thread_from_value(value: Value) -> ThreadStoreResult<StoredThread> {
    let mut stored: StoredThread = serde_json::from_value(value).map_err(internal_error)?;
    // Older releases labeled rows as paginated even though this store never supported the
    // paginated history primitives. Normalize at the store boundary so those rows replay safely.
    stored.history_mode = ThreadHistoryMode::Legacy;
    Ok(stored)
}

fn push_exclude_empty_shell_threads(builder: &mut QueryBuilder<Postgres>) {
    builder.push(
        r#"
 AND NOT (
    preview = ''
    AND EXISTS (
        SELECT 1 FROM thread_items
        WHERE thread_items.thread_id = threads.id
    )
    AND NOT EXISTS (
        SELECT 1 FROM turns
        WHERE turns.thread_id = threads.id
    )
    AND NOT EXISTS (
        SELECT 1 FROM thread_items
        WHERE thread_items.thread_id = threads.id
          AND (
            COALESCE(item_json->>'type', '') <> 'event_msg'
            OR COALESCE(item_json->'payload'->>'type', '') NOT IN ('session_configured', 'warning')
          )
    )
)
"#,
    );
}

fn remote_control_app_server_client_name_key(app_server_client_name: Option<&str>) -> &str {
    app_server_client_name.unwrap_or(REMOTE_CONTROL_APP_SERVER_CLIENT_NAME_NONE)
}

fn app_server_client_name_from_key(app_server_client_name: String) -> Option<String> {
    if app_server_client_name.is_empty() {
        None
    } else {
        Some(app_server_client_name)
    }
}

fn remote_control_enrollment_from_row(
    row: sqlx::postgres::PgRow,
) -> ThreadStoreResult<RemoteControlEnrollmentRecord> {
    let app_server_client_name: String = row
        .try_get("app_server_client_name")
        .map_err(internal_error)?;
    Ok(RemoteControlEnrollmentRecord {
        websocket_url: row.try_get("websocket_url").map_err(internal_error)?,
        account_id: row.try_get("account_id").map_err(internal_error)?,
        app_server_client_name: app_server_client_name_from_key(app_server_client_name),
        server_id: row.try_get("server_id").map_err(internal_error)?,
        environment_id: row.try_get("environment_id").map_err(internal_error)?,
        server_name: row.try_get("server_name").map_err(internal_error)?,
        remote_control_enabled: row
            .try_get("remote_control_enabled")
            .map_err(internal_error)?,
    })
}

fn migration_scope_key(database_url: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    database_url.hash(&mut hasher);
    hasher.finish()
}

fn migration_gates() -> &'static StdMutex<HashMap<u64, Arc<MigrationGate>>> {
    static GATES: OnceLock<StdMutex<HashMap<u64, Arc<MigrationGate>>>> = OnceLock::new();
    GATES.get_or_init(|| StdMutex::new(HashMap::new()))
}

async fn ensure_shared_migrations(pool: PgPool, scope_key: u64) -> ThreadStoreResult<()> {
    let gate = {
        let mut gates = migration_gates()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(
            gates
                .entry(scope_key)
                .or_insert_with(|| Arc::new(MigrationGate::default())),
        )
    };

    loop {
        let notified = gate.notify.notified();
        let mut start_worker = None;
        {
            let mut state = gate
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match &*state {
                MigrationGateState::Pending => {
                    *state = MigrationGateState::Running;
                    start_worker = Some(pool.clone());
                }
                MigrationGateState::Running => {}
                MigrationGateState::Failed(message) => {
                    let message = message.clone();
                    *state = MigrationGateState::Pending;
                    return Err(ThreadStoreError::Internal { message });
                }
                MigrationGateState::Succeeded => return Ok(()),
            }
        }
        if let Some(pool) = start_worker {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move {
                let result = run_shared_migrations(pool).await;
                let mut state = gate
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                *state = match result {
                    Ok(()) => MigrationGateState::Succeeded,
                    Err(err) => MigrationGateState::Failed(format!(
                        "failed to apply remote SQL migrations: {err}"
                    )),
                };
                drop(state);
                gate.notify.notify_waiters();
            });
        }
        notified.await;
    }
}

fn locking_disabled_migrator(migrator: &Migrator) -> Migrator {
    Migrator {
        migrations: migrator.migrations.clone(),
        ignore_missing: migrator.ignore_missing,
        locking: false,
        no_tx: migrator.no_tx,
        table_name: migrator.table_name.clone(),
        create_schemas: migrator.create_schemas.clone(),
    }
}

async fn run_shared_migrations(pool: PgPool) -> ThreadStoreResult<()> {
    let mut conn = pool.acquire().await.map_err(internal_error)?;
    let migrator = locking_disabled_migrator(&MIGRATOR);
    run_migrations_with_explicit_lock(&migrator, &mut *conn).await
}

async fn run_migrations_with_explicit_lock<C>(
    migrator: &Migrator,
    conn: &mut C,
) -> ThreadStoreResult<()>
where
    C: Migrate,
{
    conn.lock().await.map_err(internal_error)?;
    let migration_result = migrator.run_direct(None, conn, false).await;
    let unlock_result = conn.unlock().await;
    match (migration_result, unlock_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(unlock_err)) => Err(internal_error(format!(
            "failed to release migration advisory lock: {unlock_err}"
        ))),
        (Err(migration_err), Ok(())) => Err(internal_error(migration_err)),
        (Err(migration_err), Err(unlock_err)) => Err(internal_error(format!(
            "{migration_err}; failed to release migration advisory lock: {unlock_err}"
        ))),
    }
}

fn canonical_session_source_key(source: &SessionSource) -> ThreadStoreResult<String> {
    let value = serde_json::to_value(source).map_err(internal_error)?;
    session_source_key_from_value(&value)
}

fn session_source_filter_keys(source: &SessionSource) -> ThreadStoreResult<Vec<String>> {
    let value = serde_json::to_value(source).map_err(internal_error)?;
    let mut keys = Vec::new();
    push_unique_key(&mut keys, session_source_key_from_value(&value)?);
    push_unique_key(&mut keys, format!("{source:?}"));
    push_unique_key(&mut keys, source.to_string());
    if !value.is_string() {
        push_unique_key(&mut keys, postgres_jsonb_compat_key(&value)?);
    }
    Ok(keys)
}

fn session_source_key_from_value(value: &Value) -> ThreadStoreResult<String> {
    match value {
        Value::String(value) => Ok(value.clone()),
        value => serde_json::to_string(value).map_err(internal_error),
    }
}

fn postgres_jsonb_compat_key(value: &Value) -> ThreadStoreResult<String> {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => {
            serde_json::to_string(value).map_err(internal_error)
        }
        Value::String(value) => serde_json::to_string(value).map_err(internal_error),
        Value::Array(values) => values
            .iter()
            .map(postgres_jsonb_compat_key)
            .collect::<ThreadStoreResult<Vec<_>>>()
            .map(|values| format!("[{}]", values.join(", "))),
        Value::Object(map) => {
            let mut entries = map.iter().collect::<Vec<_>>();
            entries.sort_by_key(|(left, _)| *left);
            entries
                .into_iter()
                .map(|(key, value)| {
                    let key = serde_json::to_string(key).map_err(internal_error)?;
                    let value = postgres_jsonb_compat_key(value)?;
                    Ok(format!("{key}: {value}"))
                })
                .collect::<ThreadStoreResult<Vec<_>>>()
                .map(|entries| format!("{{{}}}", entries.join(", ")))
        }
    }
}

fn push_unique_key(keys: &mut Vec<String>, key: String) {
    if !keys.contains(&key) {
        keys.push(key);
    }
}

fn decode_thread_list_cursor(cursor: Option<&str>) -> ThreadStoreResult<ThreadListCursor> {
    let Some(cursor) = cursor else {
        return Ok(ThreadListCursor::Start);
    };
    let value = serde_json::from_str::<Value>(cursor).map_err(invalid_cursor)?;
    if value.get("offset").is_some() {
        return serde_json::from_value::<OffsetCursor>(value)
            .map(|cursor| ThreadListCursor::LegacyOffset(cursor.offset))
            .map_err(invalid_cursor);
    }
    serde_json::from_value::<KeysetCursor>(value)
        .map(ThreadListCursor::Keyset)
        .map_err(invalid_cursor)
}

fn invalid_cursor(err: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: format!("invalid cursor: {err}"),
    }
}

fn validate_keyset_cursor(
    cursor: &KeysetCursor,
    sort_key: ThreadSortKey,
    sort_direction: SortDirection,
) -> ThreadStoreResult<()> {
    if cursor.version != 1 || cursor.sort_key != sort_key || cursor.sort_direction != sort_direction
    {
        return Err(invalid_cursor(
            "cursor does not match the requested sort order",
        ));
    }
    match (sort_key, &cursor.value) {
        (
            ThreadSortKey::CreatedAt | ThreadSortKey::UpdatedAt | ThreadSortKey::RecencyAt,
            KeysetCursorValue::Timestamp(_),
        )
        | (ThreadSortKey::SectionPosition, KeysetCursorValue::SectionPosition(_)) => Ok(()),
        _ => Err(invalid_cursor(
            "cursor value does not match the requested sort key",
        )),
    }
}

fn push_keyset_cursor_filter(
    builder: &mut QueryBuilder<Postgres>,
    cursor: &KeysetCursor,
    sort_column: &str,
    sort_direction: SortDirection,
) {
    let operator = match sort_direction {
        SortDirection::Asc => ">",
        SortDirection::Desc => "<",
    };
    builder.push(" AND (");
    builder.push(sort_column);
    builder.push(" ");
    builder.push(operator);
    builder.push(" ");
    match &cursor.value {
        KeysetCursorValue::Timestamp(value) => {
            builder.push_bind(value.to_owned());
            builder.push(" OR (");
            builder.push(sort_column);
            builder.push(" = ");
            builder.push_bind(value.to_owned());
        }
        KeysetCursorValue::SectionPosition(value) => {
            builder.push_bind(*value);
            builder.push(" OR (");
            builder.push(sort_column);
            builder.push(" = ");
            builder.push_bind(*value);
        }
    }
    builder.push(" AND id ");
    builder.push(operator);
    builder.push(" ");
    builder.push_bind(cursor.thread_id.to_string());
    builder.push("))");
}

fn keyset_cursor_value_from_row(
    row: &sqlx::postgres::PgRow,
    sort_key: ThreadSortKey,
) -> ThreadStoreResult<KeysetCursorValue> {
    match sort_key {
        ThreadSortKey::CreatedAt | ThreadSortKey::UpdatedAt | ThreadSortKey::RecencyAt => row
            .try_get::<DateTime<Utc>, _>("cursor_sort_value")
            .map(KeysetCursorValue::Timestamp)
            .map_err(internal_error),
        ThreadSortKey::SectionPosition => row
            .try_get::<i64, _>("cursor_sort_value")
            .map(KeysetCursorValue::SectionPosition)
            .map_err(internal_error),
    }
}

fn encode_keyset_cursor(
    value: KeysetCursorValue,
    thread_id: ThreadId,
    sort_key: ThreadSortKey,
    sort_direction: SortDirection,
) -> ThreadStoreResult<String> {
    serde_json::to_string(&KeysetCursor {
        version: 1,
        sort_key,
        sort_direction,
        value,
        thread_id,
    })
    .map_err(internal_error)
}

fn graph_status(status: ThreadSpawnEdgeStatus) -> &'static str {
    match status {
        ThreadSpawnEdgeStatus::Open => "open",
        ThreadSpawnEdgeStatus::Closed => "closed",
    }
}

fn thread_goal_from_pg_row(row: &sqlx::postgres::PgRow) -> anyhow::Result<codex_state::ThreadGoal> {
    let thread_id: String = row.try_get("thread_id")?;
    let status: String = row.try_get("status")?;
    let created_at_ms: i64 = row.try_get("created_at_ms")?;
    let updated_at_ms: i64 = row.try_get("updated_at_ms")?;
    Ok(codex_state::ThreadGoal {
        thread_id: ThreadId::try_from(thread_id)?,
        goal_id: row.try_get("goal_id")?,
        objective: row.try_get("objective")?,
        status: codex_state::ThreadGoalStatus::try_from(status.as_str())?,
        token_budget: row.try_get("token_budget")?,
        tokens_used: row.try_get("tokens_used")?,
        time_used_seconds: row.try_get("time_used_seconds")?,
        created_at: epoch_millis_to_datetime(created_at_ms)?,
        updated_at: epoch_millis_to_datetime(updated_at_ms)?,
    })
}

fn status_after_budget_limit(
    status: codex_state::ThreadGoalStatus,
    tokens_used: i64,
    token_budget: Option<i64>,
) -> codex_state::ThreadGoalStatus {
    if status == codex_state::ThreadGoalStatus::Active
        && token_budget.is_some_and(|budget| tokens_used >= budget)
    {
        codex_state::ThreadGoalStatus::BudgetLimited
    } else {
        status
    }
}

fn new_goal_id(thread_id: ThreadId, now: DateTime<Utc>) -> String {
    let timestamp_nanos = now
        .timestamp_nanos_opt()
        .unwrap_or_else(|| now.timestamp_micros().saturating_mul(1000));
    format!("{thread_id}:{timestamp_nanos}")
}

fn datetime_to_epoch_millis(dt: DateTime<Utc>) -> i64 {
    dt.timestamp_millis()
}

fn epoch_millis_to_datetime(value: i64) -> anyhow::Result<DateTime<Utc>> {
    const MIN_EPOCH_MILLIS: i64 = 1_577_836_800_000;
    let millis = if value < MIN_EPOCH_MILLIS {
        value.saturating_mul(1000)
    } else {
        value
    };
    DateTime::<Utc>::from_timestamp_millis(millis)
        .ok_or_else(|| anyhow::anyhow!("invalid unix timestamp millis: {value}"))
}

fn anyhow_from_store(err: impl std::fmt::Display) -> anyhow::Error {
    anyhow::anyhow!("{err}")
}

fn internal_error(err: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: err.to_string(),
    }
}

fn agent_graph_error(err: impl std::fmt::Display) -> codex_agent_graph_store::AgentGraphStoreError {
    codex_agent_graph_store::AgentGraphStoreError::Internal {
        message: err.to_string(),
    }
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
