//! PostgreSQL-backed thread persistence for remote Codex workspaces.
//!
//! This crate is the remote-SQL replacement for the local JSONL/SQLite thread
//! store. It keeps the existing `ThreadStore` contract as the integration seam
//! while making PostgreSQL the canonical history and metadata store.

use std::str::FromStr;
use std::sync::Arc;

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
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::SessionSource;
use codex_thread_store::AppendThreadItemsParams;
use codex_thread_store::ArchiveThreadParams;
use codex_thread_store::CreateThreadParams;
use codex_thread_store::DeleteThreadParams;
use codex_thread_store::ItemPage;
use codex_thread_store::ListItemsParams;
use codex_thread_store::ListThreadsParams;
use codex_thread_store::ListTurnsParams;
use codex_thread_store::LoadThreadHistoryParams;
use codex_thread_store::ReadThreadByRolloutPathParams;
use codex_thread_store::ReadThreadParams;
use codex_thread_store::ResumeThreadParams;
use codex_thread_store::SearchThreadsParams;
use codex_thread_store::StoredThread;
use codex_thread_store::StoredThreadHistory;
use codex_thread_store::ThreadPage;
use codex_thread_store::ThreadSearchPage;
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
use sqlx::migrate::Migrator;
use sqlx::postgres::PgConnectOptions;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::OnceCell;

pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

pub const DEFAULT_DATABASE_URL_ENV: &str = "CODEX_REMOTE_SQL_URL";
pub const DEFAULT_WORKSPACE_ID: &str = "default";

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

#[derive(Clone)]
pub struct PostgresThreadStore {
    inner: Arc<PostgresThreadStoreInner>,
}

enum PostgresThreadStoreInner {
    Pool {
        pool: PgPool,
        workspace_id: String,
        redis_url_env: Option<String>,
        migrations: OnceCell<()>,
    },
    Unconfigured {
        message: String,
    },
}

#[derive(Debug, Deserialize, Serialize)]
struct OffsetCursor {
    offset: usize,
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
                migrations: OnceCell::new(),
            }),
        }
    }

    pub async fn migrate(&self) -> ThreadStoreResult<()> {
        self.ensure_migrated().await
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
                pool, migrations, ..
            } => migrations
                .get_or_try_init(|| async { MIGRATOR.run(pool).await.map_err(internal_error) })
                .await
                .map(|_| ()),
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
                model: None,
                reasoning_effort: None,
                created_at: now,
                updated_at: now,
                recency_at: now,
                archived_at: None,
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
            let stored_json = serde_json::to_value(&stored).map_err(internal_error)?;
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
    history_mode, source, thread_source, model_provider, cwd, title, preview,
    created_at, updated_at, recency_at, stored_thread_json
) VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9, $10, $11, $12,
    $13, $14, $15, $16
)
                "#,
            )
            .bind(params.thread_id.to_string())
            .bind(workspace_id)
            .bind(params.session_id.to_string())
            .bind(stored.forked_from_id.map(|id| id.to_string()))
            .bind(stored.parent_thread_id.map(|id| id.to_string()))
            .bind(format!("{:?}", stored.history_mode))
            .bind(source_key)
            .bind(thread_source_key)
            .bind(stored.model_provider.as_str())
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
            let stored_json = serde_json::to_value(&stored).map_err(internal_error)?;
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

    fn persist_thread(&self, _thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
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
            let offset = decode_offset_cursor(params.cursor.as_deref())?;
            if matches!(params.cwd_filters.as_ref(), Some(filters) if filters.is_empty()) {
                return Ok(ThreadPage {
                    items: Vec::new(),
                    next_cursor: None,
                });
            }

            let page_size = params.page_size.max(1);
            let mut builder = QueryBuilder::<Postgres>::new(
                "SELECT stored_thread_json FROM threads WHERE workspace_id = ",
            );
            builder.push_bind(workspace_id);
            if params.archived {
                builder.push(" AND archived_at IS NOT NULL");
            } else {
                builder.push(" AND archived_at IS NULL");
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
            let sort_column = match params.sort_key {
                codex_thread_store::ThreadSortKey::CreatedAt => "created_at",
                codex_thread_store::ThreadSortKey::UpdatedAt => "updated_at",
                codex_thread_store::ThreadSortKey::RecencyAt => "recency_at",
            };
            let sort_direction = match params.sort_direction {
                codex_thread_store::SortDirection::Asc => "ASC",
                codex_thread_store::SortDirection::Desc => "DESC",
            };
            builder.push(" ORDER BY ");
            builder.push(sort_column);
            builder.push(" ");
            builder.push(sort_direction);
            builder.push(", id ");
            builder.push(sort_direction);
            builder.push(" OFFSET ");
            builder.push_bind(offset as i64);
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
            Ok(ThreadPage {
                items,
                next_cursor: if has_next_page {
                    Some(encode_offset_cursor(offset + page_size)?)
                } else {
                    None
                },
            })
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
    ) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(async move {
            self.ensure_migrated().await?;
            let (pool, workspace_id) = self.pool_and_workspace()?;
            let mut tx = pool.begin().await.map_err(internal_error)?;
            let row = sqlx::query(
                r#"
SELECT stored_thread_json
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
            apply_metadata_patch(&mut stored, params.patch);
            let archived_at = stored.archived_at;
            if archived_at.is_some() && !params.include_archived {
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!("thread {} is archived", params.thread_id),
                });
            }
            let source_key = canonical_session_source_key(&stored.source)?;
            let thread_source_key = stored.thread_source.as_ref().map(ToString::to_string);
            let stored_json = serde_json::to_value(&stored).map_err(internal_error)?;
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
    stored_thread_json = $14
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
                    .map(|effort| effort.to_string()),
            )
            .bind(stored.cwd.to_string_lossy().to_string())
            .bind(stored.name.as_deref())
            .bind(stored.preview.as_str())
            .bind(stored.archived_at)
            .bind(stored.updated_at)
            .bind(stored.recency_at)
            .bind(source_key)
            .bind(thread_source_key)
            .bind(stored_json)
            .execute(&mut *tx)
            .await
            .map_err(internal_error)?;
            tx.commit().await.map_err(internal_error)?;
            Ok(stored)
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
            let stored_json = serde_json::to_value(&stored).map_err(internal_error)?;
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
            let stored_json = serde_json::to_value(&stored).map_err(internal_error)?;
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
        stored.reasoning_effort = Some(reasoning_effort);
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
    if let Some(git_info) = patch.git_info {
        let stored_git_info = stored.git_info.get_or_insert_with(|| GitInfo {
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

fn stored_thread_from_row(row: &sqlx::postgres::PgRow) -> ThreadStoreResult<StoredThread> {
    let value: serde_json::Value = row.try_get("stored_thread_json").map_err(internal_error)?;
    serde_json::from_value(value).map_err(internal_error)
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
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
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

fn decode_offset_cursor(cursor: Option<&str>) -> ThreadStoreResult<usize> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };
    serde_json::from_str::<OffsetCursor>(cursor)
        .map(|cursor| cursor.offset)
        .map_err(|err| ThreadStoreError::InvalidRequest {
            message: format!("invalid cursor: {err}"),
        })
}

fn encode_offset_cursor(offset: usize) -> ThreadStoreResult<String> {
    serde_json::to_string(&OffsetCursor { offset }).map_err(internal_error)
}

fn graph_status(status: ThreadSpawnEdgeStatus) -> &'static str {
    match status {
        ThreadSpawnEdgeStatus::Open => "open",
        ThreadSpawnEdgeStatus::Closed => "closed",
    }
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
