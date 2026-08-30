use super::*;
use chrono::Duration;
use serde::Serialize;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

const JOB_KIND_MEMORY_STAGE1: &str = "memory_stage1";
const JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL: &str = "memory_consolidate_global";
const MEMORY_CONSOLIDATION_JOB_KEY: &str = "global";
const PHASE2_SUCCESS_COOLDOWN_SECONDS: i64 = 6 * 60 * 60;
const DEFAULT_RETRY_REMAINING: i64 = 3;

static MEMORY_OWNERSHIP_COUNTER: AtomicU64 = AtomicU64::new(0);

impl codex_state::GeneratedMemoryStore for PostgresThreadStore {
    fn clear_memory_data(&self) -> codex_state::GeneratedMemoryStoreFuture<'_, ()> {
        Box::pin(async move {
            self.ensure_migrated().await?;
            let (pool, workspace_id) = self.pool_and_workspace()?;
            let mut tx = pool.begin().await.map_err(anyhow_from_store)?;
            sqlx::query("DELETE FROM memory_stage1_outputs WHERE workspace_id = $1")
                .bind(workspace_id)
                .execute(&mut *tx)
                .await
                .map_err(anyhow_from_store)?;
            sqlx::query(
                r#"
DELETE FROM memory_jobs
WHERE workspace_id = $1 AND (kind = $2 OR kind = $3)
                "#,
            )
            .bind(workspace_id)
            .bind(JOB_KIND_MEMORY_STAGE1)
            .bind(JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL)
            .execute(&mut *tx)
            .await
            .map_err(anyhow_from_store)?;
            tx.commit().await.map_err(anyhow_from_store)
        })
    }

    fn record_stage1_output_usage<'a>(
        &'a self,
        thread_ids: &'a [ThreadId],
    ) -> codex_state::GeneratedMemoryStoreFuture<'a, usize> {
        Box::pin(async move {
            if thread_ids.is_empty() {
                return Ok(0);
            }
            self.ensure_migrated().await?;
            let (pool, workspace_id) = self.pool_and_workspace()?;
            let now = Utc::now().timestamp();
            let mut tx = pool.begin().await.map_err(anyhow_from_store)?;
            let mut updated_rows = 0;
            for thread_id in thread_ids {
                updated_rows += sqlx::query(
                    r#"
UPDATE memory_stage1_outputs
SET
    usage_count = COALESCE(usage_count, 0) + 1,
    last_usage = $3
WHERE workspace_id = $1 AND thread_id = $2
                    "#,
                )
                .bind(workspace_id)
                .bind(thread_id.to_string())
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(anyhow_from_store)?
                .rows_affected() as usize;
            }
            tx.commit().await.map_err(anyhow_from_store)?;
            Ok(updated_rows)
        })
    }

    fn claim_stage1_jobs_for_startup<'a>(
        &'a self,
        current_thread_id: ThreadId,
        params: codex_state::Stage1StartupClaimParams<'a>,
    ) -> codex_state::GeneratedMemoryStoreFuture<'a, Vec<codex_state::Stage1JobClaim>> {
        Box::pin(async move {
            self.ensure_migrated().await?;
            let (pool, workspace_id) = self.pool_and_workspace()?;
            let scan_limit = params.scan_limit;
            let max_claimed = params.max_claimed;
            if scan_limit == 0 || max_claimed == 0 {
                return Ok(Vec::new());
            }

            let worker_id = current_thread_id;
            let current_thread_id = worker_id.to_string();
            let max_age_cutoff = Utc::now() - Duration::days(params.max_age_days.max(0));
            let idle_cutoff = Utc::now() - Duration::hours(params.min_rollout_idle_hours.max(0));
            let allowed_sources = allowed_source_filter_keys(params.allowed_sources)?;
            let rows = sqlx::query(
                r#"
SELECT stored_thread_json
FROM threads
WHERE workspace_id = $1
  AND archived_at IS NULL
  AND source = ANY($2)
  AND memory_mode = 'enabled'
  AND history_mode = ANY($3)
  AND id != $4
  AND updated_at >= $5
  AND updated_at <= $6
ORDER BY updated_at DESC
LIMIT $7
                "#,
            )
            .bind(workspace_id)
            .bind(allowed_sources)
            .bind(generated_memory_history_mode_keys())
            .bind(current_thread_id)
            .bind(max_age_cutoff)
            .bind(idle_cutoff)
            .bind(i64::try_from(scan_limit).unwrap_or(i64::MAX))
            .fetch_all(pool)
            .await
            .map_err(anyhow_from_store)?;

            let mut claimed = Vec::new();
            for row in rows {
                if claimed.len() >= max_claimed {
                    break;
                }
                let stored = stored_thread_from_row(&row).map_err(anyhow_from_store)?;
                let thread = memory_thread_metadata_from_stored_thread(workspace_id, stored)?;
                if !stage1_source_needs_update(
                    pool,
                    workspace_id,
                    thread.id,
                    thread.updated_at.timestamp(),
                )
                .await?
                {
                    continue;
                }
                if let codex_state::Stage1JobClaimOutcome::Claimed { ownership_token } =
                    try_claim_stage1_job(
                        pool,
                        workspace_id,
                        thread.id,
                        worker_id,
                        thread.updated_at.timestamp(),
                        params.lease_seconds,
                        max_claimed,
                    )
                    .await?
                {
                    claimed.push(codex_state::Stage1JobClaim {
                        thread,
                        ownership_token,
                    });
                }
            }

            Ok(claimed)
        })
    }

    fn prune_stage1_outputs_for_retention(
        &self,
        max_unused_days: i64,
        limit: usize,
    ) -> codex_state::GeneratedMemoryStoreFuture<'_, usize> {
        Box::pin(async move {
            if limit == 0 {
                return Ok(0);
            }
            self.ensure_migrated().await?;
            let (pool, workspace_id) = self.pool_and_workspace()?;
            let cutoff = (Utc::now() - Duration::days(max_unused_days.max(0))).timestamp();
            let rows_affected = sqlx::query(
                r#"
DELETE FROM memory_stage1_outputs
WHERE workspace_id = $1
  AND thread_id IN (
    SELECT thread_id
    FROM memory_stage1_outputs
    WHERE workspace_id = $1
      AND selected_for_phase2 = FALSE
      AND COALESCE(last_usage, source_updated_at) < $2
    ORDER BY
      COALESCE(last_usage, source_updated_at) ASC,
      source_updated_at ASC,
      thread_id ASC
    LIMIT $3
  )
                "#,
            )
            .bind(workspace_id)
            .bind(cutoff)
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
            .execute(pool)
            .await
            .map_err(anyhow_from_store)?
            .rows_affected();
            Ok(rows_affected as usize)
        })
    }

    fn get_phase2_input_selection(
        &self,
        n: usize,
        max_unused_days: i64,
    ) -> codex_state::GeneratedMemoryStoreFuture<'_, Vec<codex_state::Stage1Output>> {
        Box::pin(async move {
            if n == 0 {
                return Ok(Vec::new());
            }
            self.ensure_migrated().await?;
            let (pool, workspace_id) = self.pool_and_workspace()?;
            let cutoff = (Utc::now() - Duration::days(max_unused_days.max(0))).timestamp();
            let rows = sqlx::query(
                r#"
SELECT
    so.thread_id,
    so.source_updated_at,
    so.raw_memory,
    so.rollout_summary,
    so.rollout_slug,
    so.generated_at,
    threads.stored_thread_json
FROM memory_stage1_outputs AS so
JOIN threads
  ON threads.workspace_id = so.workspace_id
 AND threads.id = so.thread_id
WHERE so.workspace_id = $1
  AND threads.memory_mode = 'enabled'
  AND threads.history_mode = ANY($2)
  AND (length(trim(so.raw_memory)) > 0 OR length(trim(so.rollout_summary)) > 0)
  AND (
    (so.last_usage IS NOT NULL AND so.last_usage >= $3)
    OR (so.last_usage IS NULL AND so.source_updated_at >= $3)
  )
ORDER BY
    COALESCE(so.usage_count, 0) DESC,
    COALESCE(so.last_usage, so.source_updated_at) DESC,
    so.source_updated_at DESC,
    so.thread_id DESC
LIMIT $4
                "#,
            )
            .bind(workspace_id)
            .bind(generated_memory_history_mode_keys())
            .bind(cutoff)
            .bind(i64::try_from(n).unwrap_or(i64::MAX))
            .fetch_all(pool)
            .await
            .map_err(anyhow_from_store)?;

            let mut selected = rows
                .into_iter()
                .map(|row| stage1_output_from_joined_row(workspace_id, &row))
                .collect::<anyhow::Result<Vec<_>>>()?;
            selected.sort_by_key(|entry| entry.thread_id.to_string());
            Ok(selected)
        })
    }

    fn mark_thread_memory_mode_polluted(
        &self,
        thread_id: ThreadId,
    ) -> codex_state::GeneratedMemoryStoreFuture<'_, bool> {
        Box::pin(async move {
            self.ensure_migrated().await?;
            let (pool, workspace_id) = self.pool_and_workspace()?;
            let now = Utc::now().timestamp();
            let thread_id = thread_id.to_string();
            let selected_for_phase2 = sqlx::query_scalar::<_, Option<bool>>(
                r#"
SELECT selected_for_phase2
FROM memory_stage1_outputs
WHERE workspace_id = $1 AND thread_id = $2
                "#,
            )
            .bind(workspace_id)
            .bind(thread_id.as_str())
            .fetch_optional(pool)
            .await
            .map_err(anyhow_from_store)?
            .flatten()
            .unwrap_or(false);

            let rows_affected = sqlx::query(
                r#"
UPDATE threads
SET
    memory_mode = 'polluted',
    stored_thread_json = jsonb_set(
        stored_thread_json,
        '{memory_mode}',
        to_jsonb('polluted'::text),
        true
    )
WHERE workspace_id = $1
  AND id = $2
  AND memory_mode <> 'polluted'
                "#,
            )
            .bind(workspace_id)
            .bind(thread_id.as_str())
            .execute(pool)
            .await
            .map_err(anyhow_from_store)?
            .rows_affected();

            if selected_for_phase2 {
                enqueue_global_consolidation(pool, workspace_id, now).await?;
            }

            Ok(rows_affected > 0)
        })
    }

    fn mark_stage1_job_succeeded<'a>(
        &'a self,
        thread_id: ThreadId,
        ownership_token: &'a str,
        source_updated_at: i64,
        raw_memory: &'a str,
        rollout_summary: &'a str,
        rollout_slug: Option<&'a str>,
    ) -> codex_state::GeneratedMemoryStoreFuture<'a, bool> {
        Box::pin(async move {
            self.ensure_migrated().await?;
            let (pool, workspace_id) = self.pool_and_workspace()?;
            let now = Utc::now().timestamp();
            let thread_id = thread_id.to_string();
            let mut tx = pool.begin().await.map_err(anyhow_from_store)?;
            let rows_affected = sqlx::query(
                r#"
UPDATE memory_jobs
SET
    status = 'done',
    finished_at = $4,
    lease_until = NULL,
    last_error = NULL,
    last_success_watermark = input_watermark
WHERE workspace_id = $1
  AND kind = $2
  AND job_key = $3
  AND status = 'running'
  AND ownership_token = $5
                "#,
            )
            .bind(workspace_id)
            .bind(JOB_KIND_MEMORY_STAGE1)
            .bind(thread_id.as_str())
            .bind(now)
            .bind(ownership_token)
            .execute(&mut *tx)
            .await
            .map_err(anyhow_from_store)?
            .rows_affected();
            if rows_affected == 0 {
                tx.commit().await.map_err(anyhow_from_store)?;
                return Ok(false);
            }

            sqlx::query(
                r#"
INSERT INTO memory_stage1_outputs (
    workspace_id,
    thread_id,
    source_updated_at,
    raw_memory,
    rollout_summary,
    rollout_slug,
    generated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (workspace_id, thread_id) DO UPDATE SET
    source_updated_at = EXCLUDED.source_updated_at,
    raw_memory = EXCLUDED.raw_memory,
    rollout_summary = EXCLUDED.rollout_summary,
    rollout_slug = EXCLUDED.rollout_slug,
    generated_at = EXCLUDED.generated_at
WHERE EXCLUDED.source_updated_at >= memory_stage1_outputs.source_updated_at
                "#,
            )
            .bind(workspace_id)
            .bind(thread_id.as_str())
            .bind(source_updated_at)
            .bind(raw_memory)
            .bind(rollout_summary)
            .bind(rollout_slug)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(anyhow_from_store)?;

            enqueue_global_consolidation_tx(&mut *tx, workspace_id, source_updated_at).await?;
            tx.commit().await.map_err(anyhow_from_store)?;
            Ok(true)
        })
    }

    fn mark_stage1_job_succeeded_no_output<'a>(
        &'a self,
        thread_id: ThreadId,
        ownership_token: &'a str,
    ) -> codex_state::GeneratedMemoryStoreFuture<'a, bool> {
        Box::pin(async move {
            self.ensure_migrated().await?;
            let (pool, workspace_id) = self.pool_and_workspace()?;
            let now = Utc::now().timestamp();
            let thread_id = thread_id.to_string();
            let mut tx = pool.begin().await.map_err(anyhow_from_store)?;
            let source_updated_at = sqlx::query_scalar::<_, Option<i64>>(
                r#"
UPDATE memory_jobs
SET
    status = 'done',
    finished_at = $4,
    lease_until = NULL,
    last_error = NULL,
    last_success_watermark = input_watermark
WHERE workspace_id = $1
  AND kind = $2
  AND job_key = $3
  AND status = 'running'
  AND ownership_token = $5
RETURNING input_watermark
                "#,
            )
            .bind(workspace_id)
            .bind(JOB_KIND_MEMORY_STAGE1)
            .bind(thread_id.as_str())
            .bind(now)
            .bind(ownership_token)
            .fetch_optional(&mut *tx)
            .await
            .map_err(anyhow_from_store)?
            .flatten();
            let Some(source_updated_at) = source_updated_at else {
                tx.commit().await.map_err(anyhow_from_store)?;
                return Ok(false);
            };

            let deleted_rows = sqlx::query(
                r#"
DELETE FROM memory_stage1_outputs
WHERE workspace_id = $1 AND thread_id = $2
                "#,
            )
            .bind(workspace_id)
            .bind(thread_id.as_str())
            .execute(&mut *tx)
            .await
            .map_err(anyhow_from_store)?
            .rows_affected();
            if deleted_rows > 0 {
                enqueue_global_consolidation_tx(&mut *tx, workspace_id, source_updated_at).await?;
            }

            tx.commit().await.map_err(anyhow_from_store)?;
            Ok(true)
        })
    }

    fn mark_stage1_job_failed<'a>(
        &'a self,
        thread_id: ThreadId,
        ownership_token: &'a str,
        failure_reason: &'a str,
        retry_delay_seconds: i64,
    ) -> codex_state::GeneratedMemoryStoreFuture<'a, bool> {
        Box::pin(async move {
            self.ensure_migrated().await?;
            let (pool, workspace_id) = self.pool_and_workspace()?;
            let now = Utc::now().timestamp();
            let retry_at = now.saturating_add(retry_delay_seconds.max(0));
            let rows_affected = sqlx::query(
                r#"
UPDATE memory_jobs
SET
    status = 'error',
    finished_at = $4,
    lease_until = NULL,
    retry_at = $5,
    retry_remaining = GREATEST(retry_remaining - 1, 0),
    last_error = $6
WHERE workspace_id = $1
  AND kind = $2
  AND job_key = $3
  AND status = 'running'
  AND ownership_token = $7
                "#,
            )
            .bind(workspace_id)
            .bind(JOB_KIND_MEMORY_STAGE1)
            .bind(thread_id.to_string())
            .bind(now)
            .bind(retry_at)
            .bind(failure_reason)
            .bind(ownership_token)
            .execute(pool)
            .await
            .map_err(anyhow_from_store)?
            .rows_affected();
            Ok(rows_affected > 0)
        })
    }

    fn try_claim_global_phase2_job(
        &self,
        worker_id: ThreadId,
        lease_seconds: i64,
    ) -> codex_state::GeneratedMemoryStoreFuture<'_, codex_state::Phase2JobClaimOutcome> {
        Box::pin(async move {
            self.ensure_migrated().await?;
            let (pool, workspace_id) = self.pool_and_workspace()?;
            try_claim_global_phase2_job(pool, workspace_id, worker_id, lease_seconds).await
        })
    }

    fn heartbeat_global_phase2_job<'a>(
        &'a self,
        ownership_token: &'a str,
        lease_seconds: i64,
    ) -> codex_state::GeneratedMemoryStoreFuture<'a, bool> {
        Box::pin(async move {
            self.ensure_migrated().await?;
            let (pool, workspace_id) = self.pool_and_workspace()?;
            let now = Utc::now().timestamp();
            let lease_until = now.saturating_add(lease_seconds.max(0));
            let rows_affected = sqlx::query(
                r#"
UPDATE memory_jobs
SET lease_until = $4
WHERE workspace_id = $1
  AND kind = $2
  AND job_key = $3
  AND status = 'running'
  AND ownership_token = $5
                "#,
            )
            .bind(workspace_id)
            .bind(JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL)
            .bind(MEMORY_CONSOLIDATION_JOB_KEY)
            .bind(lease_until)
            .bind(ownership_token)
            .execute(pool)
            .await
            .map_err(anyhow_from_store)?
            .rows_affected();
            Ok(rows_affected > 0)
        })
    }

    fn mark_global_phase2_job_succeeded<'a>(
        &'a self,
        ownership_token: &'a str,
        completed_watermark: i64,
        selected_outputs: &'a [codex_state::Stage1Output],
    ) -> codex_state::GeneratedMemoryStoreFuture<'a, bool> {
        Box::pin(async move {
            self.ensure_migrated().await?;
            let (pool, workspace_id) = self.pool_and_workspace()?;
            let mut tx = pool.begin().await.map_err(anyhow_from_store)?;
            let rows_affected = sqlx::query(
                r#"
UPDATE memory_jobs
SET
    status = 'done',
    finished_at = $4,
    lease_until = NULL,
    last_error = NULL,
    last_success_watermark = GREATEST(COALESCE(last_success_watermark, 0), $5)
WHERE workspace_id = $1
  AND kind = $2
  AND job_key = $3
  AND status = 'running'
  AND ownership_token = $6
                "#,
            )
            .bind(workspace_id)
            .bind(JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL)
            .bind(MEMORY_CONSOLIDATION_JOB_KEY)
            .bind(Utc::now().timestamp())
            .bind(completed_watermark)
            .bind(ownership_token)
            .execute(&mut *tx)
            .await
            .map_err(anyhow_from_store)?
            .rows_affected();
            if rows_affected == 0 {
                tx.commit().await.map_err(anyhow_from_store)?;
                return Ok(false);
            }

            sqlx::query(
                r#"
UPDATE memory_stage1_outputs
SET
    selected_for_phase2 = FALSE,
    selected_for_phase2_source_updated_at = NULL
WHERE workspace_id = $1
  AND (selected_for_phase2 = TRUE OR selected_for_phase2_source_updated_at IS NOT NULL)
                "#,
            )
            .bind(workspace_id)
            .execute(&mut *tx)
            .await
            .map_err(anyhow_from_store)?;

            for output in selected_outputs {
                sqlx::query(
                    r#"
UPDATE memory_stage1_outputs
SET
    selected_for_phase2 = TRUE,
    selected_for_phase2_source_updated_at = $4
WHERE workspace_id = $1
  AND thread_id = $2
  AND source_updated_at = $3
                    "#,
                )
                .bind(workspace_id)
                .bind(output.thread_id.to_string())
                .bind(output.source_updated_at.timestamp())
                .bind(output.source_updated_at.timestamp())
                .execute(&mut *tx)
                .await
                .map_err(anyhow_from_store)?;
            }

            tx.commit().await.map_err(anyhow_from_store)?;
            Ok(true)
        })
    }

    fn mark_global_phase2_job_failed<'a>(
        &'a self,
        ownership_token: &'a str,
        failure_reason: &'a str,
        retry_delay_seconds: i64,
    ) -> codex_state::GeneratedMemoryStoreFuture<'a, bool> {
        Box::pin(async move {
            self.ensure_migrated().await?;
            let (pool, workspace_id) = self.pool_and_workspace()?;
            let now = Utc::now().timestamp();
            let retry_at = now.saturating_add(retry_delay_seconds.max(0));
            let rows_affected = sqlx::query(
                r#"
UPDATE memory_jobs
SET
    status = 'error',
    finished_at = $4,
    lease_until = NULL,
    retry_at = $5,
    retry_remaining = GREATEST(retry_remaining - 1, 0),
    last_error = $6
WHERE workspace_id = $1
  AND kind = $2
  AND job_key = $3
  AND status = 'running'
  AND ownership_token = $7
                "#,
            )
            .bind(workspace_id)
            .bind(JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL)
            .bind(MEMORY_CONSOLIDATION_JOB_KEY)
            .bind(now)
            .bind(retry_at)
            .bind(failure_reason)
            .bind(ownership_token)
            .execute(pool)
            .await
            .map_err(anyhow_from_store)?
            .rows_affected();
            Ok(rows_affected > 0)
        })
    }

    fn mark_global_phase2_job_failed_if_unowned<'a>(
        &'a self,
        ownership_token: &'a str,
        failure_reason: &'a str,
        retry_delay_seconds: i64,
    ) -> codex_state::GeneratedMemoryStoreFuture<'a, bool> {
        Box::pin(async move {
            self.ensure_migrated().await?;
            let (pool, workspace_id) = self.pool_and_workspace()?;
            let now = Utc::now().timestamp();
            let retry_at = now.saturating_add(retry_delay_seconds.max(0));
            let rows_affected = sqlx::query(
                r#"
UPDATE memory_jobs
SET
    status = 'error',
    finished_at = $4,
    lease_until = NULL,
    retry_at = $5,
    retry_remaining = GREATEST(retry_remaining - 1, 0),
    last_error = $6
WHERE workspace_id = $1
  AND kind = $2
  AND job_key = $3
  AND status = 'running'
  AND (ownership_token = $7 OR ownership_token IS NULL)
                "#,
            )
            .bind(workspace_id)
            .bind(JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL)
            .bind(MEMORY_CONSOLIDATION_JOB_KEY)
            .bind(now)
            .bind(retry_at)
            .bind(failure_reason)
            .bind(ownership_token)
            .execute(pool)
            .await
            .map_err(anyhow_from_store)?
            .rows_affected();
            Ok(rows_affected > 0)
        })
    }
}

fn allowed_source_filter_keys(allowed_sources: &[String]) -> anyhow::Result<Vec<String>> {
    let mut keys = Vec::new();
    for source in allowed_sources {
        push_unique_key(&mut keys, source.clone());
        if let Ok(parsed) = SessionSource::from_startup_arg(source) {
            for key in session_source_filter_keys(&parsed).map_err(anyhow_from_store)? {
                push_unique_key(&mut keys, key);
            }
        }
    }
    Ok(keys)
}

async fn stage1_source_needs_update(
    pool: &PgPool,
    workspace_id: &str,
    thread_id: ThreadId,
    source_updated_at: i64,
) -> anyhow::Result<bool> {
    let thread_id = thread_id.to_string();
    let existing_output = sqlx::query_scalar::<_, Option<i64>>(
        r#"
SELECT source_updated_at
FROM memory_stage1_outputs
WHERE workspace_id = $1 AND thread_id = $2
        "#,
    )
    .bind(workspace_id)
    .bind(thread_id.as_str())
    .fetch_optional(pool)
    .await
    .map_err(anyhow_from_store)?
    .flatten();
    if existing_output.is_some_and(|existing| existing >= source_updated_at) {
        return Ok(false);
    }

    let existing_watermark = sqlx::query_scalar::<_, Option<i64>>(
        r#"
SELECT last_success_watermark
FROM memory_jobs
WHERE workspace_id = $1 AND kind = $2 AND job_key = $3
        "#,
    )
    .bind(workspace_id)
    .bind(JOB_KIND_MEMORY_STAGE1)
    .bind(thread_id.as_str())
    .fetch_optional(pool)
    .await
    .map_err(anyhow_from_store)?
    .flatten();
    Ok(!existing_watermark.is_some_and(|watermark| watermark >= source_updated_at))
}

async fn try_claim_stage1_job(
    pool: &PgPool,
    workspace_id: &str,
    thread_id: ThreadId,
    worker_id: ThreadId,
    source_updated_at: i64,
    lease_seconds: i64,
    max_running_jobs: usize,
) -> anyhow::Result<codex_state::Stage1JobClaimOutcome> {
    let now = Utc::now().timestamp();
    let lease_until = now.saturating_add(lease_seconds.max(0));
    let ownership_token = new_memory_ownership_token("stage1");
    let thread_id = thread_id.to_string();
    let worker_id = worker_id.to_string();
    let mut tx = pool.begin().await.map_err(anyhow_from_store)?;
    memory_advisory_lock(&mut *tx, workspace_id, JOB_KIND_MEMORY_STAGE1).await?;

    let existing_output = sqlx::query_scalar::<_, Option<i64>>(
        r#"
SELECT source_updated_at
FROM memory_stage1_outputs
WHERE workspace_id = $1 AND thread_id = $2
FOR UPDATE
        "#,
    )
    .bind(workspace_id)
    .bind(thread_id.as_str())
    .fetch_optional(&mut *tx)
    .await
    .map_err(anyhow_from_store)?
    .flatten();
    if existing_output.is_some_and(|existing| existing >= source_updated_at) {
        tx.commit().await.map_err(anyhow_from_store)?;
        return Ok(codex_state::Stage1JobClaimOutcome::SkippedUpToDate);
    }

    let existing_job = sqlx::query(
        r#"
SELECT status, lease_until, retry_at, retry_remaining, input_watermark, last_success_watermark
FROM memory_jobs
WHERE workspace_id = $1 AND kind = $2 AND job_key = $3
FOR UPDATE
        "#,
    )
    .bind(workspace_id)
    .bind(JOB_KIND_MEMORY_STAGE1)
    .bind(thread_id.as_str())
    .fetch_optional(&mut *tx)
    .await
    .map_err(anyhow_from_store)?;
    if let Some(existing_job) = existing_job.as_ref() {
        let last_success_watermark = existing_job
            .try_get::<Option<i64>, _>("last_success_watermark")
            .map_err(anyhow_from_store)?;
        if last_success_watermark.is_some_and(|watermark| watermark >= source_updated_at) {
            tx.commit().await.map_err(anyhow_from_store)?;
            return Ok(codex_state::Stage1JobClaimOutcome::SkippedUpToDate);
        }
    }

    let running_jobs = sqlx::query_scalar::<_, i64>(
        r#"
SELECT COUNT(*)
FROM memory_jobs
WHERE workspace_id = $1
  AND kind = $2
  AND status = 'running'
  AND lease_until IS NOT NULL
  AND lease_until > $3
  AND job_key <> $4
        "#,
    )
    .bind(workspace_id)
    .bind(JOB_KIND_MEMORY_STAGE1)
    .bind(now)
    .bind(thread_id.as_str())
    .fetch_one(&mut *tx)
    .await
    .map_err(anyhow_from_store)?;

    if let Some(existing_job) = existing_job {
        let status: String = existing_job.try_get("status").map_err(anyhow_from_store)?;
        let existing_lease_until: Option<i64> = existing_job
            .try_get("lease_until")
            .map_err(anyhow_from_store)?;
        let retry_at: Option<i64> = existing_job
            .try_get("retry_at")
            .map_err(anyhow_from_store)?;
        let retry_remaining: i64 = existing_job
            .try_get("retry_remaining")
            .map_err(anyhow_from_store)?;
        let input_watermark: Option<i64> = existing_job
            .try_get("input_watermark")
            .map_err(anyhow_from_store)?;
        let source_advanced = source_updated_at > input_watermark.unwrap_or(-1);

        if status == "running" && existing_lease_until.is_some_and(|lease_until| lease_until > now)
        {
            tx.commit().await.map_err(anyhow_from_store)?;
            return Ok(codex_state::Stage1JobClaimOutcome::SkippedRunning);
        }
        if !source_advanced {
            if retry_remaining <= 0 {
                tx.commit().await.map_err(anyhow_from_store)?;
                return Ok(codex_state::Stage1JobClaimOutcome::SkippedRetryExhausted);
            }
            if retry_at.is_some_and(|retry_at| retry_at > now) {
                tx.commit().await.map_err(anyhow_from_store)?;
                return Ok(codex_state::Stage1JobClaimOutcome::SkippedRetryBackoff);
            }
        }
        if running_jobs >= i64::try_from(max_running_jobs).unwrap_or(i64::MAX) {
            tx.commit().await.map_err(anyhow_from_store)?;
            return Ok(codex_state::Stage1JobClaimOutcome::SkippedRunning);
        }

        sqlx::query(
            r#"
UPDATE memory_jobs
SET
    status = 'running',
    worker_id = $4,
    ownership_token = $5,
    started_at = $6,
    finished_at = NULL,
    lease_until = $7,
    retry_at = NULL,
    retry_remaining = $8,
    last_error = NULL,
    input_watermark = $9
WHERE workspace_id = $1 AND kind = $2 AND job_key = $3
            "#,
        )
        .bind(workspace_id)
        .bind(JOB_KIND_MEMORY_STAGE1)
        .bind(thread_id.as_str())
        .bind(worker_id.as_str())
        .bind(ownership_token.as_str())
        .bind(now)
        .bind(lease_until)
        .bind(if source_advanced {
            DEFAULT_RETRY_REMAINING
        } else {
            retry_remaining.max(0)
        })
        .bind(source_updated_at)
        .execute(&mut *tx)
        .await
        .map_err(anyhow_from_store)?;
    } else {
        if running_jobs >= i64::try_from(max_running_jobs).unwrap_or(i64::MAX) {
            tx.commit().await.map_err(anyhow_from_store)?;
            return Ok(codex_state::Stage1JobClaimOutcome::SkippedRunning);
        }
        sqlx::query(
            r#"
INSERT INTO memory_jobs (
    workspace_id,
    kind,
    job_key,
    status,
    worker_id,
    ownership_token,
    started_at,
    finished_at,
    lease_until,
    retry_at,
    retry_remaining,
    last_error,
    input_watermark,
    last_success_watermark
) VALUES ($1, $2, $3, 'running', $4, $5, $6, NULL, $7, NULL, $8, NULL, $9, NULL)
            "#,
        )
        .bind(workspace_id)
        .bind(JOB_KIND_MEMORY_STAGE1)
        .bind(thread_id.as_str())
        .bind(worker_id.as_str())
        .bind(ownership_token.as_str())
        .bind(now)
        .bind(lease_until)
        .bind(DEFAULT_RETRY_REMAINING)
        .bind(source_updated_at)
        .execute(&mut *tx)
        .await
        .map_err(anyhow_from_store)?;
    }

    tx.commit().await.map_err(anyhow_from_store)?;
    Ok(codex_state::Stage1JobClaimOutcome::Claimed { ownership_token })
}

async fn try_claim_global_phase2_job(
    pool: &PgPool,
    workspace_id: &str,
    worker_id: ThreadId,
    lease_seconds: i64,
) -> anyhow::Result<codex_state::Phase2JobClaimOutcome> {
    let now = Utc::now().timestamp();
    let lease_until = now.saturating_add(lease_seconds.max(0));
    let cooldown_cutoff = now.saturating_sub(PHASE2_SUCCESS_COOLDOWN_SECONDS);
    let ownership_token = new_memory_ownership_token("phase2");
    let worker_id = worker_id.to_string();
    let mut tx = pool.begin().await.map_err(anyhow_from_store)?;
    memory_advisory_lock(&mut *tx, workspace_id, JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL).await?;

    let existing_job = sqlx::query(
        r#"
SELECT status, lease_until, retry_at, input_watermark, finished_at, last_error
FROM memory_jobs
WHERE workspace_id = $1 AND kind = $2 AND job_key = $3
FOR UPDATE
        "#,
    )
    .bind(workspace_id)
    .bind(JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL)
    .bind(MEMORY_CONSOLIDATION_JOB_KEY)
    .fetch_optional(&mut *tx)
    .await
    .map_err(anyhow_from_store)?;

    let Some(existing_job) = existing_job else {
        sqlx::query(
            r#"
INSERT INTO memory_jobs (
    workspace_id,
    kind,
    job_key,
    status,
    worker_id,
    ownership_token,
    started_at,
    finished_at,
    lease_until,
    retry_at,
    retry_remaining,
    last_error,
    input_watermark,
    last_success_watermark
) VALUES ($1, $2, $3, 'running', $4, $5, $6, NULL, $7, NULL, $8, NULL, 0, 0)
            "#,
        )
        .bind(workspace_id)
        .bind(JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL)
        .bind(MEMORY_CONSOLIDATION_JOB_KEY)
        .bind(worker_id.as_str())
        .bind(ownership_token.as_str())
        .bind(now)
        .bind(lease_until)
        .bind(DEFAULT_RETRY_REMAINING)
        .execute(&mut *tx)
        .await
        .map_err(anyhow_from_store)?;
        tx.commit().await.map_err(anyhow_from_store)?;
        return Ok(codex_state::Phase2JobClaimOutcome::Claimed {
            ownership_token,
            input_watermark: 0,
        });
    };

    let input_watermark = existing_job
        .try_get::<Option<i64>, _>("input_watermark")
        .map_err(anyhow_from_store)?
        .unwrap_or(0);
    let status: String = existing_job.try_get("status").map_err(anyhow_from_store)?;
    let existing_lease_until: Option<i64> = existing_job
        .try_get("lease_until")
        .map_err(anyhow_from_store)?;
    let retry_at: Option<i64> = existing_job
        .try_get("retry_at")
        .map_err(anyhow_from_store)?;
    let finished_at: Option<i64> = existing_job
        .try_get("finished_at")
        .map_err(anyhow_from_store)?;
    let last_error: Option<String> = existing_job
        .try_get("last_error")
        .map_err(anyhow_from_store)?;
    if retry_at.is_some_and(|retry_at| retry_at > now) {
        tx.commit().await.map_err(anyhow_from_store)?;
        return Ok(codex_state::Phase2JobClaimOutcome::SkippedRetryUnavailable);
    }
    if status == "running" && existing_lease_until.is_some_and(|lease_until| lease_until > now) {
        tx.commit().await.map_err(anyhow_from_store)?;
        return Ok(codex_state::Phase2JobClaimOutcome::SkippedRunning);
    }
    if last_error.is_none() && finished_at.is_some_and(|finished_at| finished_at > cooldown_cutoff)
    {
        tx.commit().await.map_err(anyhow_from_store)?;
        return Ok(codex_state::Phase2JobClaimOutcome::SkippedCooldown);
    }

    sqlx::query(
        r#"
UPDATE memory_jobs
SET
    status = 'running',
    worker_id = $4,
    ownership_token = $5,
    started_at = $6,
    finished_at = NULL,
    lease_until = $7,
    retry_at = NULL,
    last_error = NULL
WHERE workspace_id = $1 AND kind = $2 AND job_key = $3
        "#,
    )
    .bind(workspace_id)
    .bind(JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL)
    .bind(MEMORY_CONSOLIDATION_JOB_KEY)
    .bind(worker_id.as_str())
    .bind(ownership_token.as_str())
    .bind(now)
    .bind(lease_until)
    .execute(&mut *tx)
    .await
    .map_err(anyhow_from_store)?;

    tx.commit().await.map_err(anyhow_from_store)?;
    Ok(codex_state::Phase2JobClaimOutcome::Claimed {
        ownership_token,
        input_watermark,
    })
}

async fn enqueue_global_consolidation(
    pool: &PgPool,
    workspace_id: &str,
    input_watermark: i64,
) -> anyhow::Result<()> {
    let mut tx = pool.begin().await.map_err(anyhow_from_store)?;
    enqueue_global_consolidation_tx(&mut *tx, workspace_id, input_watermark).await?;
    tx.commit().await.map_err(anyhow_from_store)
}

async fn enqueue_global_consolidation_tx(
    executor: impl sqlx::Executor<'_, Database = Postgres>,
    workspace_id: &str,
    input_watermark: i64,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
INSERT INTO memory_jobs (
    workspace_id,
    kind,
    job_key,
    status,
    worker_id,
    ownership_token,
    started_at,
    finished_at,
    lease_until,
    retry_at,
    retry_remaining,
    last_error,
    input_watermark,
    last_success_watermark
) VALUES ($1, $2, $3, 'pending', NULL, NULL, NULL, NULL, NULL, NULL, $4, NULL, $5, 0)
ON CONFLICT (workspace_id, kind, job_key) DO UPDATE SET
    status = CASE
        WHEN memory_jobs.status = 'running' THEN 'running'
        ELSE 'pending'
    END,
    retry_at = CASE
        WHEN memory_jobs.status = 'running' THEN memory_jobs.retry_at
        ELSE NULL
    END,
    retry_remaining = GREATEST(memory_jobs.retry_remaining, EXCLUDED.retry_remaining),
    input_watermark = CASE
        WHEN EXCLUDED.input_watermark > COALESCE(memory_jobs.input_watermark, 0)
            THEN EXCLUDED.input_watermark
        ELSE COALESCE(memory_jobs.input_watermark, 0) + 1
    END
        "#,
    )
    .bind(workspace_id)
    .bind(JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL)
    .bind(MEMORY_CONSOLIDATION_JOB_KEY)
    .bind(DEFAULT_RETRY_REMAINING)
    .bind(input_watermark)
    .execute(executor)
    .await
    .map_err(anyhow_from_store)?;
    Ok(())
}

fn stage1_output_from_joined_row(
    workspace_id: &str,
    row: &sqlx::postgres::PgRow,
) -> anyhow::Result<codex_state::Stage1Output> {
    let stored = stored_thread_from_row(row).map_err(anyhow_from_store)?;
    let thread = memory_thread_metadata_from_stored_thread(workspace_id, stored)?;
    let source_updated_at: i64 = row
        .try_get("source_updated_at")
        .map_err(anyhow_from_store)?;
    let generated_at: i64 = row.try_get("generated_at").map_err(anyhow_from_store)?;
    Ok(codex_state::Stage1Output {
        thread_id: thread.id,
        rollout_path: thread.rollout_path,
        source_updated_at: epoch_millis_to_datetime(source_updated_at.saturating_mul(1000))?,
        raw_memory: row.try_get("raw_memory").map_err(anyhow_from_store)?,
        rollout_summary: row.try_get("rollout_summary").map_err(anyhow_from_store)?,
        rollout_slug: row.try_get("rollout_slug").map_err(anyhow_from_store)?,
        cwd: thread.cwd,
        git_branch: thread.git_branch,
        generated_at: epoch_millis_to_datetime(generated_at.saturating_mul(1000))?,
    })
}

fn memory_thread_metadata_from_stored_thread(
    workspace_id: &str,
    stored: StoredThread,
) -> anyhow::Result<codex_state::ThreadMetadata> {
    let StoredThread {
        thread_id,
        rollout_path,
        preview,
        name,
        model_provider,
        model,
        reasoning_effort,
        created_at,
        updated_at,
        recency_at,
        archived_at,
        section,
        section_position,
        section_entered_at,
        project_id,
        cwd,
        cli_version,
        source,
        history_mode,
        thread_source,
        agent_nickname,
        agent_role,
        agent_path,
        git_info,
        approval_mode,
        permission_profile,
        token_usage,
        first_user_message,
        ..
    } = stored;
    let git_sha = git_info
        .as_ref()
        .and_then(|git| git.commit_hash.as_ref().map(|sha| sha.0.clone()));
    let git_branch = git_info.as_ref().and_then(|git| git.branch.clone());
    let git_origin_url = git_info.as_ref().and_then(|git| git.repository_url.clone());
    let rollout_path =
        rollout_path.unwrap_or_else(|| remote_rollout_path(workspace_id, &thread_id));
    Ok(codex_state::ThreadMetadata {
        id: thread_id,
        rollout_path,
        created_at,
        updated_at,
        recency_at,
        source: canonical_session_source_key(&source).map_err(anyhow_from_store)?,
        history_mode,
        thread_source,
        agent_nickname,
        agent_role,
        agent_path,
        model_provider,
        model,
        reasoning_effort,
        cwd,
        cli_version,
        title: name.clone().unwrap_or_default(),
        name,
        preview: Some(preview),
        sandbox_policy: serde_json::to_string(&permission_profile).unwrap_or_default(),
        approval_mode: projection_string(&approval_mode),
        tokens_used: token_usage.map_or(0, |usage| usage.total_tokens),
        first_user_message,
        archived_at,
        section,
        section_position,
        section_entered_at,
        project_id,
        git_sha,
        git_branch,
        git_origin_url,
    })
}

fn projection_string<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(Value::String(value)) => value,
        Ok(other) => other.to_string(),
        Err(_) => String::new(),
    }
}

fn remote_rollout_path(workspace_id: &str, thread_id: &ThreadId) -> PathBuf {
    PathBuf::from(format!(
        "/remote-sql/workspaces/{workspace_id}/threads/{thread_id}.jsonl"
    ))
}

pub(crate) fn generated_memory_history_mode_keys() -> Vec<String> {
    vec!["legacy".to_string(), "paginated".to_string()]
}

fn new_memory_ownership_token(prefix: &str) -> String {
    let counter = MEMORY_OWNERSHIP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp_micros().saturating_mul(1000));
    format!("{prefix}:{now}:{counter}:{}", std::process::id())
}

async fn memory_advisory_lock(
    executor: impl sqlx::Executor<'_, Database = Postgres>,
    workspace_id: &str,
    kind: &str,
) -> anyhow::Result<()> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
        .bind(format!("generated-memory:{workspace_id}:{kind}"))
        .execute(executor)
        .await
        .map_err(anyhow_from_store)?;
    Ok(())
}
