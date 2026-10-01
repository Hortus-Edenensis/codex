use anyhow::Context;
use chrono::DateTime;
use chrono::Utc;
use codex_state::AgentJob;
use codex_state::AgentJobCreateParams;
use codex_state::AgentJobItem;
use codex_state::AgentJobItemCreateParams;
use codex_state::AgentJobItemStatus;
use codex_state::AgentJobProgress;
use codex_state::AgentJobStatus;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use sqlx::Postgres;
use sqlx::QueryBuilder;
use sqlx::Row;

use crate::PostgresThreadStore;
use crate::anyhow_from_store;

#[derive(Debug, Serialize, Deserialize)]
struct AgentJobPayload {
    name: String,
    instruction: String,
    auto_export: bool,
    max_runtime_seconds: Option<u64>,
    output_schema_json: Option<Value>,
    input_headers: Vec<String>,
    input_csv_path: String,
    output_csv_path: String,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AgentJobItemPayload {
    row_index: i64,
    source_id: Option<String>,
    row_json: Value,
    attempt_count: i64,
    result_json: Option<Value>,
    last_error: Option<String>,
    created_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
    reported_at: Option<DateTime<Utc>>,
}

struct AgentJobRecord {
    id: String,
    status: String,
    payload: Value,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

struct AgentJobItemRecord {
    job_id: String,
    item_id: String,
    status: String,
    assigned_thread_id: Option<String>,
    payload: Value,
    updated_at: DateTime<Utc>,
}

impl TryFrom<AgentJobRecord> for AgentJob {
    type Error = anyhow::Error;

    fn try_from(value: AgentJobRecord) -> Result<Self, Self::Error> {
        let payload: AgentJobPayload = serde_json::from_value(value.payload)?;
        Ok(Self {
            id: value.id,
            name: payload.name,
            status: AgentJobStatus::parse(value.status.as_str())?,
            instruction: payload.instruction,
            auto_export: payload.auto_export,
            max_runtime_seconds: payload.max_runtime_seconds,
            output_schema_json: payload.output_schema_json,
            input_headers: payload.input_headers,
            input_csv_path: payload.input_csv_path,
            output_csv_path: payload.output_csv_path,
            created_at: value.created_at,
            updated_at: value.updated_at,
            started_at: payload.started_at,
            completed_at: payload.completed_at,
            last_error: payload.last_error,
        })
    }
}

impl TryFrom<AgentJobItemRecord> for AgentJobItem {
    type Error = anyhow::Error;

    fn try_from(value: AgentJobItemRecord) -> Result<Self, Self::Error> {
        let payload: AgentJobItemPayload = serde_json::from_value(value.payload)?;
        Ok(Self {
            job_id: value.job_id,
            item_id: value.item_id,
            row_index: payload.row_index,
            source_id: payload.source_id,
            row_json: payload.row_json,
            status: AgentJobItemStatus::parse(value.status.as_str())?,
            assigned_thread_id: value.assigned_thread_id,
            attempt_count: payload.attempt_count,
            result_json: payload.result_json,
            last_error: payload.last_error,
            created_at: payload.created_at,
            updated_at: value.updated_at,
            completed_at: payload.completed_at,
            reported_at: payload.reported_at,
        })
    }
}

impl PostgresThreadStore {
    pub async fn create_agent_job(
        &self,
        params: &AgentJobCreateParams,
        items: &[AgentJobItemCreateParams],
    ) -> anyhow::Result<AgentJob> {
        self.ensure_migrated().await.map_err(anyhow_from_store)?;
        let (pool, workspace_id) = self.pool_and_workspace().map_err(anyhow_from_store)?;
        let now = Utc::now();
        let payload = serde_json::to_value(AgentJobPayload {
            name: params.name.clone(),
            instruction: params.instruction.clone(),
            auto_export: params.auto_export,
            max_runtime_seconds: params.max_runtime_seconds,
            output_schema_json: params.output_schema_json.clone(),
            input_headers: params.input_headers.clone(),
            input_csv_path: params.input_csv_path.clone(),
            output_csv_path: params.output_csv_path.clone(),
            started_at: None,
            completed_at: None,
            last_error: None,
        })?;
        let mut tx = pool.begin().await?;
        sqlx::query(
            r#"
INSERT INTO agent_jobs (id, workspace_id, status, payload, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(params.id.as_str())
        .bind(workspace_id)
        .bind(AgentJobStatus::Pending.as_str())
        .bind(payload)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        for item in items {
            let payload = serde_json::to_value(AgentJobItemPayload {
                row_index: item.row_index,
                source_id: item.source_id.clone(),
                row_json: item.row_json.clone(),
                attempt_count: 0,
                result_json: None,
                last_error: None,
                created_at: now,
                completed_at: None,
                reported_at: None,
            })?;
            sqlx::query(
                r#"
INSERT INTO agent_job_items (
    job_id, item_id, status, assigned_thread_id, payload, updated_at
) VALUES ($1, $2, $3, NULL, $4, $5)
                "#,
            )
            .bind(params.id.as_str())
            .bind(item.item_id.as_str())
            .bind(AgentJobItemStatus::Pending.as_str())
            .bind(payload)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        self.get_agent_job(params.id.as_str())
            .await?
            .ok_or_else(|| anyhow::anyhow!("failed to load created agent job {}", params.id))
    }

    pub async fn get_agent_job(&self, job_id: &str) -> anyhow::Result<Option<AgentJob>> {
        self.ensure_migrated().await.map_err(anyhow_from_store)?;
        let (pool, workspace_id) = self.pool_and_workspace().map_err(anyhow_from_store)?;
        let row = sqlx::query(
            r#"
SELECT id, status, payload, created_at, updated_at
FROM agent_jobs
WHERE workspace_id = $1 AND id = $2
            "#,
        )
        .bind(workspace_id)
        .bind(job_id)
        .fetch_optional(pool)
        .await?;
        row.map(agent_job_record_from_row)
            .transpose()?
            .map(AgentJob::try_from)
            .transpose()
    }

    pub async fn list_agent_job_items(
        &self,
        job_id: &str,
        status: Option<AgentJobItemStatus>,
        limit: Option<usize>,
    ) -> anyhow::Result<Vec<AgentJobItem>> {
        self.ensure_migrated().await.map_err(anyhow_from_store)?;
        let (pool, workspace_id) = self.pool_and_workspace().map_err(anyhow_from_store)?;
        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
SELECT item.job_id, item.item_id, item.status, item.assigned_thread_id,
       item.payload, item.updated_at
FROM agent_job_items AS item
JOIN agent_jobs AS job ON job.id = item.job_id
WHERE job.workspace_id =
            "#,
        );
        builder.push(" ").push_bind(workspace_id);
        builder.push(" AND item.job_id = ");
        builder.push_bind(job_id);
        if let Some(status) = status {
            builder.push(" AND item.status = ");
            builder.push_bind(status.as_str());
        }
        builder.push(" ORDER BY (item.payload->>'row_index')::bigint ASC");
        if let Some(limit) = limit {
            builder.push(" LIMIT ");
            builder.push_bind(limit as i64);
        }
        let rows = builder.build().fetch_all(pool).await?;
        rows.into_iter()
            .map(agent_job_item_record_from_row)
            .map(|record| record.and_then(AgentJobItem::try_from))
            .collect()
    }

    pub async fn get_agent_job_item(
        &self,
        job_id: &str,
        item_id: &str,
    ) -> anyhow::Result<Option<AgentJobItem>> {
        self.ensure_migrated().await.map_err(anyhow_from_store)?;
        let (pool, workspace_id) = self.pool_and_workspace().map_err(anyhow_from_store)?;
        let row = sqlx::query(
            r#"
SELECT item.job_id, item.item_id, item.status, item.assigned_thread_id,
       item.payload, item.updated_at
FROM agent_job_items AS item
JOIN agent_jobs AS job ON job.id = item.job_id
WHERE job.workspace_id = $1 AND item.job_id = $2 AND item.item_id = $3
            "#,
        )
        .bind(workspace_id)
        .bind(job_id)
        .bind(item_id)
        .fetch_optional(pool)
        .await?;
        row.map(agent_job_item_record_from_row)
            .transpose()?
            .map(AgentJobItem::try_from)
            .transpose()
    }

    pub async fn mark_agent_job_running(&self, job_id: &str) -> anyhow::Result<()> {
        self.update_agent_job_payload(job_id, AgentJobStatus::Running, |payload, now| {
            payload.started_at.get_or_insert(now);
            payload.completed_at = None;
            payload.last_error = None;
        })
        .await
    }

    pub async fn mark_agent_job_completed(&self, job_id: &str) -> anyhow::Result<()> {
        self.update_agent_job_payload(job_id, AgentJobStatus::Completed, |payload, now| {
            payload.completed_at = Some(now);
            payload.last_error = None;
        })
        .await
    }

    pub async fn mark_agent_job_failed(
        &self,
        job_id: &str,
        error_message: &str,
    ) -> anyhow::Result<()> {
        self.update_agent_job_payload(job_id, AgentJobStatus::Failed, |payload, now| {
            payload.completed_at = Some(now);
            payload.last_error = Some(error_message.to_string());
        })
        .await
    }

    pub async fn mark_agent_job_cancelled(
        &self,
        job_id: &str,
        reason: &str,
    ) -> anyhow::Result<bool> {
        self.ensure_migrated().await.map_err(anyhow_from_store)?;
        let (pool, workspace_id) = self.pool_and_workspace().map_err(anyhow_from_store)?;
        let now = Utc::now();
        let mut tx = pool.begin().await?;
        let row = sqlx::query(
            r#"
SELECT status, payload
FROM agent_jobs
WHERE workspace_id = $1 AND id = $2
FOR UPDATE
            "#,
        )
        .bind(workspace_id)
        .bind(job_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else {
            tx.rollback().await?;
            return Ok(false);
        };
        let status: String = row.try_get("status")?;
        if !matches!(
            AgentJobStatus::parse(status.as_str())?,
            AgentJobStatus::Pending | AgentJobStatus::Running
        ) {
            tx.rollback().await?;
            return Ok(false);
        }
        let mut payload: AgentJobPayload = serde_json::from_value(row.try_get("payload")?)?;
        payload.completed_at = Some(now);
        payload.last_error = Some(reason.to_string());
        sqlx::query(
            "UPDATE agent_jobs SET status = $3, payload = $4, updated_at = $5 WHERE workspace_id = $1 AND id = $2",
        )
        .bind(workspace_id)
        .bind(job_id)
        .bind(AgentJobStatus::Cancelled.as_str())
        .bind(serde_json::to_value(payload)?)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }

    pub async fn is_agent_job_cancelled(&self, job_id: &str) -> anyhow::Result<bool> {
        Ok(self
            .get_agent_job(job_id)
            .await?
            .is_some_and(|job| job.status == AgentJobStatus::Cancelled))
    }

    pub async fn mark_agent_job_item_running(
        &self,
        job_id: &str,
        item_id: &str,
    ) -> anyhow::Result<bool> {
        self.claim_agent_job_item(job_id, item_id, /*thread_id*/ None)
            .await
    }

    pub async fn mark_agent_job_item_running_with_thread(
        &self,
        job_id: &str,
        item_id: &str,
        thread_id: &str,
    ) -> anyhow::Result<bool> {
        self.claim_agent_job_item(job_id, item_id, Some(thread_id))
            .await
    }

    pub async fn mark_agent_job_item_pending(
        &self,
        job_id: &str,
        item_id: &str,
        error_message: Option<&str>,
    ) -> anyhow::Result<bool> {
        self.update_agent_job_item_if_status(
            job_id,
            item_id,
            AgentJobItemStatus::Running,
            AgentJobItemStatus::Pending,
            /*assigned_thread_id*/ None,
            /*expected_assigned_thread_id*/ None,
            |payload, _now| {
                payload.last_error = error_message.map(str::to_string);
            },
        )
        .await
    }

    pub async fn set_agent_job_item_thread(
        &self,
        job_id: &str,
        item_id: &str,
        thread_id: &str,
    ) -> anyhow::Result<bool> {
        self.ensure_migrated().await.map_err(anyhow_from_store)?;
        let now = Utc::now();
        let (pool, workspace_id) = self.pool_and_workspace().map_err(anyhow_from_store)?;
        let result = sqlx::query(
            r#"
UPDATE agent_job_items AS item
SET assigned_thread_id = $4, updated_at = $5
FROM agent_jobs AS job
WHERE job.id = item.job_id
  AND job.workspace_id = $1
  AND item.job_id = $2
  AND item.item_id = $3
  AND item.status = $6
            "#,
        )
        .bind(workspace_id)
        .bind(job_id)
        .bind(item_id)
        .bind(thread_id)
        .bind(now)
        .bind(AgentJobItemStatus::Running.as_str())
        .execute(pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn report_agent_job_item_result(
        &self,
        job_id: &str,
        item_id: &str,
        reporting_thread_id: &str,
        result_json: &Value,
    ) -> anyhow::Result<bool> {
        self.update_agent_job_item_if_status(
            job_id,
            item_id,
            AgentJobItemStatus::Running,
            AgentJobItemStatus::Completed,
            /*assigned_thread_id*/ None,
            /*expected_assigned_thread_id*/ Some(reporting_thread_id),
            |payload, now| {
                payload.result_json = Some(result_json.clone());
                payload.reported_at = Some(now);
                payload.completed_at = Some(now);
                payload.last_error = None;
            },
        )
        .await
    }

    pub async fn mark_agent_job_item_completed(
        &self,
        job_id: &str,
        item_id: &str,
    ) -> anyhow::Result<bool> {
        let Some(item) = self.get_agent_job_item(job_id, item_id).await? else {
            return Ok(false);
        };
        if item.status != AgentJobItemStatus::Running || item.result_json.is_none() {
            return Ok(false);
        }
        self.update_agent_job_item_if_status(
            job_id,
            item_id,
            AgentJobItemStatus::Running,
            AgentJobItemStatus::Completed,
            /*assigned_thread_id*/ None,
            /*expected_assigned_thread_id*/ None,
            |payload, now| {
                payload.completed_at = Some(now);
            },
        )
        .await
    }

    pub async fn mark_agent_job_item_failed(
        &self,
        job_id: &str,
        item_id: &str,
        error_message: &str,
    ) -> anyhow::Result<bool> {
        self.update_agent_job_item_if_status(
            job_id,
            item_id,
            AgentJobItemStatus::Running,
            AgentJobItemStatus::Failed,
            /*assigned_thread_id*/ None,
            /*expected_assigned_thread_id*/ None,
            |payload, now| {
                payload.completed_at = Some(now);
                payload.last_error = Some(error_message.to_string());
            },
        )
        .await
    }

    pub async fn get_agent_job_progress(&self, job_id: &str) -> anyhow::Result<AgentJobProgress> {
        self.ensure_migrated().await.map_err(anyhow_from_store)?;
        let (pool, workspace_id) = self.pool_and_workspace().map_err(anyhow_from_store)?;
        let row = sqlx::query(
            r#"
SELECT
    COUNT(*) AS total_items,
    SUM(CASE WHEN item.status = $3 THEN 1 ELSE 0 END) AS pending_items,
    SUM(CASE WHEN item.status = $4 THEN 1 ELSE 0 END) AS running_items,
    SUM(CASE WHEN item.status = $5 THEN 1 ELSE 0 END) AS completed_items,
    SUM(CASE WHEN item.status = $6 THEN 1 ELSE 0 END) AS failed_items
FROM agent_job_items AS item
JOIN agent_jobs AS job ON job.id = item.job_id
WHERE job.workspace_id = $1 AND item.job_id = $2
            "#,
        )
        .bind(workspace_id)
        .bind(job_id)
        .bind(AgentJobItemStatus::Pending.as_str())
        .bind(AgentJobItemStatus::Running.as_str())
        .bind(AgentJobItemStatus::Completed.as_str())
        .bind(AgentJobItemStatus::Failed.as_str())
        .fetch_one(pool)
        .await?;
        let total_items: i64 = row.try_get("total_items")?;
        let pending_items: Option<i64> = row.try_get("pending_items")?;
        let running_items: Option<i64> = row.try_get("running_items")?;
        let completed_items: Option<i64> = row.try_get("completed_items")?;
        let failed_items: Option<i64> = row.try_get("failed_items")?;
        Ok(AgentJobProgress {
            total_items: usize::try_from(total_items).unwrap_or_default(),
            pending_items: usize::try_from(pending_items.unwrap_or_default()).unwrap_or_default(),
            running_items: usize::try_from(running_items.unwrap_or_default()).unwrap_or_default(),
            completed_items: usize::try_from(completed_items.unwrap_or_default())
                .unwrap_or_default(),
            failed_items: usize::try_from(failed_items.unwrap_or_default()).unwrap_or_default(),
        })
    }

    async fn update_agent_job_payload(
        &self,
        job_id: &str,
        status: AgentJobStatus,
        update: impl FnOnce(&mut AgentJobPayload, DateTime<Utc>),
    ) -> anyhow::Result<()> {
        self.ensure_migrated().await.map_err(anyhow_from_store)?;
        let (pool, workspace_id) = self.pool_and_workspace().map_err(anyhow_from_store)?;
        let now = Utc::now();
        let row = sqlx::query("SELECT payload FROM agent_jobs WHERE workspace_id = $1 AND id = $2")
            .bind(workspace_id)
            .bind(job_id)
            .fetch_optional(pool)
            .await?
            .with_context(|| format!("agent job not found: {job_id}"))?;
        let mut payload: AgentJobPayload = serde_json::from_value(row.try_get("payload")?)?;
        update(&mut payload, now);
        sqlx::query(
            "UPDATE agent_jobs SET status = $3, payload = $4, updated_at = $5 WHERE workspace_id = $1 AND id = $2",
        )
        .bind(workspace_id)
        .bind(job_id)
        .bind(status.as_str())
        .bind(serde_json::to_value(payload)?)
        .bind(now)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn claim_agent_job_item(
        &self,
        job_id: &str,
        item_id: &str,
        thread_id: Option<&str>,
    ) -> anyhow::Result<bool> {
        self.update_agent_job_item_if_status(
            job_id,
            item_id,
            AgentJobItemStatus::Pending,
            AgentJobItemStatus::Running,
            thread_id,
            /*expected_assigned_thread_id*/ None,
            |payload, _now| {
                payload.attempt_count += 1;
                payload.last_error = None;
            },
        )
        .await
    }

    async fn update_agent_job_item_if_status(
        &self,
        job_id: &str,
        item_id: &str,
        expected_status: AgentJobItemStatus,
        next_status: AgentJobItemStatus,
        assigned_thread_id: Option<&str>,
        expected_assigned_thread_id: Option<&str>,
        update: impl FnOnce(&mut AgentJobItemPayload, DateTime<Utc>),
    ) -> anyhow::Result<bool> {
        self.ensure_migrated().await.map_err(anyhow_from_store)?;
        let (pool, workspace_id) = self.pool_and_workspace().map_err(anyhow_from_store)?;
        let now = Utc::now();
        let mut tx = pool.begin().await?;
        let row = sqlx::query(
            r#"
SELECT item.status, item.assigned_thread_id, item.payload
FROM agent_job_items AS item
JOIN agent_jobs AS job ON job.id = item.job_id
WHERE job.workspace_id = $1 AND item.job_id = $2 AND item.item_id = $3
FOR UPDATE OF item
            "#,
        )
        .bind(workspace_id)
        .bind(job_id)
        .bind(item_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else {
            tx.rollback().await?;
            return Ok(false);
        };
        let status: String = row.try_get("status")?;
        if AgentJobItemStatus::parse(status.as_str())? != expected_status {
            tx.rollback().await?;
            return Ok(false);
        }
        if let Some(expected_assigned_thread_id) = expected_assigned_thread_id {
            let row_assigned_thread_id: Option<String> = row.try_get("assigned_thread_id")?;
            if row_assigned_thread_id.as_deref() != Some(expected_assigned_thread_id) {
                tx.rollback().await?;
                return Ok(false);
            }
        }
        let mut payload: AgentJobItemPayload = serde_json::from_value(row.try_get("payload")?)?;
        update(&mut payload, now);
        sqlx::query(
            r#"
UPDATE agent_job_items AS item
SET status = $4, assigned_thread_id = $5, payload = $6, updated_at = $7
FROM agent_jobs AS job
WHERE job.id = item.job_id
  AND job.workspace_id = $1
  AND item.job_id = $2
  AND item.item_id = $3
            "#,
        )
        .bind(workspace_id)
        .bind(job_id)
        .bind(item_id)
        .bind(next_status.as_str())
        .bind(assigned_thread_id)
        .bind(serde_json::to_value(payload)?)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }
}

fn agent_job_record_from_row(row: sqlx::postgres::PgRow) -> anyhow::Result<AgentJobRecord> {
    Ok(AgentJobRecord {
        id: row.try_get("id")?,
        status: row.try_get("status")?,
        payload: row.try_get("payload")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn agent_job_item_record_from_row(
    row: sqlx::postgres::PgRow,
) -> anyhow::Result<AgentJobItemRecord> {
    Ok(AgentJobItemRecord {
        job_id: row.try_get("job_id")?,
        item_id: row.try_get("item_id")?,
        status: row.try_get("status")?,
        assigned_thread_id: row.try_get("assigned_thread_id")?,
        payload: row.try_get("payload")?,
        updated_at: row.try_get("updated_at")?,
    })
}
