CREATE TABLE IF NOT EXISTS memory_stage1_outputs (
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    thread_id TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    source_updated_at BIGINT NOT NULL,
    raw_memory TEXT NOT NULL,
    rollout_summary TEXT NOT NULL,
    rollout_slug TEXT,
    generated_at BIGINT NOT NULL,
    usage_count BIGINT,
    last_usage BIGINT,
    selected_for_phase2 BOOLEAN NOT NULL DEFAULT FALSE,
    selected_for_phase2_source_updated_at BIGINT,
    PRIMARY KEY (workspace_id, thread_id)
);

CREATE INDEX IF NOT EXISTS idx_memory_stage1_outputs_workspace_selection
    ON memory_stage1_outputs(workspace_id, source_updated_at DESC, thread_id DESC);

CREATE INDEX IF NOT EXISTS idx_memory_stage1_outputs_workspace_retention
    ON memory_stage1_outputs(
        workspace_id,
        selected_for_phase2,
        last_usage,
        source_updated_at,
        thread_id
    );

CREATE TABLE IF NOT EXISTS memory_jobs (
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    job_key TEXT NOT NULL,
    status TEXT NOT NULL,
    worker_id TEXT,
    ownership_token TEXT,
    started_at BIGINT,
    finished_at BIGINT,
    lease_until BIGINT,
    retry_at BIGINT,
    retry_remaining BIGINT NOT NULL,
    last_error TEXT,
    input_watermark BIGINT,
    last_success_watermark BIGINT,
    PRIMARY KEY (workspace_id, kind, job_key)
);

CREATE INDEX IF NOT EXISTS idx_memory_jobs_workspace_running
    ON memory_jobs(workspace_id, kind, status, lease_until, job_key);
