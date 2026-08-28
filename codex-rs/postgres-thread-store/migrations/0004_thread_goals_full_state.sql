ALTER TABLE thread_goals ADD COLUMN IF NOT EXISTS goal_id TEXT;
ALTER TABLE thread_goals ADD COLUMN IF NOT EXISTS token_budget BIGINT;
ALTER TABLE thread_goals ADD COLUMN IF NOT EXISTS tokens_used BIGINT NOT NULL DEFAULT 0;
ALTER TABLE thread_goals ADD COLUMN IF NOT EXISTS time_used_seconds BIGINT NOT NULL DEFAULT 0;
ALTER TABLE thread_goals ADD COLUMN IF NOT EXISTS created_at_ms BIGINT;
ALTER TABLE thread_goals ADD COLUMN IF NOT EXISTS updated_at_ms BIGINT;

UPDATE thread_goals
SET
    goal_id = COALESCE(goal_id, 'imported-' || thread_id),
    created_at_ms = COALESCE(
        created_at_ms,
        floor(extract(epoch from COALESCE(updated_at, now())) * 1000)::BIGINT
    ),
    updated_at_ms = COALESCE(
        updated_at_ms,
        floor(extract(epoch from COALESCE(updated_at, now())) * 1000)::BIGINT
    );

ALTER TABLE thread_goals ALTER COLUMN goal_id SET NOT NULL;
ALTER TABLE thread_goals ALTER COLUMN created_at_ms SET NOT NULL;
ALTER TABLE thread_goals ALTER COLUMN updated_at_ms SET NOT NULL;
