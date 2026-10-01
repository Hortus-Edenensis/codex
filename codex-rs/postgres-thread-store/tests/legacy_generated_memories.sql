\set ON_ERROR_STOP on
BEGIN;
CREATE SCHEMA legacy_memory_migration_test;
SET LOCAL search_path TO legacy_memory_migration_test;
\ir ../migrations/0001_remote_sql_threads.sql
\ir ../migrations/0007_generated_memory_pipeline.sql
INSERT INTO workspaces (id, name) VALUES ('test', 'test'), ('other', 'other');
INSERT INTO sessions (id, workspace_id) VALUES ('session', 'test');
INSERT INTO threads (id, workspace_id, session_id, history_mode, source, model_provider, cwd,
    created_at, updated_at, recency_at, stored_thread_json)
SELECT 'thread-' || n, 'test', 'session', 'legacy', 'cli', 'test', '/', now(), now(), now(), '{}'
FROM generate_series(1, 16) n;
INSERT INTO memories (id, workspace_id, thread_id, payload)
SELECT 'legacy-' || n, 'test', 'thread-' || n, jsonb_build_object(
    'thread_id', 'thread-' || n, 'source_updated_at', 100, 'generated_at', 110,
    'raw_memory', 'original raw', 'rollout_summary', 'original summary', 'rollout_slug', NULL,
    'usage_count', 2, 'last_usage', 120, 'selected_for_phase2', 0,
    'selected_for_phase2_source_updated_at', NULL)
FROM generate_series(1, 16) n;
UPDATE memories SET payload = payload || '{"source_updated_at":90,"generated_at":500}' WHERE id = 'legacy-2';
UPDATE memories SET payload = payload || '{"source_updated_at":101,"generated_at":111}' WHERE id = 'legacy-3';
UPDATE memories SET payload = payload || '{"selected_for_phase2":true,"usage_count":null,"last_usage":null}' WHERE id = 'legacy-4';
UPDATE memories SET payload = payload || '{"selected_for_phase2":1}' WHERE id = 'legacy-5';
UPDATE memories SET payload = payload || '{"source_updated_at":"bad"}' WHERE id = 'legacy-6';
UPDATE memories SET payload = payload || '{"source_updated_at":9223372036854775808}' WHERE id = 'legacy-7';
UPDATE memories SET payload = payload || '{"usage_count":-1}' WHERE id = 'legacy-8';
UPDATE memories SET payload = payload || '{"last_usage":1.5}' WHERE id = 'legacy-9';
UPDATE memories SET payload = payload || '{"raw_memory":" \n\t"}' WHERE id = 'legacy-10';
UPDATE memories SET payload = payload || '{"rollout_summary":""}' WHERE id = 'legacy-11';
UPDATE memories SET payload = payload || '{"selected_for_phase2":2}' WHERE id = 'legacy-12';
UPDATE memories SET payload = payload || '{"thread_id":"wrong-thread"}' WHERE id = 'legacy-13';
UPDATE memories SET workspace_id = 'other' WHERE id = 'legacy-14';
UPDATE memories SET payload = '[]' WHERE id = 'legacy-15';
UPDATE memories SET payload = payload - 'generated_at' WHERE id = 'legacy-16';
INSERT INTO memories (id, workspace_id, thread_id, payload)
SELECT 'newest', workspace_id, thread_id, payload || '{"generated_at":111,"raw_memory":"newest original"}'
FROM memories WHERE id = 'legacy-1';
INSERT INTO memory_stage1_outputs (workspace_id, thread_id, source_updated_at, raw_memory,
    rollout_summary, generated_at, usage_count, last_usage, selected_for_phase2)
VALUES ('test', 'thread-2', 100, 'current raw', 'current summary', 110, 10, 200, true),
       ('test', 'thread-3', 100, 'old raw', 'old summary', 110, 10, 200, false);
CREATE TEMP TABLE import_clock AS SELECT floor(extract(epoch FROM CURRENT_TIMESTAMP))::bigint AS imported_at;
\ir ../migrations/0009_import_legacy_generated_memories.sql
DO $$
BEGIN
    ASSERT (SELECT count(*) FROM memory_stage1_outputs) = 5, 'invalid payload imported';
    ASSERT (SELECT raw_memory = 'newest original' AND source_updated_at = 100 AND generated_at = 111
        AND last_usage = (SELECT imported_at FROM import_clock) AND usage_count = 2
        FROM memory_stage1_outputs WHERE thread_id = 'thread-1'), 'latest legacy, original dates or one-time reactivation lost';
    ASSERT (SELECT raw_memory = 'current raw' AND usage_count = 10 AND last_usage = 200 AND selected_for_phase2
        FROM memory_stage1_outputs WHERE thread_id = 'thread-2'), 'newer canonical row overwritten';
    ASSERT (SELECT raw_memory = 'original raw' AND source_updated_at = 101 AND generated_at = 111 AND usage_count = 10
        AND last_usage = (SELECT imported_at FROM import_clock)
        FROM memory_stage1_outputs WHERE thread_id = 'thread-3'), 'newer legacy or monotone usage failed';
    ASSERT (SELECT selected_for_phase2 AND usage_count IS NULL AND last_usage = (SELECT imported_at FROM import_clock)
        FROM memory_stage1_outputs WHERE thread_id = 'thread-4'), 'nullable or boolean metadata lost';
    ASSERT (SELECT selected_for_phase2 FROM memory_stage1_outputs WHERE thread_id = 'thread-5'), 'SQLite integer boolean lost';
    ASSERT (SELECT count(*) FROM memories) = 17, 'legacy source rows modified';
END $$;
CREATE TEMP TABLE first_import AS TABLE memory_stage1_outputs;
\ir ../migrations/0009_import_legacy_generated_memories.sql
DO $$
BEGIN
    ASSERT NOT EXISTS ((TABLE first_import EXCEPT TABLE memory_stage1_outputs)
        UNION ALL (TABLE memory_stage1_outputs EXCEPT TABLE first_import)), 'import is not idempotent';
END $$;
ROLLBACK;
\echo 'PASS: legacy memory validity, scope, newest version, monotone usage, source dates, one-time reactivation and idempotence'
