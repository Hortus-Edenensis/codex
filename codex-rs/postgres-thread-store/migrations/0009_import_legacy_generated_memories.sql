-- Reactivate imported legacy records once without changing their source or generation dates.
WITH valid AS MATERIALIZED (
    SELECT memories.id, memories.workspace_id, memories.thread_id, memories.payload
    FROM memories
    JOIN threads ON threads.id = memories.thread_id
        AND threads.workspace_id = memories.workspace_id
    WHERE jsonb_typeof(memories.payload) = 'object'
      AND memories.payload ?& ARRAY[
          'thread_id', 'source_updated_at', 'raw_memory', 'rollout_summary', 'rollout_slug',
          'generated_at', 'usage_count', 'last_usage', 'selected_for_phase2',
          'selected_for_phase2_source_updated_at'
      ]
      AND jsonb_typeof(memories.payload->'thread_id') = 'string'
      AND memories.payload->>'thread_id' = memories.thread_id
      AND jsonb_typeof(memories.payload->'raw_memory') = 'string'
      AND btrim(memories.payload->>'raw_memory', E' \t\n\r') <> ''
      AND jsonb_typeof(memories.payload->'rollout_summary') = 'string'
      AND btrim(memories.payload->>'rollout_summary', E' \t\n\r') <> ''
      AND jsonb_typeof(memories.payload->'rollout_slug') IN ('string', 'null')
      AND memories.payload->'selected_for_phase2' IN ('0'::jsonb, '1'::jsonb, 'false'::jsonb, 'true'::jsonb)
      AND (
          SELECT bool_and(
              CASE
                  WHEN value = 'null'::jsonb THEN key IN (
                      'usage_count', 'last_usage', 'selected_for_phase2_source_updated_at'
                  )
                  WHEN jsonb_typeof(value) = 'number' AND value::text ~ '^[0-9]{1,19}$'
                      THEN value::text::numeric <= 9223372036854775807
                  ELSE FALSE
              END
          )
          FROM jsonb_each(CASE WHEN jsonb_typeof(memories.payload) = 'object'
              THEN memories.payload ELSE '{}'::jsonb END)
          WHERE key IN ('source_updated_at', 'generated_at', 'usage_count', 'last_usage',
              'selected_for_phase2_source_updated_at')
      )
), newest AS (
    SELECT DISTINCT ON (workspace_id, thread_id)
        workspace_id, thread_id,
        (payload->>'source_updated_at')::bigint AS source_updated_at,
        payload->>'raw_memory' AS raw_memory,
        payload->>'rollout_summary' AS rollout_summary,
        payload->>'rollout_slug' AS rollout_slug,
        (payload->>'generated_at')::bigint AS generated_at,
        (payload->>'usage_count')::bigint AS usage_count,
        GREATEST((payload->>'last_usage')::bigint, floor(extract(epoch FROM CURRENT_TIMESTAMP))::bigint) AS last_usage,
        payload->'selected_for_phase2' IN ('1'::jsonb, 'true'::jsonb) AS selected_for_phase2,
        (payload->>'selected_for_phase2_source_updated_at')::bigint AS selected_for_phase2_source_updated_at
    FROM valid
    ORDER BY workspace_id, thread_id,
        (payload->>'source_updated_at')::bigint DESC,
        (payload->>'generated_at')::bigint DESC, id DESC
)
INSERT INTO memory_stage1_outputs (
    workspace_id, thread_id, source_updated_at, raw_memory, rollout_summary, rollout_slug,
    generated_at, usage_count, last_usage, selected_for_phase2,
    selected_for_phase2_source_updated_at
)
SELECT * FROM newest
ON CONFLICT (workspace_id, thread_id) DO UPDATE SET
    source_updated_at = EXCLUDED.source_updated_at,
    raw_memory = EXCLUDED.raw_memory,
    rollout_summary = EXCLUDED.rollout_summary,
    rollout_slug = EXCLUDED.rollout_slug,
    generated_at = EXCLUDED.generated_at,
    usage_count = GREATEST(memory_stage1_outputs.usage_count, EXCLUDED.usage_count),
    last_usage = GREATEST(memory_stage1_outputs.last_usage, EXCLUDED.last_usage),
    selected_for_phase2 = EXCLUDED.selected_for_phase2,
    selected_for_phase2_source_updated_at = EXCLUDED.selected_for_phase2_source_updated_at
WHERE (EXCLUDED.source_updated_at, EXCLUDED.generated_at) >
    (memory_stage1_outputs.source_updated_at, memory_stage1_outputs.generated_at);
