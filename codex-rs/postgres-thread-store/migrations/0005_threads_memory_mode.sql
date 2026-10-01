ALTER TABLE threads
ADD COLUMN IF NOT EXISTS memory_mode TEXT NOT NULL DEFAULT 'enabled';

UPDATE threads
SET memory_mode = COALESCE(NULLIF(stored_thread_json->>'memory_mode', ''), memory_mode);

UPDATE threads
SET stored_thread_json = jsonb_set(
    stored_thread_json,
    '{memory_mode}',
    to_jsonb(memory_mode),
    true
)
WHERE jsonb_typeof(stored_thread_json) = 'object';
