UPDATE threads
SET source = CASE
    WHEN jsonb_typeof(stored_thread_json -> 'source') = 'string'
        THEN stored_thread_json ->> 'source'
    ELSE stored_thread_json ->> 'source'
END
WHERE stored_thread_json ? 'source'
  AND source IS DISTINCT FROM CASE
    WHEN jsonb_typeof(stored_thread_json -> 'source') = 'string'
        THEN stored_thread_json ->> 'source'
    ELSE stored_thread_json ->> 'source'
END;

UPDATE threads
SET thread_source = CASE
    WHEN stored_thread_json ? 'thread_source'
        AND stored_thread_json -> 'thread_source' IS NOT NULL
        AND jsonb_typeof(stored_thread_json -> 'thread_source') != 'null'
        THEN stored_thread_json ->> 'thread_source'
    ELSE NULL
END
WHERE thread_source IS DISTINCT FROM CASE
    WHEN stored_thread_json ? 'thread_source'
        AND stored_thread_json -> 'thread_source' IS NOT NULL
        AND jsonb_typeof(stored_thread_json -> 'thread_source') != 'null'
        THEN stored_thread_json ->> 'thread_source'
    ELSE NULL
END;
