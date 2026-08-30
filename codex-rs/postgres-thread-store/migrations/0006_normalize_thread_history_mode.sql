UPDATE threads
SET history_mode = CASE
    WHEN stored_thread_json ? 'history_mode'
        THEN stored_thread_json ->> 'history_mode'
    ELSE lower(history_mode)
END
WHERE history_mode IS DISTINCT FROM CASE
    WHEN stored_thread_json ? 'history_mode'
        THEN stored_thread_json ->> 'history_mode'
    ELSE lower(history_mode)
END;
