DROP INDEX IF EXISTS idx_threads_visible_created_at_ms;

CREATE INDEX idx_threads_visible_created_at_ms
    ON threads(archived, created_at_ms DESC, id DESC)
    WHERE preview <> '';

DROP INDEX IF EXISTS idx_threads_visible_updated_at_ms;

CREATE INDEX idx_threads_visible_updated_at_ms
    ON threads(archived, updated_at_ms DESC, id DESC)
    WHERE preview <> '';
