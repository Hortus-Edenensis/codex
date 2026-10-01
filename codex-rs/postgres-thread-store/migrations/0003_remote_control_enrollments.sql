CREATE TABLE IF NOT EXISTS remote_control_enrollments (
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    websocket_url TEXT NOT NULL,
    account_id TEXT NOT NULL,
    app_server_client_name TEXT NOT NULL,
    server_id TEXT NOT NULL,
    environment_id TEXT NOT NULL,
    server_name TEXT NOT NULL,
    remote_control_enabled BOOLEAN,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, websocket_url, account_id, app_server_client_name)
);
