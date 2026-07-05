use super::thread_store_handles_from_config;
use codex_core::config::ConfigBuilder;
use codex_core::config::ThreadStoreConfig;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn postgres_thread_store_handles_share_one_store() -> anyhow::Result<()> {
    let codex_home = TempDir::new()?;
    let mut config = ConfigBuilder::default()
        .codex_home(codex_home.path().to_path_buf())
        .fallback_cwd(Some(codex_home.path().to_path_buf()))
        .build()
        .await?;
    config.experimental_thread_store = ThreadStoreConfig::Postgres {
        database_url_env: "CODEX_TEST_REMOTE_SQL_URL".to_string(),
        default_workspace_id: "codex-workspace".to_string(),
        redis_url_env: Some("CODEX_TEST_REDIS_URL".to_string()),
    };

    let super::ThreadStoreHandles {
        thread_store: _thread_store,
        goal_store,
        postgres_store,
        ..
    } = thread_store_handles_from_config(&config, /*state_db*/ None);
    let postgres_store =
        postgres_store.expect("postgres config should construct a shared postgres store");
    let agent_graph_store = Some(Arc::clone(&postgres_store) as _)
        .or_else(|| codex_core::agent_graph_store_from_config(&config, /*state_db*/ None));

    assert!(goal_store.is_some());
    assert!(agent_graph_store.is_some());
    assert_eq!(Arc::strong_count(&postgres_store), 4);

    Ok(())
}
