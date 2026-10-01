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
        thread_store,
        goal_store,
        generated_memory_store,
        postgres_store,
    } = thread_store_handles_from_config(&config, /*state_db*/ None);
    let postgres_store =
        postgres_store.expect("postgres config should construct a shared postgres store");
    let agent_graph_store = Some(Arc::clone(&postgres_store) as _)
        .or_else(|| codex_core::agent_graph_store_from_config(&config, /*state_db*/ None));

    let goal_store = goal_store.expect("postgres config should construct a goal store");
    let generated_memory_store =
        generated_memory_store.expect("postgres config should construct a generated memory store");
    let agent_graph_store =
        agent_graph_store.expect("postgres config should construct an agent graph store");
    let postgres_ptr = Arc::as_ptr(&postgres_store).cast::<()>();
    assert_eq!(Arc::as_ptr(&thread_store).cast::<()>(), postgres_ptr);
    assert_eq!(Arc::as_ptr(&goal_store).cast::<()>(), postgres_ptr);
    assert_eq!(
        Arc::as_ptr(&generated_memory_store).cast::<()>(),
        postgres_ptr
    );
    assert_eq!(Arc::as_ptr(&agent_graph_store).cast::<()>(), postgres_ptr);
    assert_eq!(Arc::strong_count(&postgres_store), 5);

    Ok(())
}
