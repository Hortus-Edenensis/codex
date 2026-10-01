mod live_thread_goal_storage_preference_tests {
    use super::super::thread_goal_processor::LiveThreadGoalStoragePreference;
    use super::super::thread_goal_processor::live_thread_goal_storage_preference;
    use codex_core::config::ConfigBuilder;
    use codex_core::config::ThreadStoreConfig;
    use pretty_assertions::assert_eq;
    use tempfile::TempDir;

    #[tokio::test]
    async fn postgres_mode_prefers_postgres_over_local_state_db() -> anyhow::Result<()> {
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

        assert_eq!(
            live_thread_goal_storage_preference(&config, /*has_state_db*/ true),
            Some(LiveThreadGoalStoragePreference::Postgres)
        );

        Ok(())
    }

    #[tokio::test]
    async fn local_mode_uses_local_state_db_when_available() -> anyhow::Result<()> {
        let codex_home = TempDir::new()?;
        let config = ConfigBuilder::default()
            .codex_home(codex_home.path().to_path_buf())
            .fallback_cwd(Some(codex_home.path().to_path_buf()))
            .build()
            .await?;

        assert_eq!(
            live_thread_goal_storage_preference(&config, /*has_state_db*/ true),
            Some(LiveThreadGoalStoragePreference::Local)
        );

        Ok(())
    }

    #[tokio::test]
    async fn local_mode_without_state_db_has_no_live_goal_store() -> anyhow::Result<()> {
        let codex_home = TempDir::new()?;
        let config = ConfigBuilder::default()
            .codex_home(codex_home.path().to_path_buf())
            .fallback_cwd(Some(codex_home.path().to_path_buf()))
            .build()
            .await?;

        assert_eq!(live_thread_goal_storage_preference(&config, false), None);

        Ok(())
    }
}
