use super::*;

#[tokio::test]
async fn sqlite_deadline_and_cancellation_stop_workers_before_unlock() {
    for cancel in [false, true] {
        let home = tempfile::TempDir::new().expect("home");
        let config = super::super::test_support::test_config(home.path());
        let path = config.sqlite.thread_history_db_path();
        let writable = config
            .sqlite
            .open_read_write_pool(&path)
            .await
            .expect("database");
        writable.close().await;
        let mut budget = SearchBudget::new();
        budget.deadline = Instant::now() + Duration::from_millis(500);
        let lock = acquire_slot(home.path()).expect("first search slot");
        assert!(matches!(
            acquire_slot(home.path()),
            Err(ThreadStoreError::Conflict { .. })
        ));
        let pool = budget
            .open_pool(&config.sqlite, &path)
            .await
            .expect("read pool");
        let worker_budget = budget.clone();
        let worker = tokio::spawn(async move {
            let _lock = lock;
            let result = sqlx::query_scalar::<_, i64>(
                "WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x + 1 FROM n WHERE x < 100000000) SELECT sum(x) FROM n",
            ).fetch_one(&pool).await;
            assert!(result.is_err());
            assert!(worker_budget.check().is_err());
            pool.close().await;
        });
        if cancel {
            drop(CancelOnDrop(budget));
        }
        tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .expect("worker stopped")
            .expect("worker joined");
        acquire_slot(home.path()).expect("slot reusable after worker closes");
    }
}

#[test]
fn cumulative_text_budget_includes_metadata_and_all_items() {
    let budget = SearchBudget::new();
    budget.consume_bytes(1024).expect("metadata");
    budget.consume_bytes(MAX_BYTES - 1024).expect("exact limit");
    assert_eq!(budget.remaining_bytes(), 0);
    assert!(budget.consume_bytes(1).is_err());
}
