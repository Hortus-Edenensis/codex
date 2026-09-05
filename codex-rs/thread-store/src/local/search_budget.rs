use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use crate::ThreadStoreError;
use crate::ThreadStoreResult;

pub(super) const MAX_CANDIDATES: usize = 10_000;
const MAX_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub(super) struct SearchBudget {
    deadline: Instant,
    cancelled: Arc<AtomicBool>,
    bytes: Arc<AtomicUsize>,
}

impl SearchBudget {
    pub(super) fn new() -> Self {
        Self {
            deadline: Instant::now() + Duration::from_secs(5),
            cancelled: Arc::new(AtomicBool::new(false)),
            bytes: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(super) fn check(&self) -> ThreadStoreResult<()> {
        if self.cancelled.load(Ordering::Relaxed) || Instant::now() >= self.deadline {
            return Err(limit_error("5 second deadline or request cancellation"));
        }
        Ok(())
    }

    pub(super) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    pub(super) fn remaining_bytes(&self) -> usize {
        MAX_BYTES.saturating_sub(self.bytes.load(Ordering::Relaxed))
    }

    pub(super) fn consume_bytes(&self, bytes: usize) -> ThreadStoreResult<()> {
        self.check()?;
        let previous = self.bytes.fetch_add(bytes, Ordering::Relaxed);
        if previous.saturating_add(bytes) > MAX_BYTES {
            return Err(limit_error("16 MiB of text"));
        }
        Ok(())
    }

    pub(super) fn query_error(&self, error: sqlx::Error) -> ThreadStoreError {
        self.check()
            .err()
            .unwrap_or_else(|| ThreadStoreError::Internal {
                message: format!("thread/searchOccurrences database query failed: {error}"),
            })
    }

    pub(super) async fn open_pool(
        &self,
        sqlite: &codex_state::SqliteConfig,
        path: &Path,
    ) -> ThreadStoreResult<sqlx::SqlitePool> {
        self.check()?;
        let pool = sqlite
            .open_read_only_pool(
                path,
                Some(self.deadline.saturating_duration_since(Instant::now())),
            )
            .await
            .map_err(|err| self.query_error(err))?;
        let result = async {
            let mut connection = pool.acquire().await?;
            let budget = self.clone();
            connection
                .lock_handle()
                .await?
                .set_progress_handler(/*num_ops*/ 1_000, move || budget.check().is_ok());
            Ok::<_, sqlx::Error>(())
        }
        .await;
        if let Err(err) = result {
            let error = self.query_error(err);
            self.cancel();
            pool.close().await;
            return Err(error);
        }
        Ok(pool)
    }
}

pub(super) struct CancelOnDrop(pub(super) SearchBudget);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub(super) fn acquire_slot(codex_home: &Path) -> ThreadStoreResult<File> {
    // Keep the inode in place: unlinking a locked file permits a second independent lock.
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(codex_home.join(".workspace-search.lock"))
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to open workspace body search lock: {err}"),
        })?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(ThreadStoreError::Conflict {
            message: "thread/searchOccurrences is busy in this workspace; retry later".to_string(),
        }),
        Err(std::fs::TryLockError::Error(err)) => Err(ThreadStoreError::Internal {
            message: format!("failed to lock workspace body search: {err}"),
        }),
    }
}

pub(super) fn limit_error(limit: &str) -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: format!(
            "thread/searchOccurrences exceeded {limit}; search coverage is incomplete"
        ),
    }
}

#[cfg(test)]
#[path = "search_budget_tests.rs"]
mod tests;
