//! Reports v2 readiness without exposing memory contents or persistence details to clients.

use super::*;
use codex_app_server_protocol::MemoryStatusParams;
use codex_app_server_protocol::MemoryStatusResponse;
use codex_protocol::MemoryVersion;

impl ThreadRequestProcessor {
    pub(crate) async fn memory_status(
        &self,
        params: MemoryStatusParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        let minimum = params.min_consolidated_threads.unwrap_or(20);
        if !(1..=4096).contains(&minimum) {
            return Err(invalid_params(
                "minConsolidatedThreads must be between 1 and 4096",
            ));
        }
        let store: crate::thread_state::GeneratedMemoryStoreHandle =
            if let Some(db) = self.state_db.as_ref() {
                Arc::new(
                    db.memories_for_version(MemoryVersion::V2)
                        .await
                        .map_err(|error| {
                            internal_error(format!("failed to open v2 memory state: {error}"))
                        })?,
                )
            } else {
                self.generated_memory_store
                    .as_ref()
                    .and_then(|store| store.for_version(MemoryVersion::V2))
                    .ok_or_else(|| {
                        internal_error("generated memory store unavailable for memory status")
                    })?
            };
        let count = store
            .max_consolidated_thread_count()
            .await
            .map_err(|error| internal_error(format!("failed to read memory progress: {error}")))?;
        let root = self
            .config
            .codex_home
            .join(MemoryVersion::V2.directory_name());
        let summary = tokio::fs::read_to_string(root.join("memory_summary.md"))
            .await
            .ok();
        let ready = count >= minimum
            && summary
                .as_deref()
                .is_some_and(codex_memories_write::workspace::is_valid_v2_summary);
        Ok(Some(
            MemoryStatusResponse {
                v2_consolidated_threads: count,
                v2_ready: ready,
            }
            .into(),
        ))
    }
}
