use crate::model::Phase2JobClaimOutcome;
use crate::model::Stage1JobClaim;
use crate::model::Stage1Output;
use crate::model::Stage1StartupClaimParams;
use codex_protocol::ThreadId;
use std::future::Future;
use std::pin::Pin;

/// Future returned by [`GeneratedMemoryStore`] operations.
pub type GeneratedMemoryStoreFuture<'a, T> =
    Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send + 'a>>;

/// Storage-neutral persistence for generated-memory pipeline state.
///
/// Implementations own the durable stage-1 extraction outputs, stage-1 job
/// claims, phase-2 consolidation job coordination, and memory usage
/// bookkeeping. Callers rely on this trait instead of assuming a local SQLite
/// state DB exists.
pub trait GeneratedMemoryStore: Send + Sync {
    fn clear_memory_data(&self) -> GeneratedMemoryStoreFuture<'_, ()>;

    fn record_stage1_output_usage<'a>(
        &'a self,
        thread_ids: &'a [ThreadId],
    ) -> GeneratedMemoryStoreFuture<'a, usize>;

    fn claim_stage1_jobs_for_startup<'a>(
        &'a self,
        current_thread_id: ThreadId,
        params: Stage1StartupClaimParams<'a>,
    ) -> GeneratedMemoryStoreFuture<'a, Vec<Stage1JobClaim>>;

    fn prune_stage1_outputs_for_retention(
        &self,
        max_unused_days: i64,
        limit: usize,
    ) -> GeneratedMemoryStoreFuture<'_, usize>;

    fn get_phase2_input_selection(
        &self,
        n: usize,
        max_unused_days: i64,
    ) -> GeneratedMemoryStoreFuture<'_, Vec<Stage1Output>>;

    fn mark_thread_memory_mode_polluted(
        &self,
        thread_id: ThreadId,
    ) -> GeneratedMemoryStoreFuture<'_, bool>;

    fn mark_stage1_job_succeeded<'a>(
        &'a self,
        thread_id: ThreadId,
        ownership_token: &'a str,
        source_updated_at: i64,
        raw_memory: &'a str,
        rollout_summary: &'a str,
        rollout_slug: Option<&'a str>,
    ) -> GeneratedMemoryStoreFuture<'a, bool>;

    fn mark_stage1_job_succeeded_no_output<'a>(
        &'a self,
        thread_id: ThreadId,
        ownership_token: &'a str,
    ) -> GeneratedMemoryStoreFuture<'a, bool>;

    fn mark_stage1_job_failed<'a>(
        &'a self,
        thread_id: ThreadId,
        ownership_token: &'a str,
        failure_reason: &'a str,
        retry_delay_seconds: i64,
    ) -> GeneratedMemoryStoreFuture<'a, bool>;

    fn try_claim_global_phase2_job(
        &self,
        worker_id: ThreadId,
        lease_seconds: i64,
    ) -> GeneratedMemoryStoreFuture<'_, Phase2JobClaimOutcome>;

    fn heartbeat_global_phase2_job<'a>(
        &'a self,
        ownership_token: &'a str,
        lease_seconds: i64,
    ) -> GeneratedMemoryStoreFuture<'a, bool>;

    fn mark_global_phase2_job_succeeded<'a>(
        &'a self,
        ownership_token: &'a str,
        completed_watermark: i64,
        selected_outputs: &'a [Stage1Output],
    ) -> GeneratedMemoryStoreFuture<'a, bool>;

    fn mark_global_phase2_job_failed<'a>(
        &'a self,
        ownership_token: &'a str,
        failure_reason: &'a str,
        retry_delay_seconds: i64,
    ) -> GeneratedMemoryStoreFuture<'a, bool>;

    fn mark_global_phase2_job_failed_if_unowned<'a>(
        &'a self,
        ownership_token: &'a str,
        failure_reason: &'a str,
        retry_delay_seconds: i64,
    ) -> GeneratedMemoryStoreFuture<'a, bool>;
}
