use std::any::Any;
use std::path::PathBuf;
use std::sync::Mutex;

use chrono::Utc;
use codex_protocol::SessionId;
use codex_protocol::ThreadId;
use codex_protocol::models::BaseInstructions;
use codex_protocol::models::ContentItem;
use codex_protocol::models::PermissionProfile;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_rollout::RolloutItem;
use pretty_assertions::assert_eq;

use crate::AppendThreadItemsParams;
use crate::ArchiveThreadParams;
use crate::CreateThreadParams;
use crate::DeleteThreadParams;
use crate::ExtraConfig;
use crate::ItemPage;
use crate::ListItemsParams;
use crate::ListThreadsParams;
use crate::ListTurnsParams;
use crate::LoadThreadHistoryParams;
use crate::PersistContext;
use crate::ReadThreadByRolloutPathParams;
use crate::ReadThreadParams;
use crate::ResumeThreadParams;
use crate::StoredThread;
use crate::StoredThreadHistory;
use crate::ThreadPage;
use crate::ThreadStore;
use crate::ThreadStoreError;
use crate::ThreadStoreFuture;
use crate::TurnPage;
use crate::UpdateThreadMetadataParams;

use super::LiveThread;

#[derive(Default)]
struct RecordingThreadStore {
    append_batches: Mutex<Vec<AppendThreadItemsParams>>,
    metadata_updates: Mutex<Vec<UpdateThreadMetadataParams>>,
}

impl RecordingThreadStore {
    fn appended_items(&self) -> Vec<Vec<RolloutItem>> {
        self.append_batches
            .lock()
            .expect("append batches lock")
            .iter()
            .map(|batch| batch.items.clone())
            .collect()
    }

    fn metadata_update_count(&self) -> usize {
        self.metadata_updates
            .lock()
            .expect("metadata updates lock")
            .len()
    }
}

impl ThreadStore for RecordingThreadStore {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create_thread(&self, _params: CreateThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn resume_thread(&self, _params: ResumeThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn append_items(&self, params: AppendThreadItemsParams) -> ThreadStoreFuture<'_, ()> {
        self.append_batches
            .lock()
            .expect("append batches lock")
            .push(params);
        Box::pin(async { Ok(()) })
    }

    fn persist_thread(
        &self,
        _thread_id: ThreadId,
        _context: PersistContext,
    ) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn flush_thread(&self, _thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn shutdown_thread(&self, _thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn discard_thread(&self, _thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn load_history(
        &self,
        _params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredThreadHistory> {
        Box::pin(async {
            Err(ThreadStoreError::Unsupported {
                operation: "load_history",
            })
        })
    }

    fn read_thread(&self, _params: ReadThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(async {
            Err(ThreadStoreError::Unsupported {
                operation: "read_thread",
            })
        })
    }

    fn read_thread_by_rollout_path(
        &self,
        _params: ReadThreadByRolloutPathParams,
    ) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(async {
            Err(ThreadStoreError::Unsupported {
                operation: "read_thread_by_rollout_path",
            })
        })
    }

    fn list_threads(&self, _params: ListThreadsParams) -> ThreadStoreFuture<'_, ThreadPage> {
        Box::pin(async {
            Err(ThreadStoreError::Unsupported {
                operation: "list_threads",
            })
        })
    }

    fn list_turns(&self, _params: ListTurnsParams) -> ThreadStoreFuture<'_, TurnPage> {
        Box::pin(async {
            Err(ThreadStoreError::Unsupported {
                operation: "list_turns",
            })
        })
    }

    fn list_items(&self, _params: ListItemsParams) -> ThreadStoreFuture<'_, ItemPage> {
        Box::pin(async {
            Err(ThreadStoreError::Unsupported {
                operation: "list_items",
            })
        })
    }

    fn update_thread_metadata(
        &self,
        params: UpdateThreadMetadataParams,
    ) -> ThreadStoreFuture<'_, Option<StoredThread>> {
        let thread_id = params.thread_id;
        self.metadata_updates
            .lock()
            .expect("metadata updates lock")
            .push(params);
        Box::pin(async move { Ok(Some(sample_stored_thread(thread_id))) })
    }

    fn archive_thread(&self, _params: ArchiveThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async {
            Err(ThreadStoreError::Unsupported {
                operation: "archive_thread",
            })
        })
    }

    fn unarchive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(async move { Ok(sample_stored_thread(params.thread_id)) })
    }

    fn delete_thread(&self, _params: DeleteThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async {
            Err(ThreadStoreError::Unsupported {
                operation: "delete_thread",
            })
        })
    }
}

#[tokio::test]
async fn live_thread_appends_only_persisted_rollout_items() {
    let store = std::sync::Arc::new(RecordingThreadStore::default());
    let thread_id = ThreadId::default();
    let live_thread = LiveThread::create(store.clone(), create_thread_params(thread_id))
        .await
        .expect("create live thread");
    let items = vec![non_persisted_item(), persisted_message("keep me")];

    live_thread
        .append_items(items.as_slice())
        .await
        .expect("append items");

    let appended_items = store.appended_items();
    assert_eq!(appended_items.len(), 1);
    let batch = &appended_items[0];
    assert_eq!(batch.len(), 1);
    let RolloutItem::ResponseItem(response_item) = &batch[0] else {
        panic!("expected persisted response item");
    };
    let ResponseItem::Message { content, .. } = &response_item.item else {
        panic!("expected persisted response item");
    };
    assert!(content.iter().any(|item| matches!(
        item,
        ContentItem::InputText { text } if text == "keep me"
    )));
    assert_eq!(store.metadata_update_count(), 1);
}

fn create_thread_params(thread_id: ThreadId) -> CreateThreadParams {
    CreateThreadParams {
        session_id: SessionId::new(),
        thread_id,
        extra_config: Some(ExtraConfig {}),
        forked_from_id: None,
        parent_thread_id: None,
        source: SessionSource::Cli,
        thread_source: None,
        originator: "test-originator".to_string(),
        base_instructions: BaseInstructions::default(),
        dynamic_tools: Vec::new(),
        selected_capability_roots: Vec::new(),
        multi_agent_version: None,
        history_mode: ThreadHistoryMode::Legacy,
        history_base: None,
        subagent_history_start_ordinal: None,
        initial_window_id: "test-window".to_string(),
        metadata: crate::ThreadPersistenceMetadata {
            cwd: Some(PathBuf::from("/workspace/repo")),
            model_provider: "openai".to_string(),
            model: None,
            reasoning_effort: None,
            memory_mode: ThreadMemoryMode::Enabled,
        },
    }
}

fn persisted_message(text: &str) -> RolloutItem {
    RolloutItem::ResponseItem(
        ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: text.to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        }
        .into(),
    )
}

fn non_persisted_item() -> RolloutItem {
    RolloutItem::ResponseItem(ResponseItem::Other.into())
}

fn sample_stored_thread(thread_id: ThreadId) -> StoredThread {
    let now = Utc::now();
    StoredThread {
        thread_id,
        extra_config: None,
        rollout_path: None,
        forked_from_id: None,
        parent_thread_id: None,
        preview: String::new(),
        name: None,
        model_provider: "openai".to_string(),
        model: None,
        reasoning_effort: None,
        created_at: now,
        updated_at: now,
        recency_at: now,
        archived_at: None,
        section: None,
        section_position: None,
        section_entered_at: None,
        project_id: None,
        cwd: PathBuf::from("/workspace/repo"),
        cli_version: "test".to_string(),
        source: SessionSource::Cli,
        history_mode: ThreadHistoryMode::Legacy,
        thread_source: None,
        agent_nickname: None,
        agent_role: None,
        agent_path: None,
        git_info: None,
        approval_mode: AskForApproval::OnRequest,
        permission_profile: PermissionProfile::read_only(),
        token_usage: None,
        first_user_message: None,
        history: None,
    }
}
