use super::thread_input::DIRECT_INPUT_TO_MULTI_AGENT_V2_SUBAGENT_ERROR;
use super::thread_input::can_accept_direct_input;
use super::thread_input::ensure_direct_input_allowed;
use super::*;
use codex_goal_extension::GoalObjectiveUpdate;
use codex_goal_extension::GoalService;
use codex_goal_extension::GoalServiceError;
use codex_goal_extension::GoalSetRequest;
use codex_goal_extension::GoalTokenBudgetUpdate;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadSettingsAppliedEvent;
use codex_protocol::protocol::ThreadSettingsSnapshot;
use codex_rollout::RolloutRecorder;

enum GoalAccess {
    Read,
    Mutate,
}

#[derive(Clone)]
pub(crate) struct ThreadGoalRequestProcessor {
    thread_manager: Arc<ThreadManager>,
    thread_store: Arc<dyn ThreadStore>,
    outgoing: Arc<OutgoingMessageSender>,
    config: Arc<Config>,
    thread_state_manager: ThreadStateManager,
    state_db: Option<StateDbHandle>,
    goal_store: Option<ThreadGoalStoreHandle>,
    goal_service: Arc<GoalService>,
}

struct GoalStorageContext {
    store: ThreadGoalStoreHandle,
    preview_state_db: Option<StateDbHandle>,
    reconcile_local_rollout: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LiveThreadGoalStoragePreference {
    Postgres,
    Local,
}

pub(crate) fn live_thread_goal_storage_preference(
    config: &Config,
    has_state_db: bool,
) -> Option<LiveThreadGoalStoragePreference> {
    if matches!(
        config.experimental_thread_store,
        codex_core::config::ThreadStoreConfig::Postgres { .. }
    ) {
        Some(LiveThreadGoalStoragePreference::Postgres)
    } else if has_state_db {
        Some(LiveThreadGoalStoragePreference::Local)
    } else {
        None
    }
}

impl ThreadGoalRequestProcessor {
    pub(crate) fn new(
        thread_manager: Arc<ThreadManager>,
        thread_store: Arc<dyn ThreadStore>,
        outgoing: Arc<OutgoingMessageSender>,
        config: Arc<Config>,
        thread_state_manager: ThreadStateManager,
        state_db: Option<StateDbHandle>,
        goal_store: Option<ThreadGoalStoreHandle>,
        goal_service: Arc<GoalService>,
    ) -> Self {
        Self {
            thread_manager,
            thread_store,
            outgoing,
            config,
            thread_state_manager,
            state_db,
            goal_store,
            goal_service,
        }
    }

    pub(crate) async fn thread_goal_set(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadGoalSetParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.thread_goal_set_inner(request_id, params)
            .await
            .map(|()| None)
    }

    pub(crate) async fn thread_goal_get(
        &self,
        params: ThreadGoalGetParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.thread_goal_get_inner(params)
            .await
            .map(|response| Some(response.into()))
    }

    pub(crate) async fn thread_goal_clear(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadGoalClearParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.thread_goal_clear_inner(request_id, params)
            .await
            .map(|()| None)
    }

    pub(crate) async fn emit_resume_goal_snapshot(&self, thread_id: ThreadId) {
        if !self.config.features.enabled(Feature::Goals) {
            return;
        }
        self.emit_thread_goal_snapshot(thread_id).await;
    }

    pub(crate) async fn pending_resume_goal_state(
        &self,
        thread: &CodexThread,
    ) -> (bool, Option<ThreadGoalStoreHandle>) {
        let emit_thread_goal_update = self.config.features.enabled(Feature::Goals);
        let thread_goal_store = if emit_thread_goal_update {
            self.goal_storage_for_live_thread(thread)
                .map(|storage| storage.store)
        } else {
            None
        };
        (emit_thread_goal_update, thread_goal_store)
    }

    pub(crate) async fn restore_inherited_goal_runtime(&self, thread_id: ThreadId) {
        if let Err(err) = self
            .goal_service
            .restore_thread_runtime_after_resume(thread_id)
            .await
        {
            warn!("failed to restore inherited goal runtime for {thread_id}: {err}");
        }
    }

    pub(crate) async fn flush_goal_progress_for_fork(
        &self,
        thread_id: ThreadId,
    ) -> Result<(), String> {
        self.goal_service
            .flush_thread_goal_progress_for_fork(thread_id)
            .await
            .map_err(|err| err.to_string())
    }

    pub(crate) fn goal_store_for_live_thread(
        &self,
        thread: &CodexThread,
    ) -> Option<ThreadGoalStoreHandle> {
        self.goal_storage_for_live_thread(thread)
            .map(|storage| storage.store)
    }

    async fn thread_goal_set_inner(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadGoalSetParams,
    ) -> Result<(), JSONRPCErrorError> {
        if !self.config.features.enabled(Feature::Goals) {
            return Err(invalid_request("goals feature is disabled"));
        }

        let thread_id = parse_thread_id_for_request(params.thread_id.as_str())?;
        let storage = self
            .goal_storage_for_materialized_thread(thread_id, GoalAccess::Mutate)
            .await?;
        if storage.reconcile_local_rollout {
            let state_db = storage.preview_state_db.as_ref().ok_or_else(|| {
                internal_error("sqlite state db unavailable for thread goal rollout reconcile")
            })?;
            self.reconcile_thread_goal_rollout(thread_id, state_db)
                .await?;
        }
        let max_goal_token_budget = match self.thread_manager.get_thread(thread_id).await {
            Ok(thread) => thread.config().await.max_goal_token_budget,
            Err(_) => self.config.max_goal_token_budget,
        };

        let listener_command_tx = {
            let thread_state = self.thread_state_manager.thread_state(thread_id).await;
            let thread_state = thread_state.lock().await;
            thread_state.listener_command_tx()
        };
        let status = params.status.map(ThreadGoalStatus::to_core);
        let objective = params.objective.as_deref();

        let outcome = self
            .goal_service
            .set_thread_goal(
                storage.store.as_ref(),
                storage.preview_state_db.as_deref(),
                GoalSetRequest {
                    thread_id,
                    objective: objective
                        .map(GoalObjectiveUpdate::Set)
                        .unwrap_or(GoalObjectiveUpdate::Keep),
                    status,
                    token_budget: match params.token_budget {
                        Some(token_budget) => GoalTokenBudgetUpdate::Set(token_budget),
                        None => GoalTokenBudgetUpdate::Keep,
                    },
                    max_goal_token_budget,
                },
            )
            .await
            .map_err(goal_service_error)?;
        let goal = ThreadGoal::from(outcome.goal.clone());

        let persist_result: Result<(), String> =
            match self.thread_manager.get_thread(thread_id).await {
                Ok(thread) => match thread.rollout_path() {
                    Some(path) if codex_rollout::existing_rollout_path(&path).await.is_none() => {
                        // Goal-first threads need their settings captured when the goal creates the
                        // rollout. Once materialized, normal settings updates own this event.
                        let persisted_settings = thread.thread_settings_snapshot().await;
                        let items = [
                            thread_settings_applied_item(persisted_settings.clone()),
                            outcome.thread_goal_updated_item(),
                        ];
                        match thread.append_rollout_items(&items).await {
                            Err(err) => Err(err),
                            Ok(()) => {
                                // Catch up a settings update queued while the rollout materialized.
                                let current_settings = thread.thread_settings_snapshot().await;
                                if current_settings == persisted_settings {
                                    Ok(())
                                } else {
                                    thread
                                        .append_rollout_items(&[thread_settings_applied_item(
                                            current_settings,
                                        )])
                                        .await
                                }
                            }
                        }
                    }
                    Some(_) | None => {
                        thread
                            .append_rollout_items(&[outcome.thread_goal_updated_item()])
                            .await
                    }
                }
                .map_err(|err| err.to_string()),
                Err(_) if !storage.reconcile_local_rollout => self
                    .thread_store
                    .append_items(StoreAppendThreadItemsParams {
                        thread_id,
                        items: vec![outcome.thread_goal_updated_item()],
                    })
                    .await
                    .map_err(|err| err.to_string()),
                Err(_) => Ok(()),
            };
        if let Err(err) = persist_result {
            warn!("failed to persist goal update for live thread {thread_id}: {err}");
        }

        self.outgoing
            .send_response(
                request_id.clone(),
                ThreadGoalSetResponse { goal: goal.clone() },
            )
            .await;
        self.emit_thread_goal_updated_ordered(thread_id, goal, listener_command_tx)
            .await;
        outcome.apply_runtime_effects(&self.goal_service).await;
        Ok(())
    }

    async fn thread_goal_get_inner(
        &self,
        params: ThreadGoalGetParams,
    ) -> Result<ThreadGoalGetResponse, JSONRPCErrorError> {
        if !self.config.features.enabled(Feature::Goals) {
            return Err(invalid_request("goals feature is disabled"));
        }

        let thread_id = parse_thread_id_for_request(params.thread_id.as_str())?;
        let storage = self
            .goal_storage_for_materialized_thread(thread_id, GoalAccess::Read)
            .await?;
        let goal = self
            .goal_service
            .get_thread_goal(storage.store.as_ref(), thread_id)
            .await
            .map_err(goal_service_error)?
            .map(ThreadGoal::from);
        Ok(ThreadGoalGetResponse { goal })
    }

    async fn thread_goal_clear_inner(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadGoalClearParams,
    ) -> Result<(), JSONRPCErrorError> {
        if !self.config.features.enabled(Feature::Goals) {
            return Err(invalid_request("goals feature is disabled"));
        }

        let thread_id = parse_thread_id_for_request(params.thread_id.as_str())?;
        let storage = self
            .goal_storage_for_materialized_thread(thread_id, GoalAccess::Mutate)
            .await?;
        if storage.reconcile_local_rollout {
            let state_db = storage.preview_state_db.as_ref().ok_or_else(|| {
                internal_error("sqlite state db unavailable for thread goal rollout reconcile")
            })?;
            self.reconcile_thread_goal_rollout(thread_id, state_db)
                .await?;
        }

        let listener_command_tx = {
            let thread_state = self.thread_state_manager.thread_state(thread_id).await;
            let thread_state = thread_state.lock().await;
            thread_state.listener_command_tx()
        };
        let cleared = self
            .goal_service
            .clear_thread_goal(storage.store.as_ref(), thread_id)
            .await
            .map_err(goal_service_error)?;

        self.outgoing
            .send_response(request_id, ThreadGoalClearResponse { cleared })
            .await;
        if cleared {
            self.emit_thread_goal_cleared_ordered(thread_id, listener_command_tx)
                .await;
        }
        Ok(())
    }

    async fn goal_storage_for_materialized_thread(
        &self,
        thread_id: ThreadId,
        access: GoalAccess,
    ) -> Result<GoalStorageContext, JSONRPCErrorError> {
        if let Ok(thread) = self.thread_manager.get_thread(thread_id).await {
            if matches!(access, GoalAccess::Mutate) {
                ensure_direct_input_allowed(thread.as_ref()).await?;
            }
            if !matches!(
                self.config.experimental_thread_store,
                codex_core::config::ThreadStoreConfig::Postgres { .. }
            ) && thread.rollout_path().is_none()
            {
                return Err(invalid_request(format!(
                    "ephemeral thread does not support goals: {thread_id}"
                )));
            }
            if let Some(storage) = self.goal_storage_for_live_thread(thread.as_ref()) {
                return Ok(storage);
            }
        } else if matches!(
            self.config.experimental_thread_store,
            codex_core::config::ThreadStoreConfig::Postgres { .. }
        ) {
            let stored_thread = self
                .thread_store
                .read_thread(StoreReadThreadParams {
                    thread_id,
                    include_archived: true,
                    include_history: matches!(access, GoalAccess::Mutate),
                })
                .await
                .map_err(thread_store_goal_error)?;
            if matches!(access, GoalAccess::Mutate)
                && matches!(
                    stored_thread.source,
                    SessionSource::SubAgent(SubAgentSource::ThreadSpawn { .. })
                )
            {
                let source = stored_thread.source.clone();
                let history = InitialHistory::Resumed(ResumedHistory {
                    conversation_id: thread_id,
                    history: Arc::new(
                        stored_thread
                            .history
                            .map(|history| history.items)
                            .unwrap_or_default(),
                    ),
                    rollout_path: stored_thread.rollout_path,
                });
                if !can_accept_direct_input(history.get_multi_agent_version(), &source) {
                    return Err(invalid_request(
                        DIRECT_INPUT_TO_MULTI_AGENT_V2_SUBAGENT_ERROR,
                    ));
                }
            }
        } else {
            let rollout_path = codex_rollout::find_thread_path_by_id_str(
                &self.config.codex_home,
                &thread_id.to_string(),
                self.state_db.as_deref(),
            )
            .await
            .map_err(|err| {
                internal_error(format!("failed to locate thread id {thread_id}: {err}"))
            })?
            .ok_or_else(|| invalid_request(format!("thread not found: {thread_id}")))?;
            if matches!(access, GoalAccess::Mutate) {
                let session_meta = codex_rollout::read_session_meta_line(&rollout_path)
                    .await
                    .map_err(|err| {
                        internal_error(format!("failed to read thread ownership: {err}"))
                    })?;
                if session_meta.meta.id != thread_id {
                    return Err(invalid_request(
                        "thread metadata does not match requested id",
                    ));
                }
                if matches!(
                    session_meta.meta.source,
                    SessionSource::SubAgent(SubAgentSource::ThreadSpawn { .. })
                ) {
                    // Match resume's latest version metadata, including legacy TurnContext
                    // fallback, rather than trusting only the initial session header.
                    let history = RolloutRecorder::get_rollout_history(&rollout_path)
                        .await
                        .map_err(|err| {
                            internal_error(format!("failed to read thread ownership: {err}"))
                        })?;
                    if !can_accept_direct_input(
                        history.get_multi_agent_version(),
                        &session_meta.meta.source,
                    ) {
                        return Err(invalid_request(
                            DIRECT_INPUT_TO_MULTI_AGENT_V2_SUBAGENT_ERROR,
                        ));
                    }
                }
            }
        }

        if matches!(
            self.config.experimental_thread_store,
            codex_core::config::ThreadStoreConfig::Postgres { .. }
        ) {
            return self.postgres_goal_storage(thread_id);
        }

        let state_db = self
            .state_db
            .clone()
            .ok_or_else(|| internal_error("sqlite state db unavailable for thread goals"))?;
        Ok(self.local_goal_storage(state_db))
    }

    fn goal_storage_for_live_thread(&self, thread: &CodexThread) -> Option<GoalStorageContext> {
        let state_db = thread.state_db();
        match live_thread_goal_storage_preference(&self.config, state_db.is_some()) {
            Some(LiveThreadGoalStoragePreference::Postgres) => self
                .postgres_goal_storage(thread.session_configured().thread_id)
                .ok(),
            Some(LiveThreadGoalStoragePreference::Local) => {
                state_db.map(|state_db| self.local_goal_storage(state_db))
            }
            None => None,
        }
    }

    fn local_goal_storage(&self, state_db: StateDbHandle) -> GoalStorageContext {
        let store: ThreadGoalStoreHandle = Arc::new(state_db.thread_goals().clone());
        GoalStorageContext {
            store,
            preview_state_db: Some(state_db),
            reconcile_local_rollout: true,
        }
    }

    fn postgres_goal_storage(
        &self,
        thread_id: ThreadId,
    ) -> Result<GoalStorageContext, JSONRPCErrorError> {
        let store = self.goal_store.clone().ok_or_else(|| {
            internal_error(format!(
                "postgres goal store unavailable for thread goals: {thread_id}"
            ))
        })?;
        Ok(GoalStorageContext {
            store,
            preview_state_db: None,
            reconcile_local_rollout: false,
        })
    }

    async fn reconcile_thread_goal_rollout(
        &self,
        thread_id: ThreadId,
        state_db: &StateDbHandle,
    ) -> Result<(), JSONRPCErrorError> {
        let running_thread = self.thread_manager.get_thread(thread_id).await.ok();
        let rollout_path = match running_thread.as_ref() {
            Some(thread) => thread.rollout_path().ok_or_else(|| {
                invalid_request(format!(
                    "ephemeral thread does not support goals: {thread_id}"
                ))
            })?,
            None => codex_rollout::find_thread_path_by_id_str(
                &self.config.codex_home,
                &thread_id.to_string(),
                self.state_db.as_deref(),
            )
            .await
            .map_err(|err| {
                internal_error(format!("failed to locate thread id {thread_id}: {err}"))
            })?
            .ok_or_else(|| invalid_request(format!("thread not found: {thread_id}")))?,
        };

        if let Ok(Some(metadata)) = state_db.get_thread(thread_id).await
            && codex_rollout::plain_rollout_path(metadata.rollout_path.as_path())
                == codex_rollout::plain_rollout_path(rollout_path.as_path())
            && let Some(existing_path) =
                codex_rollout::existing_rollout_path(metadata.rollout_path.as_path()).await
            && codex_rollout::read_session_meta_line(existing_path.as_path())
                .await
                .is_ok_and(|session_meta| session_meta.meta.id == thread_id)
        {
            return Ok(());
        }

        reconcile_rollout(
            Some(state_db),
            rollout_path.as_path(),
            self.config.model_provider_id.as_str(),
            /*builder*/ None,
            &[],
            /*archived_only*/ None,
            /*new_thread_memory_mode*/ None,
        )
        .await;
        Ok(())
    }

    pub(crate) async fn emit_thread_goal_snapshot(&self, thread_id: ThreadId) {
        let storage = match self
            .goal_storage_for_materialized_thread(thread_id, GoalAccess::Read)
            .await
        {
            Ok(storage) => storage,
            Err(err) => {
                warn!(
                    "failed to open goal store before emitting thread goal resume snapshot for {thread_id}: {}",
                    err.message
                );
                return;
            }
        };
        let listener_command_tx = {
            let thread_state = self.thread_state_manager.thread_state(thread_id).await;
            let thread_state = thread_state.lock().await;
            thread_state.listener_command_tx()
        };
        if let Some(listener_command_tx) = listener_command_tx {
            let command = crate::thread_state::ThreadListenerCommand::EmitThreadGoalSnapshot {
                goal_store: storage.store.clone(),
            };
            if listener_command_tx.send(command).is_ok() {
                return;
            }
            warn!(
                "failed to enqueue thread goal snapshot for {thread_id}: listener command channel is closed"
            );
        }
        send_thread_goal_snapshot_notification(&self.outgoing, thread_id, storage.store.as_ref())
            .await;
    }

    async fn emit_thread_goal_updated_ordered(
        &self,
        thread_id: ThreadId,
        goal: ThreadGoal,
        listener_command_tx: Option<tokio::sync::mpsc::UnboundedSender<ThreadListenerCommand>>,
    ) {
        if let Some(listener_command_tx) = listener_command_tx {
            let command = crate::thread_state::ThreadListenerCommand::EmitThreadGoalUpdated {
                turn_id: None,
                goal: goal.clone(),
            };
            if listener_command_tx.send(command).is_ok() {
                return;
            }
            warn!(
                "failed to enqueue thread goal update for {thread_id}: listener command channel is closed"
            );
        }
        self.outgoing
            .send_server_notification(ServerNotification::ThreadGoalUpdated(
                ThreadGoalUpdatedNotification {
                    thread_id: thread_id.to_string(),
                    turn_id: None,
                    goal,
                },
            ))
            .await;
    }

    async fn emit_thread_goal_cleared_ordered(
        &self,
        thread_id: ThreadId,
        listener_command_tx: Option<tokio::sync::mpsc::UnboundedSender<ThreadListenerCommand>>,
    ) {
        if let Some(listener_command_tx) = listener_command_tx {
            let command = crate::thread_state::ThreadListenerCommand::EmitThreadGoalCleared;
            if listener_command_tx.send(command).is_ok() {
                return;
            }
            warn!(
                "failed to enqueue thread goal clear for {thread_id}: listener command channel is closed"
            );
        }
        self.outgoing
            .send_server_notification(ServerNotification::ThreadGoalCleared(
                ThreadGoalClearedNotification {
                    thread_id: thread_id.to_string(),
                },
            ))
            .await;
    }
}

fn thread_settings_applied_item(thread_settings: ThreadSettingsSnapshot) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(
        ThreadSettingsAppliedEvent { thread_settings },
    ))
}

pub(super) fn api_thread_goal_from_state(goal: codex_state::ThreadGoal) -> ThreadGoal {
    ThreadGoal {
        thread_id: goal.thread_id.to_string(),
        objective: goal.objective,
        status: api_thread_goal_status_from_state(goal.status),
        token_budget: goal.token_budget,
        tokens_used: goal.tokens_used,
        time_used_seconds: goal.time_used_seconds,
        created_at: goal.created_at.timestamp(),
        updated_at: goal.updated_at.timestamp(),
    }
}

fn api_thread_goal_status_from_state(status: codex_state::ThreadGoalStatus) -> ThreadGoalStatus {
    match status {
        codex_state::ThreadGoalStatus::Active => ThreadGoalStatus::Active,
        codex_state::ThreadGoalStatus::Paused => ThreadGoalStatus::Paused,
        codex_state::ThreadGoalStatus::Blocked => ThreadGoalStatus::Blocked,
        codex_state::ThreadGoalStatus::UsageLimited => ThreadGoalStatus::UsageLimited,
        codex_state::ThreadGoalStatus::BudgetLimited => ThreadGoalStatus::BudgetLimited,
        codex_state::ThreadGoalStatus::Complete => ThreadGoalStatus::Complete,
    }
}

fn goal_service_error(err: GoalServiceError) -> JSONRPCErrorError {
    match err {
        GoalServiceError::InvalidRequest(message) => invalid_request(message),
        GoalServiceError::Internal(message) => internal_error(message),
    }
}

fn thread_store_goal_error(err: ThreadStoreError) -> JSONRPCErrorError {
    match err {
        ThreadStoreError::ThreadNotFound { thread_id } => {
            invalid_request(format!("thread not found: {thread_id}"))
        }
        ThreadStoreError::InvalidRequest { message }
        | ThreadStoreError::Conflict { message }
        | ThreadStoreError::Internal { message } => internal_error(message),
        ThreadStoreError::Unsupported { operation } => internal_error(format!(
            "thread store does not support {operation} for goals"
        )),
    }
}

fn parse_thread_id_for_request(thread_id: &str) -> Result<ThreadId, JSONRPCErrorError> {
    ThreadId::from_string(thread_id)
        .map_err(|err| invalid_request(format!("invalid thread id: {err}")))
}
