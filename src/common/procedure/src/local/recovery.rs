// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use common_telemetry::{error, info};

use crate::error;
use crate::local::{LocalManager, RootSubmissionOrigin};
use crate::procedure::InitProcedureState;
use crate::store::ProcedureMessage;
use crate::{ProcedureId, ProcedureState, ProcedureWithId};

/// A loaded procedure ready to resume from its checkpoint.
struct RecoveredProcedure {
    procedure: ProcedureWithId,
    state: ProcedureState,
    step: u32,
}

impl LocalManager {
    /// Loads all eligible procedures before starting any recovered runner.
    pub(crate) fn submit_recovered_messages(
        &self,
        messages: impl IntoIterator<Item = (ProcedureId, ProcedureMessage, InitProcedureState)>,
    ) {
        // Load both rollback and execution checkpoints before spawning. Loaders
        // rebuild trackers that parents consult before submitting new work.
        let procedures: Vec<_> = messages
            .into_iter()
            .filter_map(|(id, message, state)| self.load_recovered_procedure(id, message, state))
            .collect();

        for RecoveredProcedure {
            procedure,
            state,
            step,
        } in procedures
        {
            let procedure_id = procedure.id;
            // Detached children use the existing root lifecycle and cleanup.
            if let Err(e) = self.submit_root(
                procedure_id,
                state,
                step,
                procedure.procedure,
                procedure.context,
                RootSubmissionOrigin::Recovery,
            ) {
                error!(e; "Failed to recover procedure {}", procedure_id);
            }
        }
    }

    fn load_recovered_procedure(
        &self,
        procedure_id: ProcedureId,
        message: ProcedureMessage,
        init_state: InitProcedureState,
    ) -> Option<RecoveredProcedure> {
        if message.parent_id.is_some() && message.subprocedure_recovery.is_discard() {
            return None;
        }
        // The loader logs failures; skip them without adding a duplicate error.
        let mut loaded = self
            .manager_ctx
            .load_one_procedure_from_message(procedure_id, &message)?;
        match message.parent_id {
            Some(parent_id) => info!(
                "Recover detached subprocedure {}-{}, former parent: {}, step: {}",
                loaded.procedure.type_name(),
                procedure_id,
                parent_id,
                loaded.step
            ),
            None => info!(
                "Recover root procedure {}-{}, step: {}",
                loaded.procedure.type_name(),
                procedure_id,
                loaded.step
            ),
        }
        let state = match init_state {
            InitProcedureState::Running => ProcedureState::Running,
            InitProcedureState::RollingBack => ProcedureState::RollingBack {
                error: Arc::new(
                    error::RollbackProcedureRecoveredSnafu {
                        error: message.error.unwrap_or_else(|| "Unknown error".to_string()),
                    }
                    .build(),
                ),
            },
        };
        if let Err(e) = loaded.procedure.recover() {
            error!(e; "Failed to recover procedure {}", procedure_id);
        }
        Some(RecoveredProcedure {
            procedure: ProcedureWithId {
                id: procedure_id,
                procedure: loaded.procedure,
                context: message.context,
            },
            state,
            step: loaded.step,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use common_test_util::temp_dir::create_temp_dir;
    use tokio::sync::Notify;
    use tokio::time::timeout;

    use super::*;
    use crate::error::Result;
    use crate::local::{ManagerConfig, test_util};
    use crate::store::state_store::ObjectStateStore;
    use crate::test_util::InMemoryPoisonStore;
    use crate::{
        Context, LockKey, Procedure, ProcedureContext, ProcedureManager, Status,
        SubprocedureRecovery,
    };

    struct MockParentProcedure {
        child: Option<ProcedureWithId>,
        recovery: SubprocedureRecovery,
    }

    #[async_trait]
    impl Procedure for MockParentProcedure {
        fn type_name(&self) -> &str {
            "MockParentProcedure"
        }

        async fn execute(&mut self, _: &Context) -> Result<Status> {
            let Some(child) = self.child.take() else {
                return Ok(Status::done());
            };
            Ok(match self.recovery {
                SubprocedureRecovery::Discard => Status::suspended(vec![child], false),
                SubprocedureRecovery::Detach => Status::Suspended {
                    subprocedures: vec![child],
                    persist: false,
                    recovery: SubprocedureRecovery::Detach,
                },
            })
        }

        fn dump(&self) -> Result<String> {
            unreachable!("The fixture deliberately does not checkpoint the parent")
        }

        fn lock_key(&self) -> LockKey {
            LockKey::default()
        }
    }

    #[derive(Clone)]
    struct MockChildProcedure {
        checkpointed: bool,
        init_state: InitProcedureState,
        entered: Arc<Notify>,
        release: Arc<Notify>,
        recoveries: Arc<AtomicUsize>,
        rollbacks: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Procedure for MockChildProcedure {
        fn type_name(&self) -> &str {
            "MockChildProcedure"
        }

        async fn execute(&mut self, _: &Context) -> Result<Status> {
            if matches!(self.init_state, InitProcedureState::RollingBack) {
                return error::UnexpectedSnafu {
                    err_msg: "Child needs rollback",
                }
                .fail();
            }
            if !self.checkpointed {
                self.checkpointed = true;
                return Ok(Status::executing(true));
            }
            self.entered.notify_one();
            self.release.notified().await;
            Ok(Status::done())
        }

        fn recover(&mut self) -> Result<()> {
            self.recoveries.fetch_add(1, Ordering::Relaxed);
            // The next execution checkpoint must persist the detached root identity.
            self.checkpointed = false;
            Ok(())
        }

        fn dump(&self) -> Result<String> {
            Ok(self.checkpointed.to_string())
        }

        fn lock_key(&self) -> LockKey {
            LockKey::default()
        }

        fn rollback_supported(&self) -> bool {
            true
        }

        async fn rollback(&mut self, _: &Context) -> Result<()> {
            self.rollbacks.fetch_add(1, Ordering::Relaxed);
            self.entered.notify_one();
            self.release.notified().await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_detach_child_after_restart() {
        check_child_recovery(SubprocedureRecovery::Detach, InitProcedureState::Running).await;
    }

    #[tokio::test]
    async fn test_detach_rollback_child_after_restart() {
        check_child_recovery(
            SubprocedureRecovery::Detach,
            InitProcedureState::RollingBack,
        )
        .await;
    }

    #[tokio::test]
    async fn test_discard_child_after_restart() {
        check_child_recovery(SubprocedureRecovery::Discard, InitProcedureState::Running).await;
    }

    #[tokio::test]
    async fn test_discard_rollback_child_after_restart() {
        check_child_recovery(
            SubprocedureRecovery::Discard,
            InitProcedureState::RollingBack,
        )
        .await;
    }

    #[tokio::test]
    async fn test_load_all_procedures_before_spawning() {
        // Cover both checkpoint kinds and both cross-kind parent/child orderings.
        for (parent_rollback, child_rollback) in
            [(false, false), (false, true), (true, false), (true, true)]
        {
            let dir = create_temp_dir("load-before-spawn");
            let manager = LocalManager::new(
                ManagerConfig::default(),
                Arc::new(ObjectStateStore::new(test_util::new_object_store(&dir))),
                Arc::new(InMemoryPoisonStore::new()),
                None,
                None,
            );
            let manager_ctx = manager.manager_ctx.clone();
            let loads = Arc::new(AtomicUsize::new(0));
            let recoveries = Arc::new(AtomicUsize::new(0));
            let loaded_count = loads.clone();
            let recovered_count = recoveries.clone();
            manager
                .register_loader(
                    "MockChildProcedure",
                    Box::new(move |data| {
                        // A loader must never observe an already spawned parent.
                        // This check is deterministic even if its runner has not run yet.
                        assert!(manager_ctx.runner_tasks.lock().unwrap().is_empty());
                        loaded_count.fetch_add(1, Ordering::Relaxed);
                        Ok(Box::new(MockChildProcedure {
                            checkpointed: data.parse().unwrap(),
                            init_state: InitProcedureState::Running,
                            entered: Arc::new(Notify::new()),
                            release: Arc::new(Notify::new()),
                            recoveries: recovered_count.clone(),
                            rollbacks: Arc::new(AtomicUsize::new(0)),
                        }))
                    }),
                )
                .unwrap();
            let parent_id = ProcedureId::random();
            let child_id = ProcedureId::random();
            for (id, parent, rollback) in [
                (parent_id, None, parent_rollback),
                (child_id, Some(parent_id), child_rollback),
            ] {
                let message = ProcedureMessage {
                    type_name: "MockChildProcedure".to_string(),
                    data: "true".to_string(),
                    parent_id: parent,
                    subprocedure_recovery: SubprocedureRecovery::Detach,
                    step: 7,
                    error: rollback.then(|| "Needs rollback".to_string()),
                    context: ProcedureContext::default(),
                };
                if rollback {
                    manager
                        .procedure_store
                        .rollback_procedure(id, message)
                        .await
                        .unwrap();
                } else {
                    manager
                        .procedure_store
                        .store_procedure_message(id, &message)
                        .await
                        .unwrap();
                }
            }

            manager.start().await.unwrap();
            assert_eq!(loads.load(Ordering::Relaxed), 2);
            assert_eq!(recoveries.load(Ordering::Relaxed), 2);
            assert_eq!(manager.manager_ctx.runner_tasks.lock().unwrap().len(), 2);
            manager.stop().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_recover_roots_and_detached_children_over_limit() {
        let dir = create_temp_dir("detached-recovery-limit");
        let manager = LocalManager::new(
            ManagerConfig {
                max_running_procedures: 1,
                ..Default::default()
            },
            Arc::new(ObjectStateStore::new(test_util::new_object_store(&dir))),
            Arc::new(InMemoryPoisonStore::new()),
            None,
            None,
        );
        manager
            .register_loader(
                "MockChildProcedure",
                Box::new(|data| {
                    Ok(Box::new(MockChildProcedure {
                        checkpointed: data.parse().unwrap(),
                        init_state: InitProcedureState::Running,
                        entered: Arc::new(Notify::new()),
                        release: Arc::new(Notify::new()),
                        recoveries: Arc::new(AtomicUsize::new(0)),
                        rollbacks: Arc::new(AtomicUsize::new(0)),
                    }))
                }),
            )
            .unwrap();
        let root_id = ProcedureId::random();
        let child_ids = [ProcedureId::random(), ProcedureId::random()];
        let root = ProcedureMessage {
            type_name: "MockChildProcedure".to_string(),
            data: "true".to_string(),
            parent_id: None,
            subprocedure_recovery: SubprocedureRecovery::Discard,
            step: 7,
            error: None,
            context: ProcedureContext::default(),
        };
        manager
            .procedure_store
            .store_procedure_message(root_id, &root)
            .await
            .unwrap();
        for child_id in child_ids {
            let child = ProcedureMessage {
                parent_id: Some(root_id),
                subprocedure_recovery: SubprocedureRecovery::Detach,
                ..root.clone()
            };
            manager
                .procedure_store
                .store_procedure_message(child_id, &child)
                .await
                .unwrap();
        }

        manager.start().await.unwrap();
        {
            let procedures = manager.manager_ctx.procedures.read().unwrap();
            assert_eq!(procedures.len(), 3);
            for id in [root_id, child_ids[0], child_ids[1]] {
                let meta = &procedures[&id];
                assert_eq!(meta.parent_id, None);
                assert_eq!(meta.num_children(), 0);
            }
        }
        // Wait for detached children to checkpoint as roots, then verify that
        // recovery retained their nonzero step rather than restarting from zero.
        timeout(Duration::from_secs(5), async {
            loop {
                let stored = manager.procedure_store.load_messages().await.unwrap();
                if child_ids.iter().all(|id| {
                    stored
                        .messages
                        .get(id)
                        .is_some_and(|message| message.parent_id.is_none())
                }) {
                    for child_id in child_ids {
                        assert_eq!(stored.messages[&child_id].step, 7);
                    }
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        manager.stop().await.unwrap();
    }

    async fn check_child_recovery(policy: SubprocedureRecovery, init_state: InitProcedureState) {
        let dir = create_temp_dir("child-recovery");
        let state_store = Arc::new(ObjectStateStore::new(test_util::new_object_store(&dir)));
        let manager = LocalManager::new(
            ManagerConfig {
                max_running_procedures: 1,
                ..Default::default()
            },
            state_store,
            Arc::new(InMemoryPoisonStore::new()),
            None,
            None,
        );
        let child_id = ProcedureId::random();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let recoveries = Arc::new(AtomicUsize::new(0));
        let rollbacks = Arc::new(AtomicUsize::new(0));
        let child = MockChildProcedure {
            checkpointed: false,
            init_state: init_state.clone(),
            entered: entered.clone(),
            release: release.clone(),
            recoveries: recoveries.clone(),
            rollbacks: rollbacks.clone(),
        };
        let loaded_child = child.clone();
        manager
            .register_loader(
                "MockChildProcedure",
                Box::new(move |data| {
                    assert_eq!(
                        policy,
                        SubprocedureRecovery::Detach,
                        "Discarded children must not be loaded"
                    );
                    let mut child = loaded_child.clone();
                    child.checkpointed = data.parse().unwrap();
                    Ok(Box::new(child))
                }),
            )
            .unwrap();
        manager.start().await.unwrap();
        let parent = ProcedureWithId::with_random_id(Box::new(MockParentProcedure {
            child: Some(ProcedureWithId {
                id: child_id,
                procedure: Box::new(child),
                context: ProcedureContext::default(),
            }),
            recovery: policy,
        }))
        .with_context(ProcedureContext {
            actor: Some("alice".to_string()),
            ..Default::default()
        });
        let parent_id = parent.id;
        manager.submit(parent).await.unwrap();
        timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();

        // The first child checkpoint carries the submission policy, even though
        // Status::Suspended.persist was false and the parent has no checkpoint.
        let stored = manager.procedure_store.load_messages().await.unwrap();
        let checkpoint = match init_state {
            InitProcedureState::Running => &stored.messages[&child_id],
            InitProcedureState::RollingBack => &stored.rollback_messages[&child_id],
        }
        .clone();
        assert_eq!(checkpoint.parent_id, Some(parent_id));
        assert_eq!(checkpoint.subprocedure_recovery, policy);
        assert_eq!(checkpoint.context.actor.as_deref(), Some("alice"));
        assert_eq!(checkpoint.step, 0);
        if matches!(init_state, InitProcedureState::RollingBack) {
            assert!(!stored.messages.contains_key(&child_id));
            assert_eq!(checkpoint.data, "false");
        }
        assert!(!stored.messages.contains_key(&parent_id));
        manager.stop().await.unwrap();
        manager.start().await.unwrap();

        if policy == SubprocedureRecovery::Discard {
            assert!(manager.procedure_state(child_id).await.unwrap().is_none());
            assert_eq!(recoveries.load(Ordering::Relaxed), 0);
            let stored = manager.procedure_store.load_messages().await.unwrap();
            let retained = match init_state {
                InitProcedureState::Running => &stored.messages[&child_id],
                InitProcedureState::RollingBack => &stored.rollback_messages[&child_id],
            };
            assert_eq!(retained, &checkpoint);
            manager.stop().await.unwrap();
            return;
        }

        // The original parent is absent. The child owns its cleanup as a root,
        // and remains recoverable across a second restart.
        for restart in 1..=2 {
            timeout(Duration::from_secs(5), entered.notified())
                .await
                .unwrap();
            assert!(manager.procedure_state(parent_id).await.unwrap().is_none());
            let meta = manager.manager_ctx.procedures.read().unwrap()[&child_id].clone();
            assert_eq!(meta.parent_id, None);
            assert_eq!(meta.context, checkpoint.context);
            assert_eq!(recoveries.load(Ordering::Relaxed), restart);
            let stored = manager.procedure_store.load_messages().await.unwrap();
            match init_state {
                InitProcedureState::Running => {
                    let recovered = &stored.messages[&child_id];
                    assert_eq!(recovered.parent_id, None);
                    assert_eq!(recovered.step, checkpoint.step);
                    assert_eq!(recovered.context, checkpoint.context);
                }
                InitProcedureState::RollingBack => {
                    assert!(meta.state().is_rolling_back());
                    assert_eq!(stored.rollback_messages[&child_id], checkpoint);
                }
            }
            if restart == 1 {
                manager.stop().await.unwrap();
                manager.start().await.unwrap();
            }
        }
        release.notify_one();
        timeout(Duration::from_secs(5), async {
            while manager
                .manager_ctx
                .runner_tasks
                .lock()
                .unwrap()
                .contains_key(&child_id)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let stored = manager.procedure_store.load_messages().await.unwrap();
        assert!(stored.messages.is_empty());
        assert!(stored.rollback_messages.is_empty());
        assert!(stored.finished_ids.is_empty());
        let state = manager.procedure_state(child_id).await.unwrap().unwrap();
        match init_state {
            InitProcedureState::Running => {
                assert!(state.is_done());
                assert_eq!(rollbacks.load(Ordering::Relaxed), 0);
            }
            InitProcedureState::RollingBack => {
                assert!(state.is_failed());
                assert_eq!(rollbacks.load(Ordering::Relaxed), 3);
            }
        }
        manager.stop().await.unwrap();
    }
}
