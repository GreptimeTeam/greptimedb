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

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common_telemetry::logging;
use tokio::sync::Notify;
use tokio::time;

use crate::error::Result;
use crate::local::{ExecMeta, ManagerContext, ProcedureMeta, ProcedureMetaRef};
use crate::store::ProcedureStore;
use crate::{BoxedProcedure, Context, ProcedureId, ProcedureState, ProcedureWithId, Status};

const ERR_WAIT_DURATION: Duration = Duration::from_secs(30);

// TODO(yingwen): Support cancellation.
pub(crate) struct Runner {
    pub(crate) meta: ProcedureMetaRef,
    pub(crate) procedure: BoxedProcedure,
    pub(crate) manager_ctx: Arc<ManagerContext>,
    pub(crate) step: u32,
    pub(crate) store: ProcedureStore,
}

impl Runner {
    /// Run the procedure.
    pub(crate) async fn run(mut self) -> Result<()> {
        logging::info!(
            "Runner {}-{} starts",
            self.procedure.type_name(),
            self.meta.id
        );
        // We use the lock key in ProcedureMeta as it considers locks inherited from
        // its parent.
        let lock_key = self.meta.lock_key.clone();

        // TODO(yingwen): Support multiple lock keys.
        // Acquire lock if necessary.
        if let Some(key) = &lock_key {
            self.manager_ctx
                .lock_map
                .acquire_lock(key.key(), self.meta.clone())
                .await;
        }

        let mut result = Ok(());
        // Execute the procedure. We need to release the lock whenever the the execution
        // is successful or fail.
        if let Err(e) = self.execute_procedure().await {
            logging::error!(
                e; "Failed to execute procedure {}-{}",
                self.procedure.type_name(),
                self.meta.id
            );
            result = Err(e);
        }

        if let Some(key) = &lock_key {
            self.manager_ctx
                .lock_map
                .release_lock(key.key(), self.meta.id);
        }

        // If this is the root procedure, clean up message cache.
        if self.meta.parent_id.is_none() {
            let procedure_ids = self.manager_ctx.procedures_in_tree(&self.meta);
            self.manager_ctx.remove_messages(&procedure_ids);
        }

        // We can't remove the metadata of the procedure now as users and its parent might
        // need to query its state.
        // TODO(yingwen): 1. Add TTL to the metadata; 2. Only keep state in the procedure store
        // so we don't need to always store the metadata in memory after the procedure is done.

        logging::info!(
            "Runner {}-{} exits",
            self.procedure.type_name(),
            self.meta.id
        );

        result
    }

    async fn execute_procedure(&mut self) -> Result<()> {
        let ctx = Context {
            procedure_id: self.meta.id,
        };

        loop {
            match self.procedure.execute(&ctx).await {
                Ok(status) => {
                    if status.need_persist() {
                        if self.persist_procedure().await.is_err() {
                            self.wait_on_err().await;
                            continue;
                        }
                    }

                    match status {
                        Status::Executing { .. } => (),
                        Status::Suspended { subprocedures, .. } => {
                            self.on_suspended(subprocedures).await;
                        }
                        Status::Done => {
                            if self.commit_procedure().await.is_err() {
                                self.wait_on_err().await;
                                continue;
                            }

                            self.done();
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    logging::error!(
                        e;
                        "Failed to execute procedure {}-{}",
                        self.procedure.type_name(),
                        self.meta.id
                    );

                    self.meta.set_state(ProcedureState::Failed);

                    // Write rollback key so we can skip this procedure while recovering procedures.
                    if self.rollback_procedure().await.is_err() {
                        self.wait_on_err().await;
                        continue;
                    }

                    return Err(e);
                }
            }
        }
    }

    /// Submit a subprocedure with specific `procedure_id`.
    fn submit_subprocedure(&self, procedure_id: ProcedureId, mut procedure: BoxedProcedure) {
        let mut step = 0;
        if let Some(loaded_procedure) = self.manager_ctx.load_one_procedure(procedure_id) {
            // Try to load procedure state from the message to avoid re-run the subprocedure
            // from initial state.
            assert_eq!(self.meta.id, loaded_procedure.parent_id.unwrap());

            // Use the dumped procedure from the procedure store.
            procedure = loaded_procedure.procedure;
            // Update step number.
            step = loaded_procedure.step;
        }

        if self.manager_ctx.contains_procedure(procedure_id) {
            // If the parent has already submitted this procedure, don't submit it again.
            return;
        }

        // Inherit locks from the parent procedure. This procedure can submit a subprocedure,
        // which indicates the procedure already owns the locks and is executing now.
        let parent_locks = self.meta.locks_needed();
        let mut child_lock = procedure.lock_key();
        if let Some(lock) = &child_lock {
            if parent_locks.contains(lock) {
                // If the parent procedure already holds this lock, we set this lock to None
                // so the subprocedure don't need to acquire lock again.
                child_lock = None;
            }
        }

        let meta = Arc::new(ProcedureMeta {
            id: procedure_id,
            lock_notify: Notify::new(),
            parent_id: Some(self.meta.id),
            child_notify: Notify::new(),
            parent_locks,
            lock_key: child_lock,
            exec_meta: Mutex::new(ExecMeta::default()),
        });
        let runner = Runner {
            meta: meta.clone(),
            procedure,
            manager_ctx: self.manager_ctx.clone(),
            step,
            store: self.store.clone(),
        };

        self.manager_ctx.insert_procedure(meta);
        // Add the id of the subprocedure to the metadata.
        self.meta.push_child(procedure_id);

        common_runtime::spawn_bg(async move {
            // Run the root procedure.
            runner.run().await
        });
    }

    async fn wait_on_err(&self) {
        time::sleep(ERR_WAIT_DURATION).await;
    }

    async fn on_suspended(&self, subprocedures: Vec<ProcedureWithId>) {
        for subprocedure in subprocedures {
            self.submit_subprocedure(subprocedure.id, subprocedure.procedure);
        }

        logging::info!(
            "Procedure {}-{} is waiting for subprocedures",
            self.procedure.type_name(),
            self.meta.id,
        );

        // Wait for subprocedures.
        self.meta.child_notify.notified().await;

        logging::info!(
            "Procedure {}-{} is waked up",
            self.procedure.type_name(),
            self.meta.id,
        );
    }

    async fn persist_procedure(&mut self) -> Result<()> {
        self.store
            .store_procedure(
                self.meta.id,
                self.step,
                &self.procedure,
                self.meta.parent_id,
            )
            .await
            .map_err(|e| {
                logging::error!(
                    e; "Failed to persist procedure {}-{}",
                    self.procedure.type_name(),
                    self.meta.id
                );
                e
            })?;
        self.step += 1;
        Ok(())
    }

    async fn commit_procedure(&mut self) -> Result<()> {
        self.store
            .commit_procedure(self.meta.id, self.step)
            .await
            .map_err(|e| {
                logging::error!(
                    e; "Failed to commit procedure {}-{}",
                    self.procedure.type_name(),
                    self.meta.id
                );
                e
            })?;
        self.step += 1;
        Ok(())
    }

    async fn rollback_procedure(&mut self) -> Result<()> {
        self.store
            .rollback_procedure(self.meta.id, self.step)
            .await
            .map_err(|e| {
                logging::error!(
                    e; "Failed to write rollback key for procedure {}-{}",
                    self.procedure.type_name(),
                    self.meta.id
                );
                e
            })?;
        self.step += 1;
        Ok(())
    }

    fn done(&self) {
        // TODO(yingwen): Add files to remove list.
        logging::info!(
            "Procedure {}-{} done",
            self.procedure.type_name(),
            self.meta.id
        );

        // Mark the state of this procedure to done.
        self.meta.set_state(ProcedureState::Done);

        // Notify parent procedure.
        if let Some(parent_id) = self.meta.parent_id {
            self.manager_ctx.notify_by_subprocedure(parent_id);
        }
    }
}
