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

use crate::error::{Error, Result};
use crate::local::{ExecMeta, ManagerContext, ProcedureMeta, ProcedureMetaRef};
use crate::store::ProcedureStore;
use crate::{BoxedProcedure, Context, ProcedureId, ProcedureState, ProcedureWithId, Status};

const ERR_WAIT_DURATION: Duration = Duration::from_secs(30);

#[derive(Debug)]
enum ExecResult {
    Continue,
    Done,
    RetryLater,
    Failed(Error),
}

#[cfg(test)]
impl ExecResult {
    fn is_continue(&self) -> bool {
        matches!(self, ExecResult::Continue)
    }

    fn is_done(&self) -> bool {
        matches!(self, ExecResult::Done)
    }

    fn is_retry_later(&self) -> bool {
        matches!(self, ExecResult::RetryLater)
    }

    fn is_failed(&self) -> bool {
        matches!(self, ExecResult::Failed(_))
    }
}

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
        if let Err(e) = self.execute_procedure_in_loop().await {
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

    async fn execute_procedure_in_loop(&mut self) -> Result<()> {
        let ctx = Context {
            procedure_id: self.meta.id,
        };

        loop {
            match self.execute_once(&ctx).await {
                ExecResult::Continue => (),
                ExecResult::Done => return Ok(()),
                ExecResult::RetryLater => {
                    self.wait_on_err().await;
                }
                ExecResult::Failed(e) => return Err(e),
            }
        }
    }

    async fn execute_once(&mut self, ctx: &Context) -> ExecResult {
        match self.procedure.execute(ctx).await {
            Ok(status) => {
                if status.need_persist() && self.persist_procedure().await.is_err() {
                    return ExecResult::RetryLater;
                }

                match status {
                    Status::Executing { .. } => (),
                    Status::Suspended { subprocedures, .. } => {
                        self.on_suspended(subprocedures).await;
                    }
                    Status::Done => {
                        if self.commit_procedure().await.is_err() {
                            return ExecResult::RetryLater;
                        }

                        self.done();
                        return ExecResult::Done;
                    }
                }

                ExecResult::Continue
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
                    return ExecResult::RetryLater;
                }

                ExecResult::Failed(e)
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
        let has_child = !subprocedures.is_empty();
        for subprocedure in subprocedures {
            logging::info!(
                "Procedure {}-{} submit subprocedure {}-{}",
                self.procedure.type_name(),
                self.meta.id,
                subprocedure.procedure.type_name(),
                subprocedure.id
            );

            self.submit_subprocedure(subprocedure.id, subprocedure.procedure);
        }

        logging::info!(
            "Procedure {}-{} is waiting for subprocedures",
            self.procedure.type_name(),
            self.meta.id,
        );

        // Wait for subprocedures.
        if has_child {
            self.meta.child_notify.notified().await;

            logging::info!(
                "Procedure {}-{} is waked up",
                self.procedure.type_name(),
                self.meta.id,
            );
        }
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

        // Notify parent procedure, remember to update state first.
        if let Some(parent_id) = self.meta.parent_id {
            self.manager_ctx.notify_by_subprocedure(parent_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use common_error::mock::MockError;
    use common_error::prelude::StatusCode;
    use futures_util::future::BoxFuture;
    use futures_util::{FutureExt, TryStreamExt};
    use object_store::ObjectStore;
    use tempdir::TempDir;

    use super::*;
    use crate::local::test_util;
    use crate::store::ObjectStateStore;
    use crate::{LockKey, Procedure};

    const ROOT_ID: &str = "9f805a1f-05f7-490c-9f91-bd56e3cc54c1";

    fn new_runner(
        meta: ProcedureMetaRef,
        procedure: BoxedProcedure,
        store: ProcedureStore,
    ) -> Runner {
        Runner {
            meta,
            procedure,
            manager_ctx: Arc::new(ManagerContext::new()),
            step: 0,
            store,
        }
    }

    fn new_procedure_store(object_store: ObjectStore) -> ProcedureStore {
        let state_store = ObjectStateStore::new(object_store);

        ProcedureStore::new(Arc::new(state_store))
    }

    async fn check_files(object_store: &ObjectStore, procedure_id: ProcedureId, files: &[&str]) {
        let dir = format!("{procedure_id}/");
        let object = object_store.object(&dir);
        let lister = object.list().await.unwrap();
        let mut files_in_dir: Vec<_> = lister
            .map_ok(|de| de.name().to_string())
            .try_collect()
            .await
            .unwrap();
        files_in_dir.sort_unstable();
        assert_eq!(files, files_in_dir);
    }

    #[derive(Debug)]
    struct ProcedureAdapter<F> {
        data: String,
        lock_key: Option<LockKey>,
        exec_fn: F,
    }

    impl<F> ProcedureAdapter<F> {
        fn new_meta(&self, uuid: &str) -> ProcedureMetaRef {
            let mut meta = test_util::procedure_meta_for_test();
            meta.id = ProcedureId::parse_str(uuid).unwrap();
            meta.lock_key = self.lock_key.clone();

            Arc::new(meta)
        }
    }

    #[async_trait]
    impl<F> Procedure for ProcedureAdapter<F>
    where
        F: FnMut() -> BoxFuture<'static, Result<Status>> + Send + Sync,
    {
        fn type_name(&self) -> &str {
            "ProcedureAdapter"
        }

        async fn execute(&mut self, _ctx: &Context) -> Result<Status> {
            let f = (self.exec_fn)();
            f.await
        }

        fn dump(&self) -> Result<String> {
            Ok(self.data.clone())
        }

        fn lock_key(&self) -> Option<LockKey> {
            self.lock_key.clone()
        }
    }

    async fn execute_once_normal(persist: bool, first_files: &[&str], second_files: &[&str]) {
        let mut times = 0;
        let exec_fn = move || {
            times += 1;
            async move {
                if times == 1 {
                    Ok(Status::Executing { persist })
                } else {
                    Ok(Status::Done)
                }
            }
            .boxed()
        };
        let normal = ProcedureAdapter {
            data: "normal".to_string(),
            lock_key: Some(LockKey::new("catalog.schema.table")),
            exec_fn,
        };

        let dir = TempDir::new("normal").unwrap();
        let meta = normal.new_meta(ROOT_ID);
        let ctx = Context {
            procedure_id: meta.id,
        };
        let object_store = test_util::new_object_store(&dir);
        let procedure_store = new_procedure_store(object_store.clone());
        let mut runner = new_runner(meta, Box::new(normal), procedure_store);

        let res = runner.execute_once(&ctx).await;
        assert!(res.is_continue(), "{res:?}");
        check_files(&object_store, ctx.procedure_id, first_files).await;

        let res = runner.execute_once(&ctx).await;
        assert!(res.is_done(), "{res:?}");
        check_files(&object_store, ctx.procedure_id, second_files).await;
    }

    #[tokio::test]
    async fn test_execute_once_normal() {
        execute_once_normal(
            true,
            &["0000000000.step"],
            &["0000000000.step", "0000000001.commit"],
        )
        .await;
    }

    #[tokio::test]
    async fn test_execute_once_normal_skip_persist() {
        execute_once_normal(false, &[], &["0000000000.commit"]).await;
    }

    #[tokio::test]
    async fn test_on_suspend_empty() {
        let exec_fn = move || {
            async move {
                Ok(Status::Suspended {
                    subprocedures: Vec::new(),
                    persist: false,
                })
            }
            .boxed()
        };
        let suspend = ProcedureAdapter {
            data: "suspend".to_string(),
            lock_key: Some(LockKey::new("catalog.schema.table")),
            exec_fn,
        };

        let dir = TempDir::new("suspend").unwrap();
        let meta = suspend.new_meta(ROOT_ID);
        let ctx = Context {
            procedure_id: meta.id,
        };
        let object_store = test_util::new_object_store(&dir);
        let procedure_store = new_procedure_store(object_store.clone());
        let mut runner = new_runner(meta, Box::new(suspend), procedure_store);

        let res = runner.execute_once(&ctx).await;
        assert!(res.is_continue(), "{res:?}");
    }

    fn new_child_procedure(procedure_id: ProcedureId, key: &str) -> ProcedureWithId {
        let mut times = 0;
        let exec_fn = move || {
            times += 1;
            async move {
                if times == 1 {
                    time::sleep(Duration::from_millis(200)).await;
                    Ok(Status::Executing { persist: true })
                } else {
                    Ok(Status::Done)
                }
            }
            .boxed()
        };
        let child = ProcedureAdapter {
            data: "child".to_string(),
            // Chidren acquire the same locks.
            lock_key: Some(LockKey::new(key)),
            exec_fn,
        };

        ProcedureWithId {
            id: procedure_id,
            procedure: Box::new(child),
        }
    }

    #[tokio::test]
    async fn test_on_suspend_by_subprocedures() {
        let mut times = 0;
        let children_ids = [ProcedureId::random(), ProcedureId::random()];
        let keys = ["catalog.schema.table", "catalog.schema.table.region-0"];
        let manager_ctx = Arc::new(ManagerContext::new());

        let ctx_in_fn = manager_ctx.clone();
        let exec_fn = move || {
            times += 1;
            let ctx_in_future = ctx_in_fn.clone();

            async move {
                if times == 1 {
                    // Submit subprocedures.
                    Ok(Status::Suspended {
                        subprocedures: children_ids
                            .into_iter()
                            .zip(keys)
                            .map(|(id, key)| new_child_procedure(id, key))
                            .collect(),
                        persist: true,
                    })
                } else {
                    // Wait for subprocedures.
                    let all_child_done = children_ids
                        .iter()
                        .all(|id| ctx_in_future.state(*id) == Some(ProcedureState::Done));
                    if all_child_done {
                        return Ok(Status::Done);
                    } else {
                        // Return suspended to wait for notify.
                        Ok(Status::Suspended {
                            subprocedures: Vec::new(),
                            persist: false,
                        })
                    }
                }
            }
            .boxed()
        };
        let parent = ProcedureAdapter {
            data: "parent".to_string(),
            lock_key: Some(LockKey::new("catalog.schema.table")),
            exec_fn,
        };

        let dir = TempDir::new("parent").unwrap();
        let meta = parent.new_meta(ROOT_ID);
        let procedure_id = meta.id;
        // Manually add this procedure to the manager ctx.
        manager_ctx.insert_procedure(meta.clone());

        let object_store = test_util::new_object_store(&dir);
        let procedure_store = new_procedure_store(object_store.clone());
        let mut runner = new_runner(meta, Box::new(parent), procedure_store);
        // Replace the manager ctx.
        runner.manager_ctx = manager_ctx;

        runner.run().await.unwrap();

        // Check files on store.
        for child_id in children_ids {
            check_files(
                &object_store,
                child_id,
                &["0000000000.step", "0000000001.commit"],
            )
            .await;
        }
        check_files(
            &object_store,
            procedure_id,
            &["0000000000.step", "0000000001.commit"],
        )
        .await;
    }

    #[tokio::test]
    async fn test_execute_on_error() {
        let exec_fn =
            || async { Err(Error::external(MockError::new(StatusCode::Unexpected))) }.boxed();
        let fail = ProcedureAdapter {
            data: "fail".to_string(),
            lock_key: Some(LockKey::new("catalog.schema.table")),
            exec_fn,
        };

        let dir = TempDir::new("fail").unwrap();
        let meta = fail.new_meta(ROOT_ID);
        let ctx = Context {
            procedure_id: meta.id,
        };
        let object_store = test_util::new_object_store(&dir);
        let procedure_store = new_procedure_store(object_store.clone());
        let mut runner = new_runner(meta.clone(), Box::new(fail), procedure_store);

        let res = runner.execute_once(&ctx).await;
        assert!(res.is_failed(), "{res:?}");
        assert_eq!(ProcedureState::Failed, meta.state());
        check_files(&object_store, ctx.procedure_id, &["0000000000.rollback"]).await;
    }
}
