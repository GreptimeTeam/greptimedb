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
use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBuilder};
use common_telemetry::logging;
use tokio::time;

use crate::error::{ProcedurePanicSnafu, Result};
use crate::local::{ManagerContext, ProcedureMeta, ProcedureMetaRef};
use crate::store::ProcedureStore;
use crate::ProcedureState::Retrying;
use crate::{BoxedProcedure, Context, Error, ProcedureId, ProcedureState, ProcedureWithId, Status};

#[derive(Debug)]
enum ExecResult {
    Continue,
    Done,
    RetryLater,
    Failed,
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
        matches!(self, ExecResult::Failed)
    }
}

/// A guard to cleanup procedure state.
struct ProcedureGuard {
    meta: ProcedureMetaRef,
    manager_ctx: Arc<ManagerContext>,
    finish: bool,
}

impl ProcedureGuard {
    /// Returns a new [ProcedureGuard].
    fn new(meta: ProcedureMetaRef, manager_ctx: Arc<ManagerContext>) -> ProcedureGuard {
        ProcedureGuard {
            meta,
            manager_ctx,
            finish: false,
        }
    }

    /// The procedure is finished successfully.
    fn finish(mut self) {
        self.finish = true;
    }
}

impl Drop for ProcedureGuard {
    fn drop(&mut self) {
        if !self.finish {
            logging::error!("Procedure {} exits unexpectedly", self.meta.id);

            // Set state to failed. This is useful in test as runtime may not abort when the runner task panics.
            // See https://github.com/tokio-rs/tokio/issues/2002 .
            // We set set_panic_hook() in the application's main function. But our tests don't have this panic hook.
            let err = ProcedurePanicSnafu {
                procedure_id: self.meta.id,
            }
            .build();
            self.meta.set_state(ProcedureState::failed(Arc::new(err)));
        }

        // Notify parent procedure.
        if let Some(parent_id) = self.meta.parent_id {
            self.manager_ctx.notify_by_subprocedure(parent_id);
        }

        // Release lock in reverse order.
        for key in self.meta.lock_key.keys_to_unlock() {
            self.manager_ctx.lock_map.release_lock(key, self.meta.id);
        }
    }
}

// TODO(yingwen): Support cancellation.
pub(crate) struct Runner {
    pub(crate) meta: ProcedureMetaRef,
    pub(crate) procedure: BoxedProcedure,
    pub(crate) manager_ctx: Arc<ManagerContext>,
    pub(crate) step: u32,
    pub(crate) exponential_builder: ExponentialBuilder,
    pub(crate) store: Arc<ProcedureStore>,
    pub(crate) rolling_back: bool,
}

impl Runner {
    /// Run the procedure.
    pub(crate) async fn run(mut self) {
        // Ensure we can update the procedure state.
        let guard = ProcedureGuard::new(self.meta.clone(), self.manager_ctx.clone());

        logging::info!(
            "Runner {}-{} starts",
            self.procedure.type_name(),
            self.meta.id
        );

        // TODO(yingwen): Detect recursive locking (and deadlock) if possible. Maybe we could detect
        // recursive locking by adding a root procedure id to the meta.
        for key in self.meta.lock_key.keys_to_lock() {
            // Acquire lock for each key.
            self.manager_ctx
                .lock_map
                .acquire_lock(key, self.meta.clone())
                .await;
        }

        // Execute the procedure. We need to release the lock whenever the the execution
        // is successful or fail.
        self.execute_procedure_in_loop().await;

        // We can't remove the metadata of the procedure now as users and its parent might
        // need to query its state.
        // TODO(yingwen): 1. Add TTL to the metadata; 2. Only keep state in the procedure store
        // so we don't need to always store the metadata in memory after the procedure is done.

        // Release locks and notify parent procedure.
        guard.finish();

        // If this is the root procedure, clean up message cache.
        if self.meta.parent_id.is_none() {
            let procedure_ids = self.manager_ctx.procedures_in_tree(&self.meta);
            // Clean resources.
            self.manager_ctx.on_procedures_finish(&procedure_ids);
            for id in procedure_ids {
                if let Err(e) = self.store.delete_procedure(id).await {
                    logging::error!(
                        e;
                        "Runner {}-{} failed to delete procedure {}",
                        self.procedure.type_name(),
                        self.meta.id,
                        id,
                    );
                }
            }
        }

        logging::info!(
            "Runner {}-{} exits",
            self.procedure.type_name(),
            self.meta.id
        );
    }

    async fn execute_procedure_in_loop(&mut self) {
        let ctx = Context {
            procedure_id: self.meta.id,
            provider: self.manager_ctx.clone(),
        };

        self.rolling_back = false;
        self.execute_once_with_retry(&ctx).await;
    }

    async fn execute_once_with_retry(&mut self, ctx: &Context) {
        let mut retry = self.exponential_builder.build();
        let mut retry_times = 0;
        loop {
            match self.execute_once(ctx).await {
                ExecResult::Done | ExecResult::Failed => return,
                ExecResult::Continue => (),
                ExecResult::RetryLater => {
                    retry_times += 1;
                    if let Some(d) = retry.next() {
                        self.wait_on_err(d, retry_times).await;
                    } else {
                        assert!(self.meta.state().is_retrying());
                        if let Retrying { error } = self.meta.state() {
                            self.meta.set_state(ProcedureState::failed(Arc::new(
                                Error::RetryTimesExceeded {
                                    source: error,
                                    procedure_id: self.meta.id,
                                },
                            )))
                        }
                        return;
                    }
                }
            }
        }
    }

    async fn rollback(&mut self, error: Arc<Error>) -> ExecResult {
        if let Err(e) = self.rollback_procedure().await {
            self.rolling_back = true;
            self.meta.set_state(ProcedureState::retrying(Arc::new(e)));
            return ExecResult::RetryLater;
        }
        self.meta.set_state(ProcedureState::failed(error));
        ExecResult::Failed
    }

    async fn execute_once(&mut self, ctx: &Context) -> ExecResult {
        // if rolling_back, there is no need to execute again.
        if self.rolling_back {
            // We can definitely get the previous error here.
            let state = self.meta.state();
            let err = state.error().unwrap();
            return self.rollback(err.clone()).await;
        }
        match self.procedure.execute(ctx).await {
            Ok(status) => {
                logging::debug!(
                    "Execute procedure {}-{} once, status: {:?}, need_persist: {}",
                    self.procedure.type_name(),
                    self.meta.id,
                    status,
                    status.need_persist(),
                );

                if status.need_persist() {
                    if let Err(err) = self.persist_procedure().await {
                        self.meta.set_state(ProcedureState::retrying(Arc::new(err)));
                        return ExecResult::RetryLater;
                    }
                }

                match status {
                    Status::Executing { .. } => (),
                    Status::Suspended { subprocedures, .. } => {
                        self.on_suspended(subprocedures).await;
                    }
                    Status::Done => {
                        if let Err(e) = self.commit_procedure().await {
                            self.meta.set_state(ProcedureState::retrying(Arc::new(e)));
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
                    "Failed to execute procedure {}-{}, retry: {}",
                    self.procedure.type_name(),
                    self.meta.id,
                    e.is_retry_later(),
                );

                if e.is_retry_later() {
                    self.meta.set_state(ProcedureState::retrying(Arc::new(e)));
                    return ExecResult::RetryLater;
                }

                // Write rollback key so we can skip this procedure while recovering procedures.
                self.rollback(Arc::new(e)).await
            }
        }
    }

    /// Submit a subprocedure with specific `procedure_id`.
    fn submit_subprocedure(&self, procedure_id: ProcedureId, mut procedure: BoxedProcedure) {
        if self.manager_ctx.contains_procedure(procedure_id) {
            // If the parent has already submitted this procedure, don't submit it again.
            return;
        }

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

        let meta = Arc::new(ProcedureMeta::new(
            procedure_id,
            Some(self.meta.id),
            procedure.lock_key(),
        ));
        let runner = Runner {
            meta: meta.clone(),
            procedure,
            manager_ctx: self.manager_ctx.clone(),
            step,
            exponential_builder: self.exponential_builder.clone(),
            store: self.store.clone(),
            rolling_back: false,
        };

        // Insert the procedure. We already check the procedure existence before inserting
        // so we add an assertion to ensure the procedure id is unique and no other procedures
        // using the same procedure id.
        assert!(
            self.manager_ctx.try_insert_procedure(meta),
            "Procedure {}-{} submit an existing procedure {}-{}",
            self.procedure.type_name(),
            self.meta.id,
            runner.procedure.type_name(),
            procedure_id,
        );

        // Add the id of the subprocedure to the metadata.
        self.meta.push_child(procedure_id);

        let _handle = common_runtime::spawn_bg(async move {
            // Run the root procedure.
            runner.run().await
        });
    }

    /// Extend the retry time to wait for the next retry.
    async fn wait_on_err(&self, d: Duration, i: u64) {
        logging::info!(
            "Procedure {}-{} retry for the {} times after {} millis",
            self.procedure.type_name(),
            self.meta.id,
            i,
            d.as_millis(),
        );
        time::sleep(d).await;
    }

    async fn on_suspended(&self, subprocedures: Vec<ProcedureWithId>) {
        let has_child = !subprocedures.is_empty();
        for subprocedure in subprocedures {
            logging::info!(
                "Procedure {}-{} submit subprocedure {}-{}",
                self.procedure.type_name(),
                self.meta.id,
                subprocedure.procedure.type_name(),
                subprocedure.id,
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
            self.meta.id,
        );

        // Mark the state of this procedure to done.
        self.meta.set_state(ProcedureState::Done);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use common_error::ext::{ErrorExt, PlainError};
    use common_error::mock::MockError;
    use common_error::status_code::StatusCode;
    use common_test_util::temp_dir::create_temp_dir;
    use futures_util::future::BoxFuture;
    use futures_util::FutureExt;
    use object_store::ObjectStore;

    use super::*;
    use crate::local::test_util;
    use crate::store::proc_path;
    use crate::{ContextProvider, Error, LockKey, Procedure};

    const ROOT_ID: &str = "9f805a1f-05f7-490c-9f91-bd56e3cc54c1";

    fn new_runner(
        meta: ProcedureMetaRef,
        procedure: BoxedProcedure,
        store: Arc<ProcedureStore>,
    ) -> Runner {
        Runner {
            meta,
            procedure,
            manager_ctx: Arc::new(ManagerContext::new()),
            step: 0,
            exponential_builder: ExponentialBuilder::default(),
            store,
            rolling_back: false,
        }
    }

    async fn check_files(
        object_store: &ObjectStore,
        procedure_store: &ProcedureStore,
        procedure_id: ProcedureId,
        files: &[&str],
    ) {
        let dir = proc_path!(procedure_store, "{procedure_id}/");
        let lister = object_store.list(&dir).await.unwrap();
        let mut files_in_dir: Vec<_> = lister.into_iter().map(|de| de.name().to_string()).collect();
        files_in_dir.sort_unstable();
        assert_eq!(files, files_in_dir);
    }

    fn context_without_provider(procedure_id: ProcedureId) -> Context {
        struct MockProvider;

        #[async_trait]
        impl ContextProvider for MockProvider {
            async fn procedure_state(
                &self,
                _procedure_id: ProcedureId,
            ) -> Result<Option<ProcedureState>> {
                unimplemented!()
            }
        }

        Context {
            procedure_id,
            provider: Arc::new(MockProvider),
        }
    }

    #[derive(Debug)]
    struct ProcedureAdapter<F> {
        data: String,
        lock_key: LockKey,
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
        F: FnMut(Context) -> BoxFuture<'static, Result<Status>> + Send + Sync,
    {
        fn type_name(&self) -> &str {
            "ProcedureAdapter"
        }

        async fn execute(&mut self, ctx: &Context) -> Result<Status> {
            let f = (self.exec_fn)(ctx.clone());
            f.await
        }

        fn dump(&self) -> Result<String> {
            Ok(self.data.clone())
        }

        fn lock_key(&self) -> LockKey {
            self.lock_key.clone()
        }
    }

    async fn execute_once_normal(persist: bool, first_files: &[&str], second_files: &[&str]) {
        let mut times = 0;
        let exec_fn = move |_| {
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
            lock_key: LockKey::single("catalog.schema.table"),
            exec_fn,
        };

        let dir = create_temp_dir("normal");
        let meta = normal.new_meta(ROOT_ID);
        let ctx = context_without_provider(meta.id);
        let object_store = test_util::new_object_store(&dir);
        let procedure_store = Arc::new(ProcedureStore::from_object_store(object_store.clone()));
        let mut runner = new_runner(meta, Box::new(normal), procedure_store.clone());

        let res = runner.execute_once(&ctx).await;
        assert!(res.is_continue(), "{res:?}");
        check_files(
            &object_store,
            &procedure_store,
            ctx.procedure_id,
            first_files,
        )
        .await;

        let res = runner.execute_once(&ctx).await;
        assert!(res.is_done(), "{res:?}");
        check_files(
            &object_store,
            &procedure_store,
            ctx.procedure_id,
            second_files,
        )
        .await;
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
        let exec_fn = move |_| {
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
            lock_key: LockKey::single("catalog.schema.table"),
            exec_fn,
        };

        let dir = create_temp_dir("suspend");
        let meta = suspend.new_meta(ROOT_ID);
        let ctx = context_without_provider(meta.id);
        let object_store = test_util::new_object_store(&dir);
        let procedure_store = Arc::new(ProcedureStore::from_object_store(object_store.clone()));
        let mut runner = new_runner(meta, Box::new(suspend), procedure_store);

        let res = runner.execute_once(&ctx).await;
        assert!(res.is_continue(), "{res:?}");
    }

    fn new_child_procedure(procedure_id: ProcedureId, keys: &[&str]) -> ProcedureWithId {
        let mut times = 0;
        let exec_fn = move |_| {
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
            lock_key: LockKey::new(keys.iter().map(|k| k.to_string())),
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
        let keys = [
            &[
                "catalog.schema.table.region-0",
                "catalog.schema.table.region-1",
            ],
            &[
                "catalog.schema.table.region-2",
                "catalog.schema.table.region-3",
            ],
        ];

        let exec_fn = move |ctx: Context| {
            times += 1;
            async move {
                if times == 1 {
                    // Submit subprocedures.
                    Ok(Status::Suspended {
                        subprocedures: children_ids
                            .into_iter()
                            .zip(keys)
                            .map(|(id, key_slice)| new_child_procedure(id, key_slice))
                            .collect(),
                        persist: true,
                    })
                } else {
                    // Wait for subprocedures.
                    let mut all_child_done = true;
                    for id in children_ids {
                        let is_not_done = ctx
                            .provider
                            .procedure_state(id)
                            .await
                            .unwrap()
                            .map(|s| !s.is_done())
                            .unwrap_or(true);
                        if is_not_done {
                            all_child_done = false;
                        }
                    }
                    if all_child_done {
                        Ok(Status::Done)
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
            lock_key: LockKey::single("catalog.schema.table"),
            exec_fn,
        };

        let dir = create_temp_dir("parent");
        let meta = parent.new_meta(ROOT_ID);
        let procedure_id = meta.id;

        let object_store = test_util::new_object_store(&dir);
        let procedure_store = Arc::new(ProcedureStore::from_object_store(object_store.clone()));
        let mut runner = new_runner(meta.clone(), Box::new(parent), procedure_store.clone());
        let manager_ctx = Arc::new(ManagerContext::new());
        // Manually add this procedure to the manager ctx.
        assert!(manager_ctx.try_insert_procedure(meta));
        // Replace the manager ctx.
        runner.manager_ctx = manager_ctx.clone();

        runner.run().await;

        // Check child procedures.
        for child_id in children_ids {
            let state = manager_ctx.state(child_id).unwrap();
            assert!(state.is_done(), "{state:?}");
        }
        let state = manager_ctx.state(procedure_id).unwrap();
        assert!(state.is_done(), "{state:?}");
        // Files are removed.
        check_files(&object_store, &procedure_store, procedure_id, &[]).await;

        tokio::time::sleep(Duration::from_millis(5)).await;
        // Clean outdated meta.
        manager_ctx.remove_outdated_meta(Duration::from_millis(1));
        assert!(manager_ctx.state(procedure_id).is_none());
        assert!(manager_ctx.finished_procedures.lock().unwrap().is_empty());
        for child_id in children_ids {
            assert!(manager_ctx.state(child_id).is_none());
        }
    }

    #[tokio::test]
    async fn test_execute_on_error() {
        let exec_fn =
            |_| async { Err(Error::external(MockError::new(StatusCode::Unexpected))) }.boxed();
        let fail = ProcedureAdapter {
            data: "fail".to_string(),
            lock_key: LockKey::single("catalog.schema.table"),
            exec_fn,
        };

        let dir = create_temp_dir("fail");
        let meta = fail.new_meta(ROOT_ID);
        let ctx = context_without_provider(meta.id);
        let object_store = test_util::new_object_store(&dir);
        let procedure_store = Arc::new(ProcedureStore::from_object_store(object_store.clone()));
        let mut runner = new_runner(meta.clone(), Box::new(fail), procedure_store.clone());

        let res = runner.execute_once(&ctx).await;
        assert!(res.is_failed(), "{res:?}");
        assert!(meta.state().is_failed());
        check_files(
            &object_store,
            &procedure_store,
            ctx.procedure_id,
            &["0000000000.rollback"],
        )
        .await;
    }

    #[tokio::test]
    async fn test_execute_on_retry_later_error() {
        let mut times = 0;

        let exec_fn = move |_| {
            times += 1;
            async move {
                if times == 1 {
                    Err(Error::retry_later(MockError::new(StatusCode::Unexpected)))
                } else {
                    Ok(Status::Done)
                }
            }
            .boxed()
        };

        let retry_later = ProcedureAdapter {
            data: "retry_later".to_string(),
            lock_key: LockKey::single("catalog.schema.table"),
            exec_fn,
        };

        let dir = create_temp_dir("retry_later");
        let meta = retry_later.new_meta(ROOT_ID);
        let ctx = context_without_provider(meta.id);
        let object_store = test_util::new_object_store(&dir);
        let procedure_store = Arc::new(ProcedureStore::from_object_store(object_store.clone()));
        let mut runner = new_runner(meta.clone(), Box::new(retry_later), procedure_store.clone());

        let res = runner.execute_once(&ctx).await;
        assert!(res.is_retry_later(), "{res:?}");
        assert!(meta.state().is_retrying());

        let res = runner.execute_once(&ctx).await;
        assert!(res.is_done(), "{res:?}");
        assert!(meta.state().is_done());
        check_files(
            &object_store,
            &procedure_store,
            ctx.procedure_id,
            &["0000000000.commit"],
        )
        .await;
    }

    #[tokio::test]
    async fn test_execute_exceed_max_retry_later() {
        let exec_fn =
            |_| async { Err(Error::retry_later(MockError::new(StatusCode::Unexpected))) }.boxed();

        let exceed_max_retry_later = ProcedureAdapter {
            data: "exceed_max_retry_later".to_string(),
            lock_key: LockKey::single("catalog.schema.table"),
            exec_fn,
        };

        let dir = create_temp_dir("exceed_max_retry_later");
        let meta = exceed_max_retry_later.new_meta(ROOT_ID);
        let object_store = test_util::new_object_store(&dir);
        let procedure_store = Arc::new(ProcedureStore::from_object_store(object_store.clone()));
        let mut runner = new_runner(
            meta.clone(),
            Box::new(exceed_max_retry_later),
            procedure_store,
        );
        runner.exponential_builder = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(1))
            .with_max_times(3);

        // Run the runner and execute the procedure.
        runner.execute_procedure_in_loop().await;
        let err = meta.state().error().unwrap().to_string();
        assert!(err.contains("Procedure retry exceeded max times"));
    }

    #[tokio::test]
    async fn test_child_error() {
        let mut times = 0;
        let child_id = ProcedureId::random();

        let exec_fn = move |ctx: Context| {
            times += 1;
            async move {
                if times == 1 {
                    // Submit subprocedures.
                    let exec_fn = |_| {
                        async { Err(Error::external(MockError::new(StatusCode::Unexpected))) }
                            .boxed()
                    };
                    let fail = ProcedureAdapter {
                        data: "fail".to_string(),
                        lock_key: LockKey::single("catalog.schema.table.region-0"),
                        exec_fn,
                    };

                    Ok(Status::Suspended {
                        subprocedures: vec![ProcedureWithId {
                            id: child_id,
                            procedure: Box::new(fail),
                        }],
                        persist: true,
                    })
                } else {
                    // Wait for subprocedures.
                    let state = ctx.provider.procedure_state(child_id).await.unwrap();
                    let is_failed = state.map(|s| s.is_failed()).unwrap_or(false);
                    if is_failed {
                        // The parent procedure to abort itself if child procedure is failed.
                        Err(Error::from_error_ext(PlainError::new(
                            "subprocedure failed".to_string(),
                            StatusCode::Unexpected,
                        )))
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
            lock_key: LockKey::single("catalog.schema.table"),
            exec_fn,
        };

        let dir = create_temp_dir("child_err");
        let meta = parent.new_meta(ROOT_ID);

        let object_store = test_util::new_object_store(&dir);
        let procedure_store = Arc::new(ProcedureStore::from_object_store(object_store.clone()));
        let mut runner = new_runner(meta.clone(), Box::new(parent), procedure_store);

        let manager_ctx = Arc::new(ManagerContext::new());
        // Manually add this procedure to the manager ctx.
        assert!(manager_ctx.try_insert_procedure(meta.clone()));
        // Replace the manager ctx.
        runner.manager_ctx = manager_ctx;

        // Run the runner and execute the procedure.
        runner.run().await;
        let err = meta.state().error().unwrap().output_msg();
        assert!(err.contains("subprocedure failed"), "{err}");
    }
}
