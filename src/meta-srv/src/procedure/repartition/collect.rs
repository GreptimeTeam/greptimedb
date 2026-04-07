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

use std::any::Any;

use common_procedure::{Context as ProcedureContext, ProcedureId, Status, watcher};
use common_telemetry::{error, info};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error::{RepartitionSubprocedureStateReceiverSnafu, Result};
use crate::procedure::repartition::deallocate_region::DeallocateRegion;
use crate::procedure::repartition::group::GroupId;
use crate::procedure::repartition::{Context, State};

/// Metadata for tracking a dispatched sub-procedure.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct ProcedureMeta {
    /// The index of the plan entry in the parent procedure's plan list.
    pub plan_index: usize,
    /// The group id of the repartition group.
    pub group_id: GroupId,
    /// The procedure id of the sub-procedure.
    pub procedure_id: ProcedureId,
}

/// State for collecting results from dispatched sub-procedures.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Collect {
    /// Sub-procedures that are currently in-flight.
    pub inflight_procedures: Vec<ProcedureMeta>,
    /// Sub-procedures that have completed successfully.
    pub succeeded_procedures: Vec<ProcedureMeta>,
    /// Sub-procedures that have failed.
    pub failed_procedures: Vec<ProcedureMeta>,
    /// Sub-procedures whose state could not be determined.
    pub unknown_procedures: Vec<ProcedureMeta>,
}

impl Collect {
    pub fn new(inflight_procedures: Vec<ProcedureMeta>) -> Self {
        Self {
            inflight_procedures,
            succeeded_procedures: Vec::new(),
            failed_procedures: Vec::new(),
            unknown_procedures: Vec::new(),
        }
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for Collect {
    async fn next(
        &mut self,
        ctx: &mut Context,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let table_id = ctx.persistent_ctx.table_id;
        for procedure_meta in self.inflight_procedures.iter() {
            let procedure_id = procedure_meta.procedure_id;
            let group_id = procedure_meta.group_id;
            let Some(mut receiver) = procedure_ctx
                .provider
                .procedure_state_receiver(procedure_id)
                .await
                .context(RepartitionSubprocedureStateReceiverSnafu { procedure_id })?
            else {
                error!(
                    "failed to get procedure state receiver, procedure_id: {}, group_id: {}",
                    procedure_id, group_id
                );
                self.unknown_procedures.push(*procedure_meta);
                continue;
            };

            match watcher::wait(&mut receiver).await {
                Ok(_) => self.succeeded_procedures.push(*procedure_meta),
                Err(e) => {
                    error!(e; "failed to wait for repartition subprocedure, procedure_id: {}, group_id: {}", procedure_id, group_id);
                    self.failed_procedures.push(*procedure_meta);
                }
            }
        }

        let inflight = self.inflight_procedures.len();
        let succeeded = self.succeeded_procedures.len();
        let failed = self.failed_procedures.len();
        let unknown = self.unknown_procedures.len();
        info!(
            "Collected repartition group results for table_id: {}, inflight: {}, succeeded: {}, failed: {}, unknown: {}",
            table_id, inflight, succeeded, failed, unknown
        );

        if failed > 0 || unknown > 0 {
            ctx.persistent_ctx
                .failed_procedures
                .extend(self.failed_procedures.iter());
            ctx.persistent_ctx
                .unknown_procedures
                .extend(self.unknown_procedures.iter());
            return crate::error::UnexpectedSnafu {
                violated: format!(
                    "Repartition groups failed or became unknown, table_id: {}, failed: {}, unknown: {}",
                    table_id, failed, unknown
                ),
            }
            .fail();
        }

        if let Some(start_time) = ctx.volatile_ctx.dispatch_start_time.take() {
            ctx.update_finish_groups_elapsed(start_time.elapsed());
        }

        Ok((Box::new(DeallocateRegion), Status::executing(true)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::mock::MockError;
    use common_error::status_code::StatusCode;
    use common_meta::test_util::MockDatanodeManager;
    use common_procedure::{
        Context as ProcedureContext, ContextProvider, Error as ProcedureError, ProcedureId,
        ProcedureState,
    };
    use common_procedure_test::MockContextProvider;
    use tokio::sync::watch;

    use super::*;
    use crate::procedure::repartition::PersistentContext;
    use crate::procedure::repartition::test_util::TestingEnv;

    struct FailedProcedureContextProvider {
        receiver: watch::Receiver<ProcedureState>,
        inner: MockContextProvider,
    }

    #[async_trait::async_trait]
    impl ContextProvider for FailedProcedureContextProvider {
        async fn procedure_state(
            &self,
            procedure_id: ProcedureId,
        ) -> common_procedure::Result<Option<ProcedureState>> {
            self.inner.procedure_state(procedure_id).await
        }

        async fn procedure_state_receiver(
            &self,
            _procedure_id: ProcedureId,
        ) -> common_procedure::Result<Option<watch::Receiver<ProcedureState>>> {
            Ok(Some(self.receiver.clone()))
        }

        async fn try_put_poison(
            &self,
            key: &common_procedure::PoisonKey,
            procedure_id: ProcedureId,
        ) -> common_procedure::Result<()> {
            self.inner.try_put_poison(key, procedure_id).await
        }

        async fn acquire_lock(
            &self,
            key: &common_procedure::StringKey,
        ) -> common_procedure::local::DynamicKeyLockGuard {
            self.inner.acquire_lock(key).await
        }
    }

    #[tokio::test]
    async fn test_collect_returns_error_when_unknown_exists() {
        let env = TestingEnv::new();
        let ddl_ctx = env.ddl_context(Arc::new(MockDatanodeManager::new(())));
        let persistent_ctx = PersistentContext::new(
            table::table_name::TableName::new("test_catalog", "test_schema", "test_table"),
            1024,
            None,
        );
        let mut ctx = crate::procedure::repartition::Context::new(
            &ddl_ctx,
            env.mailbox_ctx.mailbox().clone(),
            env.server_addr.clone(),
            persistent_ctx,
        );
        let mut state = Collect {
            inflight_procedures: vec![],
            succeeded_procedures: vec![],
            failed_procedures: vec![],
            unknown_procedures: vec![ProcedureMeta {
                plan_index: 0,
                group_id: uuid::Uuid::new_v4(),
                procedure_id: common_procedure::ProcedureId::random(),
            }],
        };

        let err = state
            .next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap_err();

        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_collect_returns_error_when_failed_exists() {
        let env = TestingEnv::new();
        let ddl_ctx = env.ddl_context(Arc::new(MockDatanodeManager::new(())));
        let persistent_ctx = PersistentContext::new(
            table::table_name::TableName::new("test_catalog", "test_schema", "test_table"),
            1024,
            None,
        );
        let mut ctx = crate::procedure::repartition::Context::new(
            &ddl_ctx,
            env.mailbox_ctx.mailbox().clone(),
            env.server_addr.clone(),
            persistent_ctx,
        );
        let procedure_id = common_procedure::ProcedureId::random();
        let (tx, rx) = watch::channel(ProcedureState::Running);
        tx.send(ProcedureState::failed(Arc::new(ProcedureError::external(
            MockError::new(StatusCode::Internal),
        ))))
        .unwrap();
        let procedure_ctx = ProcedureContext {
            procedure_id: ProcedureId::random(),
            provider: Arc::new(FailedProcedureContextProvider {
                receiver: rx,
                inner: MockContextProvider::default(),
            }),
        };
        let mut state = Collect {
            inflight_procedures: vec![ProcedureMeta {
                plan_index: 0,
                group_id: uuid::Uuid::new_v4(),
                procedure_id,
            }],
            succeeded_procedures: vec![],
            failed_procedures: vec![],
            unknown_procedures: vec![],
        };

        let err = state.next(&mut ctx, &procedure_ctx).await.unwrap_err();

        assert_eq!(state.failed_procedures.len(), 1);
        assert_eq!(state.unknown_procedures.len(), 0);
        assert!(!err.is_retryable());
    }
}
