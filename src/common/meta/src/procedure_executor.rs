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

use api::v1::meta::{ProcedureDetailResponse, ReconcileRequest, ReconcileResponse};
use common_procedure::{ProcedureId, ProcedureManagerRef};
use common_telemetry::tracing_context::W3cTrace;
use snafu::{OptionExt, ResultExt};

use crate::ddl_manager::DdlManagerRef;
use crate::error::{
    ParseProcedureIdSnafu, ProcedureNotFoundSnafu, QueryProcedureSnafu, Result, UnsupportedSnafu,
};
use crate::rpc::ddl::{SubmitDdlTaskRequest, SubmitDdlTaskResponse};
use crate::rpc::procedure::{
    self, GcRegionsRequest, GcResponse, GcTableRequest, ManageRegionFollowerRequest,
    MigrateRegionRequest, MigrateRegionResponse, ProcedureStateResponse,
};

/// The context of procedure executor.
#[derive(Debug, Default)]
pub struct ExecutorContext {
    pub tracing_context: Option<W3cTrace>,
}

/// The procedure executor that accepts ddl, region migration task etc.
#[async_trait::async_trait]
pub trait ProcedureExecutor: Send + Sync {
    /// Submit a ddl task
    async fn submit_ddl_task(
        &self,
        ctx: &ExecutorContext,
        request: SubmitDdlTaskRequest,
    ) -> Result<SubmitDdlTaskResponse>;

    /// Submit ad manage region follower task
    async fn manage_region_follower(
        &self,
        _ctx: &ExecutorContext,
        _request: ManageRegionFollowerRequest,
    ) -> Result<()> {
        UnsupportedSnafu {
            operation: "manage_region_follower",
        }
        .fail()
    }

    /// Submit a region migration task
    async fn migrate_region(
        &self,
        ctx: &ExecutorContext,
        request: MigrateRegionRequest,
    ) -> Result<MigrateRegionResponse>;

    /// Submit a reconcile task.
    async fn reconcile(
        &self,
        _ctx: &ExecutorContext,
        request: ReconcileRequest,
    ) -> Result<ReconcileResponse>;

    /// Query the procedure state by its id
    async fn query_procedure_state(
        &self,
        ctx: &ExecutorContext,
        pid: &str,
    ) -> Result<ProcedureStateResponse>;

    /// Manually trigger GC for the specified regions.
    async fn gc_regions(
        &self,
        _ctx: &ExecutorContext,
        _request: GcRegionsRequest,
    ) -> Result<GcResponse> {
        UnsupportedSnafu {
            operation: "gc_regions",
        }
        .fail()
    }

    /// Manually trigger GC for the specified table.
    async fn gc_table(
        &self,
        _ctx: &ExecutorContext,
        _request: GcTableRequest,
    ) -> Result<GcResponse> {
        UnsupportedSnafu {
            operation: "gc_table",
        }
        .fail()
    }

    async fn list_procedures(&self, ctx: &ExecutorContext) -> Result<ProcedureDetailResponse>;
}

pub type ProcedureExecutorRef = Arc<dyn ProcedureExecutor>;

/// The local procedure executor that accepts ddl, region migration task etc.
pub struct LocalProcedureExecutor {
    pub ddl_manager: DdlManagerRef,
    pub procedure_manager: ProcedureManagerRef,
}

impl LocalProcedureExecutor {
    pub fn new(ddl_manager: DdlManagerRef, procedure_manager: ProcedureManagerRef) -> Self {
        Self {
            ddl_manager,
            procedure_manager,
        }
    }
}

#[async_trait::async_trait]
impl ProcedureExecutor for LocalProcedureExecutor {
    async fn submit_ddl_task(
        &self,
        ctx: &ExecutorContext,
        request: SubmitDdlTaskRequest,
    ) -> Result<SubmitDdlTaskResponse> {
        self.ddl_manager.submit_ddl_task(ctx, request).await
    }

    async fn migrate_region(
        &self,
        _ctx: &ExecutorContext,
        _request: MigrateRegionRequest,
    ) -> Result<MigrateRegionResponse> {
        UnsupportedSnafu {
            operation: "migrate_region",
        }
        .fail()
    }

    async fn reconcile(
        &self,
        _ctx: &ExecutorContext,
        _request: ReconcileRequest,
    ) -> Result<ReconcileResponse> {
        UnsupportedSnafu {
            operation: "reconcile",
        }
        .fail()
    }

    async fn query_procedure_state(
        &self,
        _ctx: &ExecutorContext,
        pid: &str,
    ) -> Result<ProcedureStateResponse> {
        let pid =
            ProcedureId::parse_str(pid).with_context(|_| ParseProcedureIdSnafu { key: pid })?;

        let state = self
            .procedure_manager
            .procedure_state(pid)
            .await
            .context(QueryProcedureSnafu)?
            .with_context(|| ProcedureNotFoundSnafu {
                pid: pid.to_string(),
            })?;

        Ok(procedure::procedure_state_to_pb_response(&state))
    }

    async fn list_procedures(&self, _ctx: &ExecutorContext) -> Result<ProcedureDetailResponse> {
        let metas = self
            .procedure_manager
            .list_procedures()
            .await
            .context(QueryProcedureSnafu)?;
        Ok(procedure::procedure_details_to_pb_response(metas))
    }
}
