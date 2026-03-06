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

use api::v1::meta::ReconcileRequest;
use async_trait::async_trait;
use catalog::CatalogManagerRef;
use common_error::ext::BoxedError;
use common_function::handlers::ProcedureServiceHandler;
use common_meta::procedure_executor::{ExecutorContext, ProcedureExecutorRef};
use common_meta::rpc::procedure::{
    GcRegionsRequest as MetaGcRegionsRequest, GcResponse as MetaGcResponse,
    GcTableRequest as MetaGcTableRequest, ManageRegionFollowerRequest, MigrateRegionRequest,
    ProcedureStateResponse,
};
use common_query::error as query_error;
use common_query::error::Result as QueryResult;
use snafu::ResultExt;

/// The operator for procedures which implements [`ProcedureServiceHandler`].
#[derive(Clone)]
pub struct ProcedureServiceOperator {
    procedure_executor: ProcedureExecutorRef,
    catalog_manager: CatalogManagerRef,
}

impl ProcedureServiceOperator {
    pub fn new(
        procedure_executor: ProcedureExecutorRef,
        catalog_manager: CatalogManagerRef,
    ) -> Self {
        Self {
            procedure_executor,
            catalog_manager,
        }
    }
}

#[async_trait]
impl ProcedureServiceHandler for ProcedureServiceOperator {
    async fn migrate_region(&self, request: MigrateRegionRequest) -> QueryResult<Option<String>> {
        Ok(self
            .procedure_executor
            .migrate_region(&ExecutorContext::default(), request)
            .await
            .map_err(BoxedError::new)
            .context(query_error::ProcedureServiceSnafu)?
            .pid
            .map(|pid| String::from_utf8_lossy(&pid.key).to_string()))
    }

    async fn reconcile(&self, request: ReconcileRequest) -> QueryResult<Option<String>> {
        Ok(self
            .procedure_executor
            .reconcile(&ExecutorContext::default(), request)
            .await
            .map_err(BoxedError::new)
            .context(query_error::ProcedureServiceSnafu)?
            .pid
            .map(|pid| String::from_utf8_lossy(&pid.key).to_string()))
    }

    async fn query_procedure_state(&self, pid: &str) -> QueryResult<ProcedureStateResponse> {
        self.procedure_executor
            .query_procedure_state(&ExecutorContext::default(), pid)
            .await
            .map_err(BoxedError::new)
            .context(query_error::ProcedureServiceSnafu)
    }

    async fn manage_region_follower(
        &self,
        request: ManageRegionFollowerRequest,
    ) -> QueryResult<()> {
        self.procedure_executor
            .manage_region_follower(&ExecutorContext::default(), request)
            .await
            .map_err(BoxedError::new)
            .context(query_error::ProcedureServiceSnafu)
    }

    fn catalog_manager(&self) -> &CatalogManagerRef {
        &self.catalog_manager
    }

    async fn gc_regions(&self, request: MetaGcRegionsRequest) -> QueryResult<MetaGcResponse> {
        self.procedure_executor
            .gc_regions(&ExecutorContext::default(), request)
            .await
            .map_err(BoxedError::new)
            .context(query_error::ProcedureServiceSnafu)
    }

    async fn gc_table(&self, request: MetaGcTableRequest) -> QueryResult<MetaGcResponse> {
        self.procedure_executor
            .gc_table(&ExecutorContext::default(), request)
            .await
            .map_err(BoxedError::new)
            .context(query_error::ProcedureServiceSnafu)
    }
}
