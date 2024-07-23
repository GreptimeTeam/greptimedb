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

use crate::handlers::{FlowServiceHandlerRef, ProcedureServiceHandlerRef, TableMutationHandlerRef};

/// Shared state for SQL functions.
/// The handlers in state may be `None` in cli command-line or test cases.
#[derive(Clone, Default)]
pub struct FunctionState {
    // The table mutation handler
    pub table_mutation_handler: Option<TableMutationHandlerRef>,
    // The procedure service handler
    pub procedure_service_handler: Option<ProcedureServiceHandlerRef>,
    // The flownode handler
    pub flow_service_handler: Option<FlowServiceHandlerRef>,
}

impl FunctionState {
    /// Create a mock [`FunctionState`] for test.
    #[cfg(any(test, feature = "testing"))]
    pub fn mock() -> Self {
        use std::sync::Arc;

        use api::v1::meta::ProcedureStatus;
        use async_trait::async_trait;
        use common_base::AffectedRows;
        use common_meta::rpc::procedure::{MigrateRegionRequest, ProcedureStateResponse};
        use common_query::error::Result;
        use common_query::Output;
        use session::context::QueryContextRef;
        use store_api::storage::RegionId;
        use table::requests::{
            CompactTableRequest, DeleteRequest, FlushTableRequest, InsertRequest,
        };

        use crate::handlers::{FlowServiceHandler, ProcedureServiceHandler, TableMutationHandler};
        struct MockProcedureServiceHandler;
        struct MockTableMutationHandler;
        struct MockFlowServiceHandler;
        const ROWS: usize = 42;

        #[async_trait]
        impl ProcedureServiceHandler for MockProcedureServiceHandler {
            async fn migrate_region(
                &self,
                _request: MigrateRegionRequest,
            ) -> Result<Option<String>> {
                Ok(Some("test_pid".to_string()))
            }

            async fn query_procedure_state(&self, _pid: &str) -> Result<ProcedureStateResponse> {
                Ok(ProcedureStateResponse {
                    status: ProcedureStatus::Done.into(),
                    error: "OK".to_string(),
                    ..Default::default()
                })
            }
        }

        #[async_trait]
        impl TableMutationHandler for MockTableMutationHandler {
            async fn insert(
                &self,
                _request: InsertRequest,
                _ctx: QueryContextRef,
            ) -> Result<Output> {
                Ok(Output::new_with_affected_rows(ROWS))
            }

            async fn delete(
                &self,
                _request: DeleteRequest,
                _ctx: QueryContextRef,
            ) -> Result<AffectedRows> {
                Ok(ROWS)
            }

            async fn flush(
                &self,
                _request: FlushTableRequest,
                _ctx: QueryContextRef,
            ) -> Result<AffectedRows> {
                Ok(ROWS)
            }

            async fn compact(
                &self,
                _request: CompactTableRequest,
                _ctx: QueryContextRef,
            ) -> Result<AffectedRows> {
                Ok(ROWS)
            }

            async fn flush_region(
                &self,
                _region_id: RegionId,
                _ctx: QueryContextRef,
            ) -> Result<AffectedRows> {
                Ok(ROWS)
            }

            async fn compact_region(
                &self,
                _region_id: RegionId,
                _ctx: QueryContextRef,
            ) -> Result<AffectedRows> {
                Ok(ROWS)
            }
        }

        #[async_trait]
        impl FlowServiceHandler for MockFlowServiceHandler {
            async fn flush(
                &self,
                _catalog: &str,
                _flow: &str,
                _ctx: QueryContextRef,
            ) -> Result<api::v1::flow::FlowResponse> {
                todo!()
            }
        }

        Self {
            table_mutation_handler: Some(Arc::new(MockTableMutationHandler)),
            procedure_service_handler: Some(Arc::new(MockProcedureServiceHandler)),
            flow_service_handler: Some(Arc::new(MockFlowServiceHandler)),
        }
    }
}
