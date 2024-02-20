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

use crate::handlers::{ProcedureServiceHandlerRef, TableMutationHandlerRef};

/// Shared state for SQL functions.
/// The handlers in state may be `None` in cli command-line or test cases.
#[derive(Clone, Default)]
pub struct FunctionState {
    // The table mutation handler
    pub table_mutation_handler: Option<TableMutationHandlerRef>,
    // The procedure service handler
    pub procedure_service_handler: Option<ProcedureServiceHandlerRef>,
}

impl FunctionState {
    /// Create a mock [`FunctionState`] for test.
    #[cfg(any(test, feature = "testing"))]
    pub fn mock() -> Self {
        use std::sync::Arc;

        use api::v1::meta::ProcedureStatus;
        use async_trait::async_trait;
        use common_meta::rpc::procedure::{MigrateRegionRequest, ProcedureStateResponse};
        use common_query::error::Result;

        use crate::handlers::ProcedureServiceHandler;
        struct MockProcedureServiceHandler;

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

        Self {
            table_mutation_handler: None,
            procedure_service_handler: Some(Arc::new(MockProcedureServiceHandler)),
        }
    }
}
