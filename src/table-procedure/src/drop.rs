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

//! Procedure to drop a table.

use async_trait::async_trait;
use catalog::CatalogManagerRef;
use common_procedure::error::SubprocedureFailedSnafu;
use common_procedure::{
    Context, Error, LockKey, Procedure, ProcedureId, ProcedureManager, ProcedureState,
    ProcedureWithId, Result, Status,
};
use common_telemetry::logging;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use table::engine::{EngineContext, TableEngineProcedureRef, TableReference};
use table::requests::DropTableRequest;

use crate::error::{
    AccessCatalogSnafu, DeserializeProcedureSnafu, SerializeProcedureSnafu, TableNotFoundSnafu,
};

/// Procedure to drop a table.
pub struct DropTableProcedure {
    data: DropTableData,
    catalog_manager: CatalogManagerRef,
    engine_procedure: TableEngineProcedureRef,
}

#[async_trait]
impl Procedure for DropTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, ctx: &Context) -> Result<Status> {
        match self.data.state {
            DropTableState::Prepare => self.on_prepare().await,
            DropTableState::RemoveFromCatalog => self.on_remove_from_catalog().await,
            DropTableState::EngineDropTable => self.on_engine_drop_table(ctx).await,
        }
    }

    fn dump(&self) -> Result<String> {
        let json = serde_json::to_string(&self.data).context(SerializeProcedureSnafu)?;
        Ok(json)
    }

    fn lock_key(&self) -> LockKey {
        // We lock the whole table.
        let table_name = self.data.table_ref().to_string();
        LockKey::single(table_name)
    }
}

impl DropTableProcedure {
    const TYPE_NAME: &str = "table-procedure::DropTableProcedure";

    /// Returns a new [DropTableProcedure].
    pub fn new(
        request: DropTableRequest,
        catalog_manager: CatalogManagerRef,
        engine_procedure: TableEngineProcedureRef,
    ) -> DropTableProcedure {
        DropTableProcedure {
            data: DropTableData {
                state: DropTableState::Prepare,
                request,
                subprocedure_id: None,
            },
            catalog_manager,
            engine_procedure,
        }
    }

    /// Register the loader of this procedure to the `procedure_manager`.
    ///
    /// # Panics
    /// Panics on error.
    pub fn register_loader(
        catalog_manager: CatalogManagerRef,
        engine_procedure: TableEngineProcedureRef,
        procedure_manager: &dyn ProcedureManager,
    ) {
        procedure_manager
            .register_loader(
                Self::TYPE_NAME,
                Box::new(move |data| {
                    Self::from_json(data, catalog_manager.clone(), engine_procedure.clone())
                        .map(|p| Box::new(p) as _)
                }),
            )
            .unwrap()
    }

    /// Recover the procedure from json.
    fn from_json(
        json: &str,
        catalog_manager: CatalogManagerRef,
        engine_procedure: TableEngineProcedureRef,
    ) -> Result<Self> {
        let data: DropTableData = serde_json::from_str(json).context(DeserializeProcedureSnafu)?;

        Ok(DropTableProcedure {
            data,
            catalog_manager,
            engine_procedure,
        })
    }

    async fn on_prepare(&mut self) -> Result<Status> {
        let request = &self.data.request;
        // Ensure the table exists.
        let table_exists = self
            .catalog_manager
            .table_exist(
                &request.catalog_name,
                &request.schema_name,
                &request.table_name,
            )
            .await
            .context(AccessCatalogSnafu)?;
        ensure!(
            table_exists,
            TableNotFoundSnafu {
                name: &request.table_name,
            }
        );

        self.data.state = DropTableState::RemoveFromCatalog;

        Ok(Status::executing(true))
    }

    async fn on_remove_from_catalog(&mut self) -> Result<Status> {
        self.data.state = DropTableState::EngineDropTable;
        // Assign procedure id to the subprocedure.
        self.data.subprocedure_id = Some(ProcedureId::random());

        Ok(Status::executing(true))
    }

    async fn on_engine_drop_table(&mut self, ctx: &Context) -> Result<Status> {
        // Safety: subprocedure id is always set in this state.
        let sub_id = self.data.subprocedure_id.unwrap();

        // Query subprocedure state.
        let Some(sub_state) = ctx.provider.procedure_state(sub_id).await? else {
            logging::info!(
                "On engine drop table {}, subprocedure not found, sub_id: {}",
                self.data.request.table_name,
                sub_id
            );

            // If the subprocedure is not found, we create a new subprocedure with the same id.
            let engine_ctx = EngineContext::default();

            let procedure = self
                .engine_procedure
                .drop_table_procedure(&engine_ctx, self.data.request.clone())
                .map_err(Error::from_error_ext)?;
            return Ok(Status::Suspended {
                subprocedures: vec![ProcedureWithId {
                    id: sub_id,
                    procedure,
                }],
                persist: true,
            });
        };

        match sub_state {
            ProcedureState::Running | ProcedureState::Retrying { .. } => Ok(Status::Suspended {
                subprocedures: Vec::new(),
                persist: false,
            }),
            ProcedureState::Done => {
                logging::info!(
                    "On engine drop table {}, done, sub_id: {}",
                    self.data.request.table_name,
                    sub_id
                );

                Ok(Status::Done)
            }
            ProcedureState::Failed { error } => {
                // Return error if the subprocedure is failed.
                Err(error).context(SubprocedureFailedSnafu {
                    subprocedure_id: sub_id,
                })?
            }
        }
    }
}

/// Represents each step while dropping a table in the datanode.
#[derive(Debug, Serialize, Deserialize)]
enum DropTableState {
    /// Validate request and prepare to drop table.
    Prepare,
    /// Remove the table from the catalog.
    RemoveFromCatalog,
    /// Drop table in the table engine.
    EngineDropTable,
}

/// Serializable data of [DropTableProcedure].
#[derive(Debug, Serialize, Deserialize)]
struct DropTableData {
    /// Current state.
    state: DropTableState,
    /// Request to drop this table.
    request: DropTableRequest,
    /// Id of the subprocedure to drop this table from the engine.
    ///
    /// This id is `Some` while the procedure is in [DropTableState::EngineDropTable]
    /// state.
    subprocedure_id: Option<ProcedureId>,
}

impl DropTableData {
    fn table_ref(&self) -> TableReference {
        TableReference {
            catalog: &self.request.catalog_name,
            schema: &self.request.schema_name,
            table: &self.request.table_name,
        }
    }
}

#[cfg(test)]
mod tests {
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};

    use super::*;
    use crate::test_util::TestEnv;

    #[tokio::test]
    async fn test_drop_not_exists_table() {
        common_telemetry::init_default_ut_logging();
        let TestEnv {
            dir: _,
            table_engine,
            procedure_manager: _,
            catalog_manager,
        } = TestEnv::new("drop");
        let table_name = "test_drop";

        let request = DropTableRequest {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table_name.to_string(),
            table_id: 0,
        };

        let mut procedure =
            DropTableProcedure::new(request, catalog_manager.clone(), table_engine.clone());
        assert!(procedure.on_prepare().await.is_err());
    }
}
