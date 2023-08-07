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

//! Procedure to truncate a table.

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
use table::requests::TruncateTableRequest;

use crate::error::{
    AccessCatalogSnafu, DeserializeProcedureSnafu, SerializeProcedureSnafu, TableNotFoundSnafu,
};

/// Procedure to truncate a table.
#[allow(dead_code)]
pub struct TruncateTableProcedure {
    data: TruncateTableData,
    catalog_manager: CatalogManagerRef,
    engine_procedure: TableEngineProcedureRef,
}

#[async_trait]
impl Procedure for TruncateTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, ctx: &Context) -> Result<Status> {
        match self.data.state {
            TruncateTableState::Prepare => self.on_prepare().await,
            TruncateTableState::EngineTruncateTable => self.on_engine_truncate_table(ctx).await,
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

impl TruncateTableProcedure {
    const TYPE_NAME: &str = "table-procedure::TruncateTableProcedure";

    /// Returns a new [TruncateTableProcedure].
    pub fn new(
        request: TruncateTableRequest,
        catalog_manager: CatalogManagerRef,
        engine_procedure: TableEngineProcedureRef,
    ) -> TruncateTableProcedure {
        TruncateTableProcedure {
            data: TruncateTableData {
                state: TruncateTableState::Prepare,
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
        let data: TruncateTableData =
            serde_json::from_str(json).context(DeserializeProcedureSnafu)?;

        Ok(TruncateTableProcedure {
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

        self.data.state = TruncateTableState::EngineTruncateTable;
        // Assign procedure id to the subprocedure.
        self.data.subprocedure_id = Some(ProcedureId::random());

        Ok(Status::executing(true))
    }

    async fn on_engine_truncate_table(&mut self, ctx: &Context) -> Result<Status> {
        // Safety: subprocedure id is always set in this state.
        let sub_id = self.data.subprocedure_id.unwrap();

        // Query subprocedure state.
        let Some(sub_state) = ctx.provider.procedure_state(sub_id).await? else {
                logging::info!("On engine truncate table {}, subprocedure not found, sub_id: {}",
                    self.data.request.table_name,
                    sub_id,
                );
                // If the subprocedure is not found, we create a new subprocedure with the same id.
                let engine_ctx = EngineContext::default();

                let procedure = self
                    .engine_procedure
                    .truncate_table_procedure(&engine_ctx, self.data.request.clone())
                    .map_err(Error::from_error_ext)?;

                return Ok(Status::Suspended {
                    subprocedures: vec![ProcedureWithId {
                        id: sub_id,
                        procedure,
                    }],
                    persist: true,
                })
        };

        match sub_state {
            ProcedureState::Running | ProcedureState::Retrying { .. } => Ok(Status::Suspended {
                subprocedures: Vec::new(),
                persist: false,
            }),
            ProcedureState::Done => {
                logging::info!(
                    "On engine truncate table {}, done, sub_id: {}",
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

/// Represents each step while truncating a table in the datanode.
#[derive(Debug, Serialize, Deserialize)]
enum TruncateTableState {
    /// Validate request and prepare to drop table.
    Prepare,
    /// Truncate table in the table engine.
    EngineTruncateTable,
}

/// Serializable data of [TruncateTableProcedure].
#[derive(Debug, Serialize, Deserialize)]
struct TruncateTableData {
    /// Current state.
    state: TruncateTableState,
    /// Request to truncate this table.
    request: TruncateTableRequest,
    /// Id of the subprocedure to truncate this table from the engine.
    ///
    /// This id is `Some` while the procedure is in [TruncateTableState::EngineTruncateTable]
    /// state.
    subprocedure_id: Option<ProcedureId>,
}

impl TruncateTableData {
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
    use table::engine::TableEngine;

    use super::*;
    use crate::test_util::TestEnv;

    #[tokio::test]
    async fn test_truncate_table_procedure() {
        let env = TestEnv::new("truncate");
        let table_name = "test_truncate";
        let table_id = env.create_table(table_name).await;

        let request = TruncateTableRequest {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table_name.to_string(),
            table_id,
        };
        let TestEnv {
            dir: _dir,
            table_engine,
            procedure_manager,
            catalog_manager,
        } = env;
        let procedure =
            TruncateTableProcedure::new(request, catalog_manager.clone(), table_engine.clone());
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        let mut watcher = procedure_manager.submit(procedure_with_id).await.unwrap();
        watcher.changed().await.unwrap();

        assert!(catalog_manager
            .table_exist(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, table_name)
            .await
            .unwrap());

        let ctx = EngineContext::default();
        assert!(table_engine.table_exists(&ctx, table_id));
    }

    #[tokio::test]
    async fn test_truncate_not_exists_table() {
        common_telemetry::init_default_ut_logging();
        let TestEnv {
            dir: _,
            table_engine,
            procedure_manager: _,
            catalog_manager,
        } = TestEnv::new("truncate");
        let table_name = "test_truncate";

        let request = TruncateTableRequest {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table_name.to_string(),
            table_id: 0,
        };

        let mut procedure =
            TruncateTableProcedure::new(request, catalog_manager.clone(), table_engine.clone());
        assert!(procedure.on_prepare().await.is_err());
    }
}
