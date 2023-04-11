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

//! Procedure to create a table.

use async_trait::async_trait;
use catalog::{CatalogManagerRef, RegisterTableRequest};
use common_procedure::{
    Context, Error, LockKey, Procedure, ProcedureId, ProcedureManager, ProcedureState,
    ProcedureWithId, Result, Status,
};
use common_telemetry::logging;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use table::engine::{EngineContext, TableEngineProcedureRef, TableEngineRef, TableReference};
use table::requests::CreateTableRequest;

use crate::error::{
    AccessCatalogSnafu, CatalogNotFoundSnafu, DeserializeProcedureSnafu, SchemaNotFoundSnafu,
    SerializeProcedureSnafu, SubprocedureFailedSnafu,
};

/// Procedure to create a table.
pub struct CreateTableProcedure {
    data: CreateTableData,
    catalog_manager: CatalogManagerRef,
    table_engine: TableEngineRef,
    engine_procedure: TableEngineProcedureRef,
}

#[async_trait]
impl Procedure for CreateTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, ctx: &Context) -> Result<Status> {
        match self.data.state {
            CreateTableState::Prepare => self.on_prepare(),
            CreateTableState::EngineCreateTable => self.on_engine_create_table(ctx).await,
            CreateTableState::RegisterCatalog => self.on_register_catalog().await,
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

impl CreateTableProcedure {
    const TYPE_NAME: &str = "table-procedures::CreateTableProcedure";

    /// Returns a new [CreateTableProcedure].
    pub fn new(
        request: CreateTableRequest,
        catalog_manager: CatalogManagerRef,
        table_engine: TableEngineRef,
        engine_procedure: TableEngineProcedureRef,
    ) -> CreateTableProcedure {
        CreateTableProcedure {
            data: CreateTableData {
                state: CreateTableState::Prepare,
                request,
                subprocedure_id: None,
            },
            catalog_manager,
            table_engine,
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
        table_engine: TableEngineRef,
        procedure_manager: &dyn ProcedureManager,
    ) {
        procedure_manager
            .register_loader(
                Self::TYPE_NAME,
                Box::new(move |data| {
                    Self::from_json(
                        data,
                        catalog_manager.clone(),
                        table_engine.clone(),
                        engine_procedure.clone(),
                    )
                    .map(|p| Box::new(p) as _)
                }),
            )
            .unwrap()
    }

    /// Recover the procedure from json.
    fn from_json(
        json: &str,
        catalog_manager: CatalogManagerRef,
        table_engine: TableEngineRef,
        engine_procedure: TableEngineProcedureRef,
    ) -> Result<Self> {
        let data: CreateTableData =
            serde_json::from_str(json).context(DeserializeProcedureSnafu)?;

        Ok(CreateTableProcedure {
            data,
            catalog_manager,
            table_engine,
            engine_procedure,
        })
    }

    fn on_prepare(&mut self) -> Result<Status> {
        // Check whether catalog and schema exist.
        let catalog = self
            .catalog_manager
            .catalog(&self.data.request.catalog_name)
            .context(AccessCatalogSnafu)?
            .with_context(|| {
                logging::error!(
                    "Failed to create table {}, catalog not found",
                    self.data.table_ref()
                );
                CatalogNotFoundSnafu {
                    name: &self.data.request.catalog_name,
                }
            })?;
        catalog
            .schema(&self.data.request.schema_name)
            .context(AccessCatalogSnafu)?
            .with_context(|| {
                logging::error!(
                    "Failed to create table {}, schema not found",
                    self.data.table_ref(),
                );
                SchemaNotFoundSnafu {
                    name: &self.data.request.schema_name,
                }
            })?;

        self.data.state = CreateTableState::EngineCreateTable;
        // Assign procedure id to the subprocedure.
        self.data.subprocedure_id = Some(ProcedureId::random());

        Ok(Status::executing(true))
    }

    async fn on_engine_create_table(&mut self, ctx: &Context) -> Result<Status> {
        // Safety: subprocedure id is always set in this state.
        let sub_id = self.data.subprocedure_id.unwrap();

        // Query subprocedure state.
        let Some(sub_state) = ctx.provider.procedure_state(sub_id).await? else {
            // We need to submit the subprocedure if it doesn't exist. We always need to
            // do this check as we might not submitted the subprocedure yet when the manager
            // recover this procedure from procedure store.
            logging::info!(
                "On engine create table {}, not found, sub_id: {}",
                self.data.request.table_name,
                sub_id
            );

            // If the sub procedure is not found, we create a new sub procedure with the same id.
            let engine_ctx = EngineContext::default();
            let procedure = self
                .engine_procedure
                .create_table_procedure(&engine_ctx, self.data.request.clone())
                .map_err(Error::external)?;
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
                    "On engine create table {}, done, sub_id: {}",
                    self.data.request.table_name,
                    sub_id
                );
                // The sub procedure is done, we can execute next step.
                self.data.state = CreateTableState::RegisterCatalog;
                Ok(Status::executing(true))
            }
            ProcedureState::Failed { .. } => {
                // Return error if the subprocedure is failed.
                SubprocedureFailedSnafu {
                    subprocedure_id: sub_id,
                }
                .fail()?
            }
        }
    }

    async fn on_register_catalog(&mut self) -> Result<Status> {
        let catalog = self
            .catalog_manager
            .catalog(&self.data.request.catalog_name)
            .context(AccessCatalogSnafu)?
            .context(CatalogNotFoundSnafu {
                name: &self.data.request.catalog_name,
            })?;
        let schema = catalog
            .schema(&self.data.request.schema_name)
            .context(AccessCatalogSnafu)?
            .context(SchemaNotFoundSnafu {
                name: &self.data.request.schema_name,
            })?;
        let table_exists = schema
            .table(&self.data.request.table_name)
            .await
            .map_err(Error::external)?
            .is_some();
        if table_exists {
            // Table already exists.
            return Ok(Status::Done);
        }

        let engine_ctx = EngineContext::default();
        let table_ref = self.data.table_ref();
        // Safety: The procedure owns the lock so the table should exist.
        let table = self
            .table_engine
            .get_table(&engine_ctx, &table_ref)
            .map_err(Error::external)?
            .unwrap();

        let register_req = RegisterTableRequest {
            catalog: self.data.request.catalog_name.clone(),
            schema: self.data.request.schema_name.clone(),
            table_name: self.data.request.table_name.clone(),
            table_id: self.data.request.id,
            table,
        };
        self.catalog_manager
            .register_table(register_req)
            .await
            .map_err(Error::external)?;

        Ok(Status::Done)
    }
}

/// Represents each step while creating a table in the datanode.
#[derive(Debug, Serialize, Deserialize)]
enum CreateTableState {
    /// Validate request and prepare to create table.
    Prepare,
    /// Create table in the table engine.
    EngineCreateTable,
    /// Register the table to the catalog.
    RegisterCatalog,
}

/// Serializable data of [CreateTableProcedure].
#[derive(Debug, Serialize, Deserialize)]
struct CreateTableData {
    /// Current state.
    state: CreateTableState,
    /// Request to create this table.
    request: CreateTableRequest,
    /// Id of the subprocedure to create this table in the engine.
    ///
    /// This id is `Some` while the procedure is in [CreateTableState::EngineCreateTable]
    /// state.
    subprocedure_id: Option<ProcedureId>,
}

impl CreateTableData {
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
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, RawSchema};
    use mito::engine::MITO_ENGINE;
    use table::engine::{EngineContext, TableEngine};

    use super::*;
    use crate::test_util::TestEnv;

    fn schema_for_test() -> RawSchema {
        let column_schemas = vec![
            // Key
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
            // Nullable value column: cpu
            ColumnSchema::new("cpu", ConcreteDataType::float64_datatype(), true),
            // Non-null value column: memory
            ColumnSchema::new("memory", ConcreteDataType::float64_datatype(), false),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                true,
            )
            .with_time_index(true),
        ];

        RawSchema::new(column_schemas)
    }

    fn new_create_request(table_name: &str) -> CreateTableRequest {
        CreateTableRequest {
            id: 1,
            catalog_name: "greptime".to_string(),
            schema_name: "public".to_string(),
            table_name: table_name.to_string(),
            desc: Some("a test table".to_string()),
            schema: schema_for_test(),
            region_numbers: vec![0, 1],
            create_if_not_exists: true,
            primary_key_indices: vec![0],
            table_options: Default::default(),
            engine: MITO_ENGINE.to_string(),
        }
    }

    #[tokio::test]
    async fn test_create_table_procedure() {
        let TestEnv {
            dir: _dir,
            table_engine,
            procedure_manager,
            catalog_manager,
        } = TestEnv::new("create");

        let table_name = "test_create";
        let request = new_create_request(table_name);
        let procedure = CreateTableProcedure::new(
            request.clone(),
            catalog_manager,
            table_engine.clone(),
            table_engine.clone(),
        );

        let table_ref = TableReference {
            catalog: &request.catalog_name,
            schema: &request.schema_name,
            table: &request.table_name,
        };
        let engine_ctx = EngineContext::default();
        assert!(table_engine
            .get_table(&engine_ctx, &table_ref)
            .unwrap()
            .is_none());

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
        let mut watcher = procedure_manager.submit(procedure_with_id).await.unwrap();
        watcher.changed().await.unwrap();

        assert!(table_engine
            .get_table(&engine_ctx, &table_ref)
            .unwrap()
            .is_some());
    }
}
