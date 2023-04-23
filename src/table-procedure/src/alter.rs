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

//! Procedure to alter a table.

use async_trait::async_trait;
use catalog::{CatalogManagerRef, RenameTableRequest};
use common_procedure::{
    Context, Error, LockKey, Procedure, ProcedureId, ProcedureManager, ProcedureState,
    ProcedureWithId, Result, Status,
};
use common_telemetry::logging;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use table::engine::{EngineContext, TableEngineProcedureRef, TableReference};
use table::metadata::TableId;
use table::requests::{AlterKind, AlterTableRequest};

use crate::error::{
    AccessCatalogSnafu, CatalogNotFoundSnafu, DeserializeProcedureSnafu, SchemaNotFoundSnafu,
    SerializeProcedureSnafu, SubprocedureFailedSnafu, TableExistsSnafu, TableNotFoundSnafu,
};

/// Procedure to alter a table.
pub struct AlterTableProcedure {
    data: AlterTableData,
    catalog_manager: CatalogManagerRef,
    engine_procedure: TableEngineProcedureRef,
}

#[async_trait]
impl Procedure for AlterTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, ctx: &Context) -> Result<Status> {
        match self.data.state {
            AlterTableState::Prepare => self.on_prepare().await,
            AlterTableState::EngineAlterTable => self.on_engine_alter_table(ctx).await,
            AlterTableState::RenameInCatalog => self.on_rename_in_catalog().await,
        }
    }

    fn dump(&self) -> Result<String> {
        let json = serde_json::to_string(&self.data).context(SerializeProcedureSnafu)?;
        Ok(json)
    }

    fn lock_key(&self) -> LockKey {
        // We lock the whole table.
        let table_name = self.data.table_ref().to_string();
        // If alter kind is rename, we also need to lock the renamed table.
        if let AlterKind::RenameTable { new_table_name } = &self.data.request.alter_kind {
            let new_table_name = TableReference {
                catalog: &self.data.request.catalog_name,
                schema: &self.data.request.schema_name,
                table: new_table_name,
            }
            .to_string();
            LockKey::new([table_name, new_table_name])
        } else {
            LockKey::single(table_name)
        }
    }
}

impl AlterTableProcedure {
    const TYPE_NAME: &str = "table-procedure:AlterTableProcedure";

    /// Returns a new [AlterTableProcedure].
    pub fn new(
        request: AlterTableRequest,
        catalog_manager: CatalogManagerRef,
        engine_procedure: TableEngineProcedureRef,
    ) -> AlterTableProcedure {
        AlterTableProcedure {
            data: AlterTableData {
                state: AlterTableState::Prepare,
                request,
                table_id: None,
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
        let data: AlterTableData = serde_json::from_str(json).context(DeserializeProcedureSnafu)?;

        Ok(AlterTableProcedure {
            data,
            catalog_manager,
            engine_procedure,
        })
    }

    async fn on_prepare(&mut self) -> Result<Status> {
        // Check whether catalog and schema exist.
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

        let table = schema
            .table(&self.data.request.table_name)
            .await
            .context(AccessCatalogSnafu)?
            .context(TableNotFoundSnafu {
                name: &self.data.request.table_name,
            })?;
        if let AlterKind::RenameTable { new_table_name } = &self.data.request.alter_kind {
            ensure!(
                !schema
                    .table_exist(new_table_name)
                    .context(AccessCatalogSnafu)?,
                TableExistsSnafu {
                    name: new_table_name,
                }
            );
        }

        self.data.state = AlterTableState::EngineAlterTable;
        // Assign procedure id to the subprocedure.
        self.data.subprocedure_id = Some(ProcedureId::random());
        // Set the table id.
        self.data.table_id = Some(table.table_info().ident.table_id);

        Ok(Status::executing(true))
    }

    async fn on_engine_alter_table(&mut self, ctx: &Context) -> Result<Status> {
        // Safety: subprocedure id is always set in this state.
        let sub_id = self.data.subprocedure_id.unwrap();

        // Query subprocedure state.
        let Some(sub_state) = ctx.provider.procedure_state(sub_id).await? else {
            logging::info!(
                "On engine alter table {}, subprocedure not found, sub_id: {}",
                self.data.request.table_name,
                sub_id
            );

            // If the subprocedure is not found, we create a new subprocedure with the same id.
            let engine_ctx = EngineContext::default();
            let procedure = self
                .engine_procedure
                .alter_table_procedure(&engine_ctx, self.data.request.clone())
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
                    "On engine alter table {}, done, sub_id: {}",
                    self.data.request.table_name,
                    sub_id
                );
                // The sub procedure is done, we can execute next step.
                if self.data.request.is_rename_table() {
                    // We also need to rename the table in the catalog.
                    self.data.state = AlterTableState::RenameInCatalog;
                    Ok(Status::executing(true))
                } else {
                    // If this isn't a rename operation, we are done.
                    Ok(Status::Done)
                }
            }
            ProcedureState::Failed { error } => {
                // Return error if the subprocedure is failed.
                Err(error).context(SubprocedureFailedSnafu {
                    subprocedure_id: sub_id,
                })?
            }
        }
    }

    async fn on_rename_in_catalog(&mut self) -> Result<Status> {
        // Safety: table id is available in this state.
        let table_id = self.data.table_id.unwrap();
        if let AlterKind::RenameTable { new_table_name } = &self.data.request.alter_kind {
            let rename_req = RenameTableRequest {
                catalog: self.data.request.catalog_name.clone(),
                schema: self.data.request.schema_name.clone(),
                table_name: self.data.request.table_name.clone(),
                new_table_name: new_table_name.clone(),
                table_id,
            };

            self.catalog_manager
                .rename_table(rename_req)
                .await
                .map_err(Error::from_error_ext)?;
        }

        Ok(Status::Done)
    }
}

/// Represents each step while altering a table in the datanode.
#[derive(Debug, Serialize, Deserialize)]
enum AlterTableState {
    /// Validate request and prepare to alter table.
    Prepare,
    /// Alter table in the table engine.
    EngineAlterTable,
    /// Rename the table in the catalog (optional).
    RenameInCatalog,
}

/// Serializable data of [AlterTableProcedure].
#[derive(Debug, Serialize, Deserialize)]
struct AlterTableData {
    /// Current state.
    state: AlterTableState,
    /// Request to alter this table.
    request: AlterTableRequest,
    /// Id of the table.
    ///
    /// Available after [AlterTableState::Prepare] state.
    table_id: Option<TableId>,
    /// Id of the subprocedure to alter this table in the engine.
    ///
    /// This id is `Some` while the procedure is in [AlterTableState::EngineAlterTable]
    /// state.
    subprocedure_id: Option<ProcedureId>,
}

impl AlterTableData {
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
    async fn test_alter_table_procedure_rename() {
        let env = TestEnv::new("rename");
        let table_name = "test_old";
        env.create_table(table_name).await;

        let new_table_name = "test_new";
        let request = AlterTableRequest {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table_name.to_string(),
            alter_kind: AlterKind::RenameTable {
                new_table_name: new_table_name.to_string(),
            },
        };

        let TestEnv {
            dir: _dir,
            table_engine,
            procedure_manager,
            catalog_manager,
        } = env;
        let procedure =
            AlterTableProcedure::new(request, catalog_manager.clone(), table_engine.clone());
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        let mut watcher = procedure_manager.submit(procedure_with_id).await.unwrap();
        watcher.changed().await.unwrap();

        let catalog = catalog_manager
            .catalog(DEFAULT_CATALOG_NAME)
            .unwrap()
            .unwrap();
        let schema = catalog.schema(DEFAULT_SCHEMA_NAME).unwrap().unwrap();
        let table = schema.table(new_table_name).await.unwrap().unwrap();
        let table_info = table.table_info();
        assert_eq!(new_table_name, table_info.name);

        assert!(schema.table(table_name).await.unwrap().is_none());
    }
}
