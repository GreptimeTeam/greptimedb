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

//! Procedure to drop a database.

use async_trait::async_trait;
use catalog::{CatalogManagerRef, DeregisterSchemaRequest};
use common_procedure::error::SubprocedureFailedSnafu;
use common_procedure::{
    Context, Error, LockKey, Procedure, ProcedureId, ProcedureManager, ProcedureState,
    ProcedureWithId, Result, Status,
};
use common_telemetry::logging;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use table::engine::{EngineContext, TableEngineProcedureRef};
use table::requests::{DropDatabaseRequest, DropTableRequest};

use crate::error::{
    AccessCatalogSnafu, DeserializeProcedureSnafu, SchemaNotFoundSnafu, SerializeProcedureSnafu,
};

/// Procedure to drop a database.
pub struct DropDatabaseProcedure {
    data: DropDatabaseData,
    catalog_manager: CatalogManagerRef,
    engine_procedure: TableEngineProcedureRef,
}

#[async_trait]
impl Procedure for DropDatabaseProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, ctx: &Context) -> Result<Status> {
        match self.data.state {
            DropDatabaseState::Prepare => self.on_prepare().await,
            DropDatabaseState::EngineDropAllTables => self.on_engine_drop_all_tables(ctx).await,
            DropDatabaseState::RemoveFromCatalog => self.on_remove_from_catalog().await,
        }
    }

    fn dump(&self) -> Result<String> {
        let json = serde_json::to_string(&self.data).context(SerializeProcedureSnafu)?;
        Ok(json)
    }

    fn lock_key(&self) -> LockKey {
        LockKey::new(
            self.data
                .request
                .table_names
                .iter()
                .map(|(name, _)| name.clone())
                .collect::<Vec<_>>(),
        )
    }
}

#[allow(unused)]
impl DropDatabaseProcedure {
    const TYPE_NAME: &str = "table-procedure::DropDatabaseProcedure";

    /// Returns a new [DropDatabaseProcedure].
    pub fn new(
        request: DropDatabaseRequest,
        catalog_manager: CatalogManagerRef,
        engine_procedure: TableEngineProcedureRef,
    ) -> DropDatabaseProcedure {
        DropDatabaseProcedure {
            data: DropDatabaseData {
                state: DropDatabaseState::Prepare,
                request,
                subprocedure_ids: None,
                now_subprocedure_id: 0,
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
        let data: DropDatabaseData =
            serde_json::from_str(json).context(DeserializeProcedureSnafu)?;

        Ok(DropDatabaseProcedure {
            data,
            catalog_manager,
            engine_procedure,
        })
    }

    async fn on_prepare(&mut self) -> Result<Status> {
        let request = &self.data.request;
        // Ensure the schema exists.
        let schema_exists = self
            .catalog_manager
            .schema_exist(&request.catalog_name, &request.schema_name)
            .await
            .context(AccessCatalogSnafu)?;
        ensure!(
            schema_exists,
            SchemaNotFoundSnafu {
                name: &request.schema_name,
            }
        );
        self.data.state = DropDatabaseState::EngineDropAllTables;
        // Assign all procedure ids to each subprocedures.
        self.data.subprocedure_ids = Some(
            (0..self.data.request.table_names.len())
                .map(|_| ProcedureId::random())
                .collect(),
        );
        Ok(Status::executing(true))
    }

    async fn on_remove_from_catalog(&mut self) -> Result<Status> {
        let request = &self.data.request;
        let has_schema = self
            .catalog_manager
            .schema_exist(&request.catalog_name, &request.schema_name)
            .await
            .context(AccessCatalogSnafu)?;
        if has_schema {
            // The schema is still in the catalog.
            let deregister_schema_req = DeregisterSchemaRequest {
                catalog: self.data.request.catalog_name.clone(),
                schema: self.data.request.schema_name.clone(),
            };
            self.catalog_manager
                .deregister_schema(deregister_schema_req)
                .await
                .context(AccessCatalogSnafu)?;
        }
        Ok(Status::Done)
    }

    async fn on_engine_drop_all_tables(&mut self, ctx: &Context) -> Result<Status> {
        // Safety: subprocedure id is always set in this state.
        let sub_ids = self.data.subprocedure_ids.as_ref().unwrap();
        let sub_id = sub_ids[self.data.now_subprocedure_id];

        // Query subprocedure state.
        let Some(sub_state) = ctx.provider.procedure_state(sub_id).await? else {
            logging::info!(
                "On engine drop all tables from {}, subprocedure not found, sub_id: {}",
                self.data.request.schema_name,
                sub_id
            );
            // If the subprocedure is not found, we create a new subprocedure with the same id.
            let engine_ctx = EngineContext::default();
            let request = DropTableRequest {
                catalog_name: self.data.request.catalog_name.to_string(),
                schema_name:  self.data.request.schema_name.to_string(),
                table_name: self.data.request.table_names[self.data.now_subprocedure_id].0.clone(),
                table_id: self.data.request.table_names[self.data.now_subprocedure_id].1,
            };
            let procedure = self
                .engine_procedure
                .drop_table_procedure(&engine_ctx, request)
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
                    "On engine drop all tables from {}, done, sub_id: {}",
                    self.data.request.schema_name,
                    sub_id
                );
                self.data.now_subprocedure_id += 1;
                if self.data.subprocedure_ids.as_ref().unwrap().len()
                    == self.data.now_subprocedure_id
                {
                    self.data.state = DropDatabaseState::RemoveFromCatalog;
                    Ok(Status::executing(true))
                } else {
                    Ok(Status::Suspended {
                        subprocedures: Vec::new(),
                        persist: false,
                    })
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
}

/// Represents each step while dropping a table in the datanode.
#[derive(Debug, Serialize, Deserialize)]
enum DropDatabaseState {
    /// Validate request and prepare to drop table.
    Prepare,
    /// Drop table in the table engine.
    EngineDropAllTables,
    /// Remove the table from the catalog.
    RemoveFromCatalog,
}

/// Serializable data of [DropDatabaseProcedure].
#[derive(Debug, Serialize, Deserialize)]
struct DropDatabaseData {
    /// Current state.
    state: DropDatabaseState,
    /// Request to drop this database.
    request: DropDatabaseRequest,
    /// Ids of the subprocedures corresponding to each table dropped from the database.
    ///
    /// This ids is `Some` while the procedure is in [DropDatabaseState::EngineDropTable]
    /// state.
    subprocedure_ids: Option<Vec<ProcedureId>>,
    now_subprocedure_id: usize,
}

#[cfg(test)]
mod tests {
    use catalog::RegisterSchemaRequest;
    use table::engine::TableEngine;

    use super::*;
    use crate::test_util::TestEnv;

    #[tokio::test]
    async fn test_drop_database_procedure() {
        let mut env = TestEnv::new("drop");
        let table_name1 = "test_drop1";
        let table_id1 = env.create_table(table_name1).await;
        let table_name2 = "test_drop2";
        let table_id2 = env.create_table(table_name2).await;
        assert_ne!(table_id1, table_id2);

        let TestEnv {
            dir: _dir,
            table_engine,
            procedure_manager,
            catalog_manager,
            ..
        } = env;
        let catalog_name = "foo_catalog".to_string();
        let schema_name = "foo_schema".to_string();

        let request = RegisterSchemaRequest {
            catalog: catalog_name.clone(),
            schema: schema_name.clone(),
        };
        catalog_manager
            .register_catalog(catalog_name.clone())
            .await
            .unwrap();
        catalog_manager.register_schema(request).await.unwrap();

        let request = DropDatabaseRequest {
            catalog_name: catalog_name.clone(),
            schema_name: schema_name.clone(),
            table_names: vec![
                (table_name1.to_string(), table_id1),
                (table_name2.to_string(), table_id2),
            ],
        };
        let procedure =
            DropDatabaseProcedure::new(request, catalog_manager.clone(), table_engine.clone());
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        let mut watcher = procedure_manager.submit(procedure_with_id).await.unwrap();
        watcher.changed().await.unwrap();

        assert!(!catalog_manager
            .schema_exist(&catalog_name, &schema_name)
            .await
            .unwrap());
        let ctx = EngineContext::default();
        assert!(!table_engine.table_exists(&ctx, table_id1,));
        assert!(!table_engine.table_exists(&ctx, table_id2,));
    }
}
