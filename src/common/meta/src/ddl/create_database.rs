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

use common_catalog::build_db_string;
use common_procedure::error::ToJsonSnafu;
use common_procedure::{
    Context as ProcedureContext, LockKey, Procedure, Result as ProcedureResult, Status,
};
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use strum::AsRefStr;

use super::utils::handle_retry_error;
use super::DdlContext;
use crate::error::{self, Result};
use crate::key::schema_name::{SchemaManager, SchemaNameKey};
use crate::metrics;
use crate::rpc::ddl::CreateDatabaseTask;

/// Create database procedure
pub struct CreateDatabaseProcedure {
    pub context: DdlContext,
    pub data: CreateDatabaseData,
}

impl CreateDatabaseProcedure {
    pub(crate) const TYPE_NAME: &'static str = "metasrv-procedure::CreateDatabase";

    pub fn new(cluster_id: u64, task: CreateDatabaseTask, context: DdlContext) -> Self {
        Self {
            context,
            data: CreateDatabaseData {
                state: CreateDatabaseState::Prepare,
                cluster_id,
                task,
            },
        }
    }

    /// Check if the database already exists
    async fn on_prepare(&mut self) -> Result<Status> {
        let catalog = &self.data.task.catalog;
        let database_name = &self.data.task.database_name;
        let schema_key = SchemaNameKey::new(catalog, database_name);

        let schema_manger = self.context.table_metadata_manager.schema_manager();
        let exists = schema_manger.exists(schema_key).await?;

        if exists {
            ensure!(
                self.data.task.create_if_not_exists,
                error::DatabaseAlreadyExistsSnafu {
                    database: database_name.to_string(),
                }
            );

            return Ok(Status::Done);
        }

        self.data.state = CreateDatabaseState::CreateMetadata;

        Ok(Status::executing(true))
    }

    /// Using schema manager to create a database
    async fn on_create_database(&mut self) -> Result<Status> {
        let catalog = &self.data.task.catalog;
        let database_name = &self.data.task.database_name;
        let schema_key = SchemaNameKey::new(catalog, database_name);

        self.schema_manager()
            .create(schema_key, None, false)
            .await?;

        Ok(Status::Done)
    }

    /// Get schema manager
    fn schema_manager(&self) -> &SchemaManager {
        self.context.table_metadata_manager.schema_manager()
    }
}

#[async_trait::async_trait]
impl Procedure for CreateDatabaseProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    /// Execute the procedure:
    /// step 1: check if the database already exists.
    /// step 2: create database in schema manager.
    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &self.data.state;

        let _timer = metrics::METRIC_META_PROCEDURE_CREATE_DATABASE
            .with_label_values(&[state.as_ref()])
            .start_timer();

        match state {
            CreateDatabaseState::Prepare => self.on_prepare().await,
            CreateDatabaseState::CreateMetadata => self.on_create_database().await,
        }
        .map_err(handle_retry_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let catalog = &self.data.task.catalog;
        let schema = &self.data.task.database_name;
        let db_name = build_db_string(catalog, schema);

        LockKey::single(db_name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, AsRefStr)]
pub enum CreateDatabaseState {
    /// Prepares to create the table
    Prepare,
    /// Creates metadata
    CreateMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateDatabaseData {
    pub cluster_id: u64,
    pub task: CreateDatabaseTask,
    pub state: CreateDatabaseState,
}
