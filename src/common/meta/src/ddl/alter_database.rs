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

use std::collections::HashMap;

use api::v1::alter_database_expr::Kind;
use async_trait::async_trait;
use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{Context as ProcedureContext, LockKey, Procedure, Status};
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use strum::AsRefStr;

use super::utils::handle_retry_error;
use crate::ddl::DdlContext;
use crate::error::{Result, SchemaNotFoundSnafu};
use crate::key::schema_name::SchemaNameKey;
use crate::lock_key::{CatalogLock, SchemaLock};
use crate::rpc::ddl::AlterDatabaseTask;
use crate::ClusterId;

pub struct AlterDatabaseProcedure {
    pub context: DdlContext,
    pub data: AlterDatabaseData,
}

impl AlterDatabaseProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::AlterDatabase";

    pub fn new(cluster_id: ClusterId, task: AlterDatabaseTask, context: DdlContext) -> Self {
        Self {
            context,
            data: AlterDatabaseData {
                state: AlterDatabaseState::Prepare,
                cluster_id,
                task,
            },
        }
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;

        Ok(Self { context, data })
    }

    pub async fn on_prepare(&mut self) -> Result<Status> {
        let exists = self
            .context
            .table_metadata_manager
            .schema_manager()
            .exists(SchemaNameKey::new(self.data.catalog(), self.data.schema()))
            .await?;

        ensure!(
            exists,
            SchemaNotFoundSnafu {
                table_schema: self.data.schema(),
            }
        );

        self.data.task.validate()?;
        self.data.state = AlterDatabaseState::UpdateMetadata;

        Ok(Status::executing(true))
    }

    pub async fn on_update_metadata(&mut self) -> Result<Status> {
        let schema_name = SchemaNameKey::new(self.data.catalog(), self.data.schema());
        // Safety: Validated in on_prepare
        let alter_kind = self.data.task.alter_expr.kind.as_ref().unwrap();
        match alter_kind {
            Kind::SetDatabaseOptions(options) => {
                let option_map = options
                    .set_database_options
                    .iter()
                    .map(|option| (option.key.clone(), option.value.clone()))
                    .collect::<HashMap<String, String>>();
                self.validate_options(&option_map)?;
                let schema_value = (&option_map).try_into()?;
                self.context
                    .table_metadata_manager
                    .schema_manager()
                    .update(schema_name, schema_value)
                    .await?;
            }
            Kind::UnsetDatabaseOptions(options) => {
                let option_map = options
                    .keys
                    .iter()
                    .map(|option| (option.clone(), "".to_string()))
                    .collect::<HashMap<String, String>>();
                self.validate_options(&option_map)?;
                let schema_value = (&option_map).try_into()?;
                self.context
                    .table_metadata_manager
                    .schema_manager()
                    .update(schema_name, schema_value)
                    .await?;
            }
        };
        Ok(Status::done())
    }

    fn validate_options(&self, _options: &HashMap<String, String>) -> Result<()> {
        todo!()
    }
}

#[async_trait]
impl Procedure for AlterDatabaseProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        match self.data.state {
            AlterDatabaseState::Prepare => self.on_prepare().await,
            AlterDatabaseState::UpdateMetadata => self.on_update_metadata().await,
        }
        .map_err(handle_retry_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let catalog = self.data.catalog();
        let schema = self.data.schema();

        let lock_key = vec![
            CatalogLock::Read(catalog).into(),
            SchemaLock::write(catalog, schema).into(),
        ];

        LockKey::new(lock_key)
    }
}

#[derive(Debug, Serialize, Deserialize, AsRefStr)]
enum AlterDatabaseState {
    Prepare,
    UpdateMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AlterDatabaseData {
    cluster_id: ClusterId,
    state: AlterDatabaseState,
    task: AlterDatabaseTask,
}

impl AlterDatabaseData {
    pub fn new(task: AlterDatabaseTask, cluster_id: ClusterId) -> Self {
        Self {
            cluster_id,
            state: AlterDatabaseState::Prepare,
            task,
        }
    }

    pub fn catalog(&self) -> &str {
        self.task.catalog()
    }

    pub fn schema(&self) -> &str {
        self.task.schema()
    }
}
