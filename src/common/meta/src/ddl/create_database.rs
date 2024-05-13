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

use async_trait::async_trait;
use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{Context as ProcedureContext, LockKey, Procedure, Status};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DefaultOnNull};
use snafu::{ensure, ResultExt};
use strum::AsRefStr;

use crate::ddl::utils::handle_retry_error;
use crate::ddl::DdlContext;
use crate::error::{self, Result};
use crate::key::schema_name::{SchemaNameKey, SchemaNameValue};
use crate::lock_key::{CatalogLock, SchemaLock};

pub struct CreateDatabaseProcedure {
    pub context: DdlContext,
    pub data: CreateDatabaseData,
}

impl CreateDatabaseProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::CreateDatabase";

    pub fn new(
        catalog: String,
        schema: String,
        create_if_not_exists: bool,
        options: HashMap<String, String>,
        context: DdlContext,
    ) -> Self {
        Self {
            context,
            data: CreateDatabaseData {
                state: CreateDatabaseState::Prepare,
                catalog,
                schema,
                create_if_not_exists,
                options,
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
            .exists(SchemaNameKey::new(&self.data.catalog, &self.data.schema))
            .await?;

        if exists && self.data.create_if_not_exists {
            return Ok(Status::done());
        }

        ensure!(
            !exists,
            error::SchemaAlreadyExistsSnafu {
                catalog: &self.data.catalog,
                schema: &self.data.schema,
            }
        );

        self.data.state = CreateDatabaseState::CreateMetadata;
        Ok(Status::executing(true))
    }

    pub async fn on_create_metadata(&mut self) -> Result<Status> {
        let value: SchemaNameValue = (&self.data.options).try_into()?;

        self.context
            .table_metadata_manager
            .schema_manager()
            .create(
                SchemaNameKey::new(&self.data.catalog, &self.data.schema),
                Some(value),
                self.data.create_if_not_exists,
            )
            .await?;

        Ok(Status::done())
    }
}

#[async_trait]
impl Procedure for CreateDatabaseProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &self.data.state;

        match state {
            CreateDatabaseState::Prepare => self.on_prepare().await,
            CreateDatabaseState::CreateMetadata => self.on_create_metadata().await,
        }
        .map_err(handle_retry_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let lock_key = vec![
            CatalogLock::Read(&self.data.catalog).into(),
            SchemaLock::write(&self.data.catalog, &self.data.schema).into(),
        ];

        LockKey::new(lock_key)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, AsRefStr)]
pub enum CreateDatabaseState {
    Prepare,
    CreateMetadata,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateDatabaseData {
    pub state: CreateDatabaseState,
    pub catalog: String,
    pub schema: String,
    pub create_if_not_exists: bool,
    #[serde_as(deserialize_as = "DefaultOnNull")]
    pub options: HashMap<String, String>,
}
