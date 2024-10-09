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

use async_trait::async_trait;
use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{Context, LockKey, Procedure, Status};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use super::utils::handle_retry_error;
use crate::ddl::DdlContext;
use crate::error::Result;
use crate::lock_key::{CatalogLock, SchemaLock};
use crate::ClusterId;

pub struct AlterDatabaseProcedure {
    _context: DdlContext,
    data: AlterDatabaseData,
}

impl AlterDatabaseProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::AlterDatabase";

    pub fn new(
        _cluster_id: ClusterId,
        context: DdlContext,
        catalog: String,
        schema: String,
    ) -> Self {
        AlterDatabaseProcedure {
            _context: context,
            data: AlterDatabaseData {
                state: AlterDatabaseState::Prepare,
                catalog,
                schema,
            },
        }
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;

        Ok(Self {
            _context: context,
            data,
        })
    }

    async fn on_prepare(&mut self) -> Result<Status> {
        todo!();
    }

    async fn on_update_metadata(&mut self) -> Result<Status> {
        todo!();
    }
}

#[async_trait]
impl Procedure for AlterDatabaseProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }
    async fn execute(&mut self, _ctx: &Context) -> ProcedureResult<Status> {
        let state = &self.data.state;
        match state {
            AlterDatabaseState::Prepare => self.on_prepare().await,
            AlterDatabaseState::UpdateMetadata => self.on_update_metadata().await,
        }
        .map_err(handle_retry_error)
    }
    fn lock_key(&self) -> LockKey {
        let lock_key = vec![
            CatalogLock::Read(&self.data.catalog).into(),
            SchemaLock::write(&self.data.catalog, &self.data.schema).into(),
        ];
        LockKey::new(lock_key)
    }
    fn dump(&self) -> common_procedure::Result<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum AlterDatabaseState {
    Prepare,
    UpdateMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
struct AlterDatabaseData {
    state: AlterDatabaseState,
    catalog: String,
    schema: String,
}
