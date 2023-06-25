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

//! Procedure to create an immutable file table.

use async_trait::async_trait;
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{Context, Error, LockKey, Procedure, ProcedureManager, Result, Status};
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use table::engine::{EngineContext, TableEngine, TableReference};
use table::requests::CreateTableRequest;

use crate::engine::immutable::ImmutableFileTableEngine;
use crate::error::TableExistsSnafu;

/// Procedure to create an immutable file table.
pub(crate) struct CreateImmutableFileTable {
    data: CreateTableData,
    engine: ImmutableFileTableEngine,
}

#[async_trait]
impl Procedure for CreateImmutableFileTable {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &Context) -> Result<Status> {
        match self.data.state {
            CreateTableState::Prepare => self.on_prepare(),
            CreateTableState::CreateTable => self.on_create_table().await,
        }
    }

    fn dump(&self) -> Result<String> {
        let json = serde_json::to_string(&self.data).context(ToJsonSnafu)?;
        Ok(json)
    }

    fn lock_key(&self) -> LockKey {
        // We don't need to support multiple region so we only lock region-0.
        let table_ref = self.data.table_ref();
        let key = format!("{table_ref}/region-0");
        LockKey::single(key)
    }
}

impl CreateImmutableFileTable {
    const TYPE_NAME: &str = "file-table-engine:CreateImmutableFileTable";

    pub(crate) fn new(request: CreateTableRequest, engine: ImmutableFileTableEngine) -> Self {
        CreateImmutableFileTable {
            data: CreateTableData {
                state: CreateTableState::Prepare,
                request,
            },
            engine,
        }
    }

    pub(crate) fn register_loader(
        engine: ImmutableFileTableEngine,
        procedure_manager: &dyn ProcedureManager,
    ) {
        procedure_manager
            .register_loader(
                Self::TYPE_NAME,
                Box::new(move |data| {
                    Self::from_json(data, engine.clone()).map(|p| Box::new(p) as _)
                }),
            )
            .unwrap()
    }

    fn from_json(json: &str, engine: ImmutableFileTableEngine) -> Result<Self> {
        let data: CreateTableData = serde_json::from_str(json).context(FromJsonSnafu)?;

        Ok(CreateImmutableFileTable { data, engine })
    }

    fn on_prepare(&mut self) -> Result<Status> {
        let engine_ctx = EngineContext::default();
        // Safety: Current get_table implementation always returns Ok.
        if self.engine.table_exists(&engine_ctx, self.data.request.id) {
            // The table already exists.
            ensure!(
                self.data.request.create_if_not_exists,
                TableExistsSnafu {
                    table_name: self.data.table_ref().to_string(),
                }
            );

            return Ok(Status::Done);
        }

        self.data.state = CreateTableState::CreateTable;

        Ok(Status::executing(true))
    }

    async fn on_create_table(&mut self) -> Result<Status> {
        let engine_ctx = EngineContext::default();
        if self.engine.table_exists(&engine_ctx, self.data.request.id) {
            // Table already created. We don't need to check create_if_not_exists as
            // we have checked it in prepare state.
            return Ok(Status::Done);
        }

        let _ = self
            .engine
            .create_table(&engine_ctx, self.data.request.clone())
            .await
            .map_err(Error::from_error_ext)?;

        Ok(Status::Done)
    }
}

/// Represents each step while creating table in the immutable file engine.
#[derive(Debug, Serialize, Deserialize)]
enum CreateTableState {
    /// Prepare to create table.
    Prepare,
    /// Create table.
    CreateTable,
}

/// Serializable data of [CreateImmutableFileTable].
#[derive(Debug, Serialize, Deserialize)]
struct CreateTableData {
    state: CreateTableState,
    request: CreateTableRequest,
}

impl CreateTableData {
    fn table_ref(&self) -> TableReference {
        self.request.table_ref()
    }
}
