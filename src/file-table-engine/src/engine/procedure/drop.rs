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

//! Procedure to drop an immutable file table.

use async_trait::async_trait;
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{Context, Error, LockKey, Procedure, ProcedureManager, Result, Status};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use table::engine::{EngineContext, TableEngine, TableReference};
use table::requests::DropTableRequest;

use crate::engine::immutable::ImmutableFileTableEngine;

/// Procedure to drop an immutable file table.
pub(crate) struct DropImmutableFileTable {
    data: DropTableData,
    engine: ImmutableFileTableEngine,
}

#[async_trait]
impl Procedure for DropImmutableFileTable {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &Context) -> Result<Status> {
        // To simplify the implementation, we skip prepare phase and drop
        // the table directly.
        let engine_ctx = EngineContext::default();
        // Currently, `drop_table()` of ImmutableFileTableEngine is idempotent so we just
        // invoke it.
        let _ = self
            .engine
            .drop_table(&engine_ctx, self.data.request.clone())
            .await
            .map_err(Error::from_error_ext)?;

        Ok(Status::Done)
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

impl DropImmutableFileTable {
    const TYPE_NAME: &str = "file-table-engine:DropImmutableFileTable";

    pub(crate) fn new(request: DropTableRequest, engine: ImmutableFileTableEngine) -> Self {
        DropImmutableFileTable {
            data: DropTableData { request },
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
        let data: DropTableData = serde_json::from_str(json).context(FromJsonSnafu)?;

        Ok(DropImmutableFileTable { data, engine })
    }
}

/// Serializable data of [DropImmutableFileTable].
#[derive(Debug, Serialize, Deserialize)]
struct DropTableData {
    request: DropTableRequest,
}

impl DropTableData {
    fn table_ref(&self) -> TableReference {
        self.request.table_ref()
    }
}
