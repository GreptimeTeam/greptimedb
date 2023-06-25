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

use std::sync::Arc;

use async_trait::async_trait;
use common_procedure::error::{Error, FromJsonSnafu, ToJsonSnafu};
use common_procedure::{Context, LockKey, Procedure, ProcedureManager, Result, Status};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::StorageEngine;
use table::engine::TableReference;
use table::requests::DropTableRequest;
use table::Table;

use crate::engine::MitoEngineInner;
use crate::table::MitoTable;

/// Procedure to drop a [MitoTable].
pub(crate) struct DropMitoTable<S: StorageEngine> {
    data: DropTableData,
    engine_inner: Arc<MitoEngineInner<S>>,
    table: Option<Arc<MitoTable<S::Region>>>,
}

#[async_trait]
impl<S: StorageEngine> Procedure for DropMitoTable<S> {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &Context) -> Result<Status> {
        match self.data.state {
            DropTableState::Prepare => self.on_prepare(),
            DropTableState::EngineDropTable => {
                let _ = self
                    .engine_inner
                    .drop_table(self.data.request.clone())
                    .await
                    .map_err(Error::from_error_ext)?;

                Ok(Status::Done)
            }
        }
    }

    fn dump(&self) -> Result<String> {
        let json = serde_json::to_string(&self.data).context(ToJsonSnafu)?;
        Ok(json)
    }

    fn lock_key(&self) -> LockKey {
        let table_ref = self.data.table_ref();
        let Some(table) = &self.table else { return LockKey::default() };
        let info = table.table_info();
        let keys = info
            .meta
            .region_numbers
            .iter()
            .map(|number| format!("{table_ref}/region-{number}"));
        LockKey::new(keys)
    }
}

impl<S: StorageEngine> DropMitoTable<S> {
    const TYPE_NAME: &str = "mito::DropMitoTable";

    /// Returns a new [DropMitoTable].
    pub(crate) fn new(
        request: DropTableRequest,
        engine_inner: Arc<MitoEngineInner<S>>,
    ) -> Result<Self> {
        let data = DropTableData {
            state: DropTableState::Prepare,
            request,
        };
        let table = engine_inner.get_mito_table(data.request.table_id);

        Ok(DropMitoTable {
            data,
            engine_inner,
            table,
        })
    }

    /// Register the loader of this procedure to the `procedure_manager`.
    ///
    /// # Panics
    /// Panics on error.
    pub(crate) fn register_loader(
        engine_inner: Arc<MitoEngineInner<S>>,
        procedure_manager: &dyn ProcedureManager,
    ) {
        procedure_manager
            .register_loader(
                Self::TYPE_NAME,
                Box::new(move |data| {
                    Self::from_json(data, engine_inner.clone()).map(|p| Box::new(p) as _)
                }),
            )
            .unwrap()
    }

    /// Recover the procedure from json.
    fn from_json(json: &str, engine_inner: Arc<MitoEngineInner<S>>) -> Result<Self> {
        let data: DropTableData = serde_json::from_str(json).context(FromJsonSnafu)?;
        let table = engine_inner.get_mito_table(data.request.table_id);

        Ok(DropMitoTable {
            data,
            engine_inner,
            table,
        })
    }

    /// Prepare table info.
    fn on_prepare(&mut self) -> Result<Status> {
        self.data.state = DropTableState::EngineDropTable;

        Ok(Status::executing(true))
    }
}

/// Represents each step while dropping table in the mito engine.
#[derive(Debug, Serialize, Deserialize)]
enum DropTableState {
    /// Prepare to drop the table.
    Prepare,
    /// Engine drop the table.
    EngineDropTable,
}

/// Serializable data of [DropMitoTable].
#[derive(Debug, Serialize, Deserialize)]
struct DropTableData {
    state: DropTableState,
    request: DropTableRequest,
}

impl DropTableData {
    fn table_ref(&self) -> TableReference {
        self.request.table_ref()
    }
}

#[cfg(test)]
mod tests {
    use table::engine::{EngineContext, TableEngine, TableEngineProcedure};

    use super::*;
    use crate::engine::procedure::procedure_test_util::{self, TestEnv};
    use crate::table::test_util;

    #[tokio::test]
    async fn test_procedure_drop_table() {
        common_telemetry::init_default_ut_logging();

        let TestEnv {
            table_engine,
            dir: _dir,
        } = procedure_test_util::setup_test_engine("add_column").await;
        let schema = Arc::new(test_util::schema_for_test());
        let request = test_util::new_create_request(schema.clone());

        let engine_ctx = EngineContext::default();
        // Create table first.
        let mut procedure = table_engine
            .create_table_procedure(&engine_ctx, request.clone())
            .unwrap();
        procedure_test_util::execute_procedure_until_done(&mut procedure).await;

        // Drop the table.
        let table_id = request.id;
        let request = test_util::new_drop_request();
        let mut procedure = table_engine
            .drop_table_procedure(&engine_ctx, request.clone())
            .unwrap();
        procedure_test_util::execute_procedure_until_done(&mut procedure).await;

        // The table is dropped.
        assert!(table_engine
            .get_table(&engine_ctx, table_id)
            .unwrap()
            .is_none());
    }
}
