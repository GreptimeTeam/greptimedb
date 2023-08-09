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
use table::requests::TruncateTableRequest;
use table::Table;

use crate::engine::MitoEngineInner;
use crate::table::MitoTable;

/// Procedure to truncate a [MitoTable].
pub(crate) struct TruncateMitoTable<S: StorageEngine> {
    data: TruncateTableData,
    engine_inner: Arc<MitoEngineInner<S>>,
    table: Option<Arc<MitoTable<S::Region>>>,
}

#[async_trait]
impl<S: StorageEngine> Procedure for TruncateMitoTable<S> {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &Context) -> Result<Status> {
        match self.data.state {
            TruncateTableState::Prepare => self.on_prepare(),
            TruncateTableState::EngineTruncateTable => self.on_engine_truncate_table().await,
        }
    }

    fn dump(&self) -> Result<String> {
        let json = serde_json::to_string(&self.data).context(ToJsonSnafu)?;
        Ok(json)
    }

    fn lock_key(&self) -> LockKey {
        let table_ref = self.data.table_ref();
        let Some(table) = &self.table else {
            return LockKey::default();
        };
        let info = table.table_info();
        let keys = info
            .meta
            .region_numbers
            .iter()
            .map(|number| format!("{table_ref}/region-{number}"));
        LockKey::new(keys)
    }
}

impl<S: StorageEngine> TruncateMitoTable<S> {
    const TYPE_NAME: &str = "mito::TruncateMitoTable";

    /// Returns a new [TruncateMitoTable].
    pub(crate) fn new(
        request: TruncateTableRequest,
        engine_inner: Arc<MitoEngineInner<S>>,
    ) -> Result<Self> {
        let data = TruncateTableData {
            state: TruncateTableState::Prepare,
            request,
        };
        let table = engine_inner.get_mito_table(data.request.table_id);

        Ok(TruncateMitoTable {
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
        let data: TruncateTableData = serde_json::from_str(json).context(FromJsonSnafu)?;
        let table = engine_inner.get_mito_table(data.request.table_id);

        Ok(TruncateMitoTable {
            data,
            engine_inner,
            table,
        })
    }

    /// Prepare table info.
    fn on_prepare(&mut self) -> Result<Status> {
        self.data.state = TruncateTableState::EngineTruncateTable;

        Ok(Status::executing(true))
    }

    async fn on_engine_truncate_table(&mut self) -> Result<Status> {
        let engine = &self.engine_inner;
        engine
            .truncate_table(self.data.request.clone())
            .await
            .map_err(Error::from_error_ext)?;
        Ok(Status::Done)
    }
}

/// Represents each step while truncating table in the mito engine.
#[derive(Debug, Serialize, Deserialize)]
enum TruncateTableState {
    /// Prepare to truncate the table.
    Prepare,
    /// Engine truncate the table.
    EngineTruncateTable,
}

/// Serializable data of [TruncateMitoTable].
#[derive(Debug, Serialize, Deserialize)]
struct TruncateTableData {
    state: TruncateTableState,
    request: TruncateTableRequest,
}

impl TruncateTableData {
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
    async fn test_procedure_truncate_table() {
        common_telemetry::init_default_ut_logging();

        let TestEnv {
            table_engine,
            dir: _dir,
        } = procedure_test_util::setup_test_engine("truncate_table").await;
        let schema = Arc::new(test_util::schema_for_test());
        let request = test_util::new_create_request(schema.clone());

        let engine_ctx = EngineContext::default();
        // Create table first.
        let mut procedure = table_engine
            .create_table_procedure(&engine_ctx, request.clone())
            .unwrap();
        procedure_test_util::execute_procedure_until_done(&mut procedure).await;

        let table_id = request.id;

        let request = test_util::new_truncate_request();

        // Truncate the table.
        let mut procedure = table_engine
            .truncate_table_procedure(&engine_ctx, request.clone())
            .unwrap();
        procedure_test_util::execute_procedure_until_done(&mut procedure).await;

        assert!(table_engine
            .get_table(&engine_ctx, table_id)
            .unwrap()
            .is_some());
    }
}
