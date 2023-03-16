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
use snafu::ResultExt;
use common_procedure::{Status, Result, LockKey, Procedure, Context};
use common_procedure::error::ToJsonSnafu;
use store_api::storage::StorageEngine;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use crate::engine::{MitoEngineInner};
use table::requests::AlterTableRequest;

/// Procedure to alter a [MitoTable].
pub(crate) struct AlterMitoTable<S: StorageEngine> {
    data: AlterTableData,
    engine_inner: Arc<MitoEngineInner<S>>,
    // TODO(yingwen): Table.
    // problem: What if table isn't opened.
}

#[async_trait]
impl<S: StorageEngine> Procedure for AlterMitoTable<S> {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &Context) -> Result<Status> {
        match self.data.state {
            AlterTableState::Prepare => self.on_prepare(),
            AlterTableState::AlterRegions => self.on_alter_regions().await,
            AlterTableState::UpdateTableManifest => self.on_update_table_manifest().await,
        }
    }

    fn dump(&self) -> Result<String> {
        let json = serde_json::to_string(&self.data).context(ToJsonSnafu)?;
        Ok(json)
    }

    fn lock_key(&self) -> LockKey {
        unimplemented!()
    }
}

impl<S: StorageEngine> AlterMitoTable<S> {
    const TYPE_NAME: &str = "mito::AlterMitoTable";

    fn on_prepare(&mut self) -> Result<Status> {
        unimplemented!()
    }

    async fn on_alter_regions(&mut self) -> Result<Status> {
        unimplemented!()
    }

    async fn on_update_table_manifest(&mut self) -> Result<Status> {
        unimplemented!()
    }
}

/// Represents each step while altering table in the mito engine.
#[derive(Debug, Serialize, Deserialize)]
enum AlterTableState {
    /// Prepare to alter table.
    Prepare,
    /// Alter regions.
    AlterRegions,
    /// Update table manifest.
    UpdateTableManifest,
}

/// Serializable data of [AlterMitoTable].
#[derive(Debug, Serialize, Deserialize)]
struct AlterTableData {
    state: AlterTableState,
    request: AlterTableRequest,
}
