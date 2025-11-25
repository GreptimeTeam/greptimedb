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

use common_meta::key::{TableMetadataManager, TableMetadataManagerRef};
use common_meta::kv_backend::memory::MemoryKvBackend;
use datatypes::value::Value;
use partition::expr::{PartitionExpr, col};
use store_api::storage::TableId;
use uuid::Uuid;

use crate::procedure::repartition::group::{Context, PersistentContext};
use crate::procedure::repartition::plan::RegionDescriptor;

/// `TestingEnv` provides components during the tests.
pub struct TestingEnv {
    pub table_metadata_manager: TableMetadataManagerRef,
}

impl Default for TestingEnv {
    fn default() -> Self {
        Self::new()
    }
}

impl TestingEnv {
    pub fn new() -> Self {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));

        Self {
            table_metadata_manager,
        }
    }

    pub fn create_context(self, persistent_context: PersistentContext) -> Context {
        Context {
            persistent_ctx: persistent_context,
            table_metadata_manager: self.table_metadata_manager.clone(),
        }
    }
}

pub fn range_expr(col_name: &str, start: i64, end: i64) -> PartitionExpr {
    col(col_name)
        .gt_eq(Value::Int64(start))
        .and(col(col_name).lt(Value::Int64(end)))
}

pub fn new_persistent_context(
    table_id: TableId,
    sources: Vec<RegionDescriptor>,
    targets: Vec<RegionDescriptor>,
) -> PersistentContext {
    PersistentContext {
        group_id: Uuid::new_v4(),
        table_id,
        sources,
        targets,
        group_prepare_result: None,
    }
}
