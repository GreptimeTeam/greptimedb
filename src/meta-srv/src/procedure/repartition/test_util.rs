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
use std::sync::Arc;

use common_meta::key::{TableMetadataManager, TableMetadataManagerRef};
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::sequence::SequenceBuilder;
use datatypes::value::Value;
use partition::expr::{PartitionExpr, col};
use store_api::storage::TableId;
use uuid::Uuid;

use crate::cache_invalidator::MetasrvCacheInvalidator;
use crate::metasrv::MetasrvInfo;
use crate::procedure::repartition::group::{Context, PersistentContext};
use crate::procedure::repartition::plan::RegionDescriptor;
use crate::procedure::test_util::MailboxContext;

/// `TestingEnv` provides components during the tests.
pub struct TestingEnv {
    pub table_metadata_manager: TableMetadataManagerRef,
    pub mailbox_ctx: MailboxContext,
    pub server_addr: String,
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
        let mailbox_sequence =
            SequenceBuilder::new("test_heartbeat_mailbox", kv_backend.clone()).build();
        let mailbox_ctx = MailboxContext::new(mailbox_sequence);

        Self {
            table_metadata_manager,
            mailbox_ctx,
            server_addr: "localhost".to_string(),
        }
    }

    pub fn create_context(&self, persistent_context: PersistentContext) -> Context {
        let cache_invalidator = Arc::new(MetasrvCacheInvalidator::new(
            self.mailbox_ctx.mailbox().clone(),
            MetasrvInfo {
                server_addr: String::new(),
            },
        ));

        Context {
            persistent_ctx: persistent_context,
            table_metadata_manager: self.table_metadata_manager.clone(),
            cache_invalidator,
            mailbox: self.mailbox_ctx.mailbox().clone(),
            server_addr: self.server_addr.clone(),
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
        catalog_name: "test_catalog".to_string(),
        schema_name: "test_schema".to_string(),
        table_id,
        sources,
        targets,
        region_mapping: HashMap::new(),
        group_prepare_result: None,
        staging_manifest_paths: HashMap::new(),
    }
}
