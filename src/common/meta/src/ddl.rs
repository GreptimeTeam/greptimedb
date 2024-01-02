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

use common_telemetry::tracing_context::W3cTrace;
use store_api::storage::{RegionNumber, TableId};

use crate::cache_invalidator::CacheInvalidatorRef;
use crate::datanode_manager::DatanodeManagerRef;
use crate::error::Result;
use crate::key::table_route::TableRouteValue;
use crate::key::TableMetadataManagerRef;
use crate::region_keeper::MemoryRegionKeeperRef;
use crate::rpc::ddl::{SubmitDdlTaskRequest, SubmitDdlTaskResponse};

pub mod alter_table;
pub mod create_table;
pub mod drop_table;
pub mod table_meta;
pub mod truncate_table;
pub mod utils;

#[derive(Debug, Default)]
pub struct ExecutorContext {
    pub cluster_id: Option<u64>,
    pub tracing_context: Option<W3cTrace>,
}

#[async_trait::async_trait]
pub trait DdlTaskExecutor: Send + Sync {
    async fn submit_ddl_task(
        &self,
        ctx: &ExecutorContext,
        request: SubmitDdlTaskRequest,
    ) -> Result<SubmitDdlTaskResponse>;
}

pub type DdlTaskExecutorRef = Arc<dyn DdlTaskExecutor>;

pub struct TableMetadataAllocatorContext {
    pub cluster_id: u64,
}

/// Metadata allocated to a table.
pub struct TableMetadata {
    /// Table id.
    pub table_id: TableId,
    /// Route information for each region of the table.
    pub table_route: TableRouteValue,
    /// The encoded wal options for regions of the table.
    // If a region does not have an associated wal options, no key for the region would be found in the map.
    pub region_wal_options: HashMap<RegionNumber, String>,
}

#[derive(Clone)]
pub struct DdlContext {
    pub datanode_manager: DatanodeManagerRef,
    pub cache_invalidator: CacheInvalidatorRef,
    pub table_metadata_manager: TableMetadataManagerRef,
    pub memory_region_keeper: MemoryRegionKeeperRef,
}
