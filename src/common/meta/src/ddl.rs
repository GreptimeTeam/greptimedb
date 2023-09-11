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

use api::v1::meta::Partition;
use store_api::storage::TableId;
use table::metadata::RawTableInfo;

use crate::cache_invalidator::CacheInvalidatorRef;
use crate::datanode_manager::DatanodeManagerRef;
use crate::error::Result;
use crate::key::TableMetadataManagerRef;
use crate::rpc::ddl::{SubmitDdlTaskRequest, SubmitDdlTaskResponse};
use crate::rpc::router::RegionRoute;

pub mod alter_table;
pub mod create_table;
pub mod drop_table;
pub mod truncate_table;
pub mod utils;

#[derive(Debug, Default)]
pub struct ExecutorContext {
    pub cluster_id: Option<u64>,
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

#[async_trait::async_trait]
pub trait TableMetadataAllocator: Send + Sync {
    async fn create(
        &self,
        ctx: &TableMetadataAllocatorContext,
        table_info: &mut RawTableInfo,
        partitions: &[Partition],
    ) -> Result<(TableId, Vec<RegionRoute>)>;
}

pub type TableMetadataAllocatorRef = Arc<dyn TableMetadataAllocator>;

#[derive(Clone)]
pub struct DdlContext {
    pub datanode_manager: DatanodeManagerRef,
    pub cache_invalidator: CacheInvalidatorRef,
    pub table_metadata_manager: TableMetadataManagerRef,
}
