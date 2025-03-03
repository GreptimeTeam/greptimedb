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
use common_base::AffectedRows;
use common_meta::rpc::procedure::{MigrateRegionRequest, ProcedureStateResponse};
use common_query::error::Result;
use common_query::Output;
use session::context::QueryContextRef;
use store_api::storage::RegionId;
use table::requests::{CompactTableRequest, DeleteRequest, FlushTableRequest, InsertRequest};

/// A trait for handling table mutations in `QueryEngine`.
#[async_trait]
pub trait TableMutationHandler: Send + Sync {
    /// Inserts rows into the table.
    async fn insert(&self, request: InsertRequest, ctx: QueryContextRef) -> Result<Output>;

    /// Delete rows from the table.
    async fn delete(&self, request: DeleteRequest, ctx: QueryContextRef) -> Result<AffectedRows>;

    /// Trigger a flush task for table.
    async fn flush(&self, request: FlushTableRequest, ctx: QueryContextRef)
        -> Result<AffectedRows>;

    /// Trigger a compaction task for table.
    async fn compact(
        &self,
        request: CompactTableRequest,
        ctx: QueryContextRef,
    ) -> Result<AffectedRows>;

    /// Trigger a flush task for a table region.
    async fn flush_region(&self, region_id: RegionId, ctx: QueryContextRef)
        -> Result<AffectedRows>;

    /// Trigger a compaction task for a table region.
    async fn compact_region(
        &self,
        region_id: RegionId,
        ctx: QueryContextRef,
    ) -> Result<AffectedRows>;
}

/// A trait for handling procedure service requests in `QueryEngine`.
#[async_trait]
pub trait ProcedureServiceHandler: Send + Sync {
    /// Migrate a region from source peer to target peer, returns the procedure id if success.
    async fn migrate_region(&self, request: MigrateRegionRequest) -> Result<Option<String>>;

    /// Query the procedure' state by its id
    async fn query_procedure_state(&self, pid: &str) -> Result<ProcedureStateResponse>;
}

/// This flow service handler is only use for flush flow for now.
#[async_trait]
pub trait FlowServiceHandler: Send + Sync {
    async fn flush(
        &self,
        catalog: &str,
        flow: &str,
        ctx: QueryContextRef,
    ) -> Result<api::v1::flow::FlowResponse>;
}

pub type TableMutationHandlerRef = Arc<dyn TableMutationHandler>;

pub type ProcedureServiceHandlerRef = Arc<dyn ProcedureServiceHandler>;

pub type FlowServiceHandlerRef = Arc<dyn FlowServiceHandler>;
