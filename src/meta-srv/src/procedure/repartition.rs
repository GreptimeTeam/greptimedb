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

pub mod allocate_region;
pub mod collect;
pub mod deallocate_region;
pub mod dispatch;
pub mod group;
pub mod plan;
pub mod repartition_end;
pub mod repartition_start;

use std::any::Any;
use std::fmt::Debug;

use common_error::ext::BoxedError;
use common_meta::cache_invalidator::CacheInvalidatorRef;
use common_meta::instruction::CacheIdent;
use common_meta::key::table_route::TableRouteValue;
use common_meta::key::{DeserializedValueWithBytes, TableMetadataManagerRef};
use common_meta::node_manager::NodeManagerRef;
use common_meta::region_keeper::{MemoryRegionKeeperRef, OperatingRegionGuard};
use common_meta::region_registry::LeaderRegionRegistryRef;
use common_procedure::{Context as ProcedureContext, Status};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::storage::TableId;

use crate::error::{self, Result};
use crate::procedure::repartition::plan::RepartitionPlanEntry;
use crate::service::mailbox::MailboxRef;

#[cfg(test)]
pub mod test_util;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistentContext {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub table_id: TableId,
    pub plans: Vec<RepartitionPlanEntry>,
}

pub struct VolatileContext {
    pub deallocating_regions: Vec<OperatingRegionGuard>,
    pub allocating_regions: Vec<OperatingRegionGuard>,
}

pub struct Context {
    pub persistent_ctx: PersistentContext,
    pub volatile_ctx: VolatileContext,
    pub table_metadata_manager: TableMetadataManagerRef,
    pub memory_region_keeper: MemoryRegionKeeperRef,
    pub node_manager: NodeManagerRef,
    pub leader_region_registry: LeaderRegionRegistryRef,
    pub mailbox: MailboxRef,
    pub server_addr: String,
    pub cache_invalidator: CacheInvalidatorRef,
}

impl Context {
    /// Retrieves the table route value for the given table id.
    ///
    /// Retry:
    /// - Failed to retrieve the metadata of table.
    ///
    /// Abort:
    /// - Table route not found.
    pub async fn get_table_route_value(
        &self,
    ) -> Result<DeserializedValueWithBytes<TableRouteValue>> {
        let table_id = self.persistent_ctx.table_id;
        let table_route_value = self
            .table_metadata_manager
            .table_route_manager()
            .table_route_storage()
            .get_with_raw_bytes(table_id)
            .await
            .map_err(BoxedError::new)
            .with_context(|_| error::RetryLaterWithSourceSnafu {
                reason: format!("Failed to get table route for table: {}", table_id),
            })?
            .context(error::TableRouteNotFoundSnafu { table_id })?;

        Ok(table_route_value)
    }

    /// Broadcasts the invalidate table cache message.
    pub async fn invalidate_table_cache(&self) -> Result<()> {
        let table_id = self.persistent_ctx.table_id;
        let subject = format!(
            "Invalidate table cache for repartition table, table: {}",
            table_id,
        );
        let ctx = common_meta::cache_invalidator::Context {
            subject: Some(subject),
        };
        let _ = self
            .cache_invalidator
            .invalidate(&ctx, &[CacheIdent::TableId(table_id)])
            .await;
        Ok(())
    }
}

#[async_trait::async_trait]
#[typetag::serde(tag = "repartition_state")]
pub(crate) trait State: Sync + Send + Debug {
    fn name(&self) -> &'static str {
        let type_name = std::any::type_name::<Self>();
        // short name
        type_name.split("::").last().unwrap_or(type_name)
    }

    /// Yields the next [State] and [Status].
    async fn next(
        &mut self,
        ctx: &mut Context,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)>;

    fn as_any(&self) -> &dyn Any;
}
