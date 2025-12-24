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

pub(crate) mod enter_staging_region;
pub(crate) mod repartition_start;
pub(crate) mod update_metadata;
pub(crate) mod utils;

use std::any::Any;
use std::fmt::Debug;
use std::time::Duration;

use common_error::ext::BoxedError;
use common_meta::DatanodeId;
use common_meta::cache_invalidator::CacheInvalidatorRef;
use common_meta::instruction::CacheIdent;
use common_meta::key::datanode_table::{DatanodeTableKey, DatanodeTableValue, RegionInfo};
use common_meta::key::table_route::TableRouteValue;
use common_meta::key::{DeserializedValueWithBytes, TableMetadataManagerRef};
use common_meta::rpc::router::RegionRoute;
use common_procedure::{Context as ProcedureContext, Status};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::storage::{RegionId, TableId};
use uuid::Uuid;

use crate::error::{self, Result};
use crate::procedure::repartition::plan::RegionDescriptor;
use crate::service::mailbox::MailboxRef;

pub type GroupId = Uuid;

pub struct RepartitionGroupProcedure {}

pub struct Context {
    pub persistent_ctx: PersistentContext,

    pub cache_invalidator: CacheInvalidatorRef,

    pub table_metadata_manager: TableMetadataManagerRef,

    pub mailbox: MailboxRef,

    pub server_addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GroupPrepareResult {
    pub source_routes: Vec<RegionRoute>,
    pub target_routes: Vec<RegionRoute>,
    pub central_region: RegionId,
    pub central_region_datanode_id: DatanodeId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistentContext {
    pub group_id: GroupId,
    /// The table id of the repartition group.
    pub table_id: TableId,
    /// The source regions of the repartition group.
    pub sources: Vec<RegionDescriptor>,
    /// The target regions of the repartition group.
    pub targets: Vec<RegionDescriptor>,
    /// The result of group prepare.
    /// The value will be set in [RepartitionStart](crate::procedure::repartition::group::repartition_start::RepartitionStart) state.
    pub group_prepare_result: Option<GroupPrepareResult>,
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
        let group_id = self.persistent_ctx.group_id;
        let table_route_value = self
            .table_metadata_manager
            .table_route_manager()
            .table_route_storage()
            .get_with_raw_bytes(table_id)
            .await
            .map_err(BoxedError::new)
            .with_context(|_| error::RetryLaterWithSourceSnafu {
                reason: format!(
                    "Failed to get table route for table: {}, repartition group: {}",
                    table_id, group_id
                ),
            })?
            .context(error::TableRouteNotFoundSnafu { table_id })?;

        Ok(table_route_value)
    }

    /// Returns the `datanode_table_value`
    ///
    /// Retry:
    /// - Failed to retrieve the metadata of datanode table.
    pub async fn get_datanode_table_value(
        &self,
        table_id: TableId,
        datanode_id: u64,
    ) -> Result<DatanodeTableValue> {
        let datanode_table_value = self
            .table_metadata_manager
            .datanode_table_manager()
            .get(&DatanodeTableKey {
                datanode_id,
                table_id,
            })
            .await
            .context(error::TableMetadataManagerSnafu)
            .map_err(BoxedError::new)
            .with_context(|_| error::RetryLaterWithSourceSnafu {
                reason: format!("Failed to get DatanodeTable: {table_id}"),
            })?
            .context(error::DatanodeTableNotFoundSnafu {
                table_id,
                datanode_id,
            })?;
        Ok(datanode_table_value)
    }

    /// Broadcasts the invalidate table cache message.
    pub async fn invalidate_table_cache(&self) -> Result<()> {
        let table_id = self.persistent_ctx.table_id;
        let group_id = self.persistent_ctx.group_id;
        let subject = format!(
            "Invalidate table cache for repartition table, group: {}, table: {}",
            group_id, table_id,
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

    /// Updates the table route.
    ///
    /// Retry:
    /// - Failed to retrieve the metadata of datanode table.
    ///
    /// Abort:
    /// - Table route not found.
    /// - Failed to update the table route.
    pub async fn update_table_route(
        &self,
        current_table_route_value: &DeserializedValueWithBytes<TableRouteValue>,
        new_region_routes: Vec<RegionRoute>,
    ) -> Result<()> {
        let table_id = self.persistent_ctx.table_id;
        // Safety: prepare result is set in [RepartitionStart] state.
        let prepare_result = self.persistent_ctx.group_prepare_result.as_ref().unwrap();
        let central_region_datanode_table_value = self
            .get_datanode_table_value(table_id, prepare_result.central_region_datanode_id)
            .await?;
        let RegionInfo {
            region_options,
            region_wal_options,
            ..
        } = &central_region_datanode_table_value.region_info;

        self.table_metadata_manager
            .update_table_route(
                table_id,
                central_region_datanode_table_value.region_info.clone(),
                current_table_route_value,
                new_region_routes,
                region_options,
                region_wal_options,
            )
            .await
            .context(error::TableMetadataManagerSnafu)
    }

    /// Returns the next operation timeout.
    ///
    /// If the next operation timeout is not set, it will return `None`.
    pub fn next_operation_timeout(&self) -> Option<Duration> {
        Some(Duration::from_secs(10))
    }
}

/// Returns the region routes of the given table route value.
///
/// Abort:
/// - Table route value is not physical.
pub fn region_routes(
    table_id: TableId,
    table_route_value: &TableRouteValue,
) -> Result<&Vec<RegionRoute>> {
    table_route_value
        .region_routes()
        .with_context(|_| error::UnexpectedLogicalRouteTableSnafu {
            err_msg: format!(
                "TableRoute({:?}) is a non-physical TableRouteValue.",
                table_id
            ),
        })
}

#[async_trait::async_trait]
#[typetag::serde(tag = "repartition_group_state")]
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

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use common_meta::key::TableMetadataManager;
    use common_meta::kv_backend::test_util::MockKvBackendBuilder;

    use crate::error::Error;
    use crate::procedure::repartition::test_util::{TestingEnv, new_persistent_context};

    #[tokio::test]
    async fn test_get_table_route_value_not_found_error() {
        let env = TestingEnv::new();
        let persistent_context = new_persistent_context(1024, vec![], vec![]);
        let ctx = env.create_context(persistent_context);
        let err = ctx.get_table_route_value().await.unwrap_err();
        assert_matches!(err, Error::TableRouteNotFound { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_get_table_route_value_retry_error() {
        let kv = MockKvBackendBuilder::default()
            .range_fn(Arc::new(|_| {
                common_meta::error::UnexpectedSnafu {
                    err_msg: "mock err",
                }
                .fail()
            }))
            .build()
            .unwrap();
        let mut env = TestingEnv::new();
        env.table_metadata_manager = Arc::new(TableMetadataManager::new(Arc::new(kv)));
        let persistent_context = new_persistent_context(1024, vec![], vec![]);
        let ctx = env.create_context(persistent_context);
        let err = ctx.get_table_route_value().await.unwrap_err();
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn test_get_datanode_table_value_retry_error() {
        let kv = MockKvBackendBuilder::default()
            .range_fn(Arc::new(|_| {
                common_meta::error::UnexpectedSnafu {
                    err_msg: "mock err",
                }
                .fail()
            }))
            .build()
            .unwrap();
        let mut env = TestingEnv::new();
        env.table_metadata_manager = Arc::new(TableMetadataManager::new(Arc::new(kv)));
        let persistent_context = new_persistent_context(1024, vec![], vec![]);
        let ctx = env.create_context(persistent_context);
        let err = ctx.get_datanode_table_value(1024, 1).await.unwrap_err();
        assert!(err.is_retryable());
    }
}
