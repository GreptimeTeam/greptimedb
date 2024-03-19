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

use api::v1::region::{
    region_request, DropRequest as PbDropRegionRequest, RegionRequest, RegionRequestHeader,
};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_telemetry::debug;
use common_telemetry::tracing_context::TracingContext;
use futures::future::join_all;
use snafu::ensure;
use store_api::storage::RegionId;
use table::metadata::TableId;

use crate::cache_invalidator::Context;
use crate::ddl::utils::add_peer_context_if_needed;
use crate::ddl::DdlContext;
use crate::error::{self, Result};
use crate::key::table_info::TableInfoValue;
use crate::key::table_name::TableNameKey;
use crate::key::table_route::TableRouteValue;
use crate::key::DeserializedValueWithBytes;
use crate::rpc::router::{find_leader_regions, find_leaders};
use crate::table_name::TableName;

/// [Control] indicated to the caller whether to go to the next step.
#[derive(Debug)]
pub enum Control<T> {
    Continue(T),
    Stop,
}

impl<T> Control<T> {
    /// Returns true if it's [Control::Stop].
    pub fn stop(&self) -> bool {
        matches!(self, Control::Stop)
    }
}

impl DropTableExecutor {
    /// Returns the [DropTableExecutor].
    pub fn new(table: TableName, table_id: TableId, drop_if_exists: bool) -> Self {
        Self {
            table,
            table_id,
            drop_if_exists,
        }
    }
}

/// [DropTableExecutor] performs:
/// - Drops the metadata of the table.
/// - Invalidates the cache on the Frontend nodes.
/// - Drops the regions on the Datanode nodes.
pub struct DropTableExecutor {
    table: TableName,
    table_id: TableId,
    drop_if_exists: bool,
}

impl DropTableExecutor {
    /// Checks whether table exists.
    /// - Early returns if table not exists and `drop_if_exists` is `true`.
    /// - Throws an error if table not exists and `drop_if_exists` is `false`.
    pub async fn on_prepare(&self, ctx: &DdlContext) -> Result<Control<()>> {
        let table_ref = self.table.table_ref();

        let exist = ctx
            .table_metadata_manager
            .table_name_manager()
            .exists(TableNameKey::new(
                table_ref.catalog,
                table_ref.schema,
                table_ref.table,
            ))
            .await?;

        if !exist && self.drop_if_exists {
            return Ok(Control::Stop);
        }

        ensure!(
            exist,
            error::TableNotFoundSnafu {
                table_name: table_ref.to_string()
            }
        );

        Ok(Control::Continue(()))
    }

    /// Removes the table metadata.
    pub async fn on_remove_metadata(
        &self,
        ctx: &DdlContext,
        table_info_value: &DeserializedValueWithBytes<TableInfoValue>,
        table_route_value: &DeserializedValueWithBytes<TableRouteValue>,
    ) -> Result<()> {
        ctx.table_metadata_manager
            .delete_table_metadata(table_info_value, table_route_value)
            .await
    }

    /// Invalidates frontend caches
    pub async fn invalidate_table_cache(&self, ctx: &DdlContext) -> Result<()> {
        let cache_invalidator = &ctx.cache_invalidator;
        let ctx = Context {
            subject: Some("Invalidate table cache by dropping table".to_string()),
        };

        // TODO(weny): merge these two invalidation instructions.
        cache_invalidator
            .invalidate_table_name(&ctx, self.table.table_ref().into())
            .await?;

        cache_invalidator
            .invalidate_table_id(&ctx, self.table_id)
            .await?;

        Ok(())
    }

    /// Drops region on datanode.
    pub async fn on_drop_regions(
        &self,
        ctx: &DdlContext,
        table_route_value: &DeserializedValueWithBytes<TableRouteValue>,
    ) -> Result<()> {
        // The `table_route_value` always be the physical table route.
        let region_routes = table_route_value.region_routes()?;
        let leaders = find_leaders(region_routes);
        let mut drop_region_tasks = Vec::with_capacity(leaders.len());
        let table_id = self.table_id;

        for datanode in leaders {
            let requester = ctx.datanode_manager.datanode(&datanode).await;
            let regions = find_leader_regions(region_routes, &datanode);
            let region_ids = regions
                .iter()
                .map(|region_number| RegionId::new(table_id, *region_number))
                .collect::<Vec<_>>();

            for region_id in region_ids {
                debug!("Dropping region {region_id} on Datanode {datanode:?}");
                let request = RegionRequest {
                    header: Some(RegionRequestHeader {
                        tracing_context: TracingContext::from_current_span().to_w3c(),
                        ..Default::default()
                    }),
                    body: Some(region_request::Body::Drop(PbDropRegionRequest {
                        region_id: region_id.as_u64(),
                    })),
                };
                let datanode = datanode.clone();
                let requester = requester.clone();
                drop_region_tasks.push(async move {
                    if let Err(err) = requester.handle(request).await {
                        if err.status_code() != StatusCode::RegionNotFound {
                            return Err(add_peer_context_if_needed(datanode)(err));
                        }
                    }
                    Ok(())
                });
            }
        }

        join_all(drop_region_tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashMap;
    use std::sync::Arc;

    use api::v1::{ColumnDataType, SemanticType};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use table::metadata::RawTableInfo;

    use super::*;
    use crate::ddl::test_util::create_table::build_raw_table_info_from_expr;
    use crate::ddl::test_util::{TestColumnDefBuilder, TestCreateTableExprBuilder};
    use crate::table_name::TableName;
    use crate::test_util::{new_ddl_context, MockDatanodeManager};

    fn test_create_raw_table_info(name: &str) -> RawTableInfo {
        let create_table = TestCreateTableExprBuilder::default()
            .column_defs([
                TestColumnDefBuilder::default()
                    .name("ts")
                    .data_type(ColumnDataType::TimestampMillisecond)
                    .semantic_type(SemanticType::Timestamp)
                    .build()
                    .unwrap()
                    .into(),
                TestColumnDefBuilder::default()
                    .name("host")
                    .data_type(ColumnDataType::String)
                    .semantic_type(SemanticType::Tag)
                    .build()
                    .unwrap()
                    .into(),
                TestColumnDefBuilder::default()
                    .name("cpu")
                    .data_type(ColumnDataType::Float64)
                    .semantic_type(SemanticType::Field)
                    .build()
                    .unwrap()
                    .into(),
            ])
            .time_index("ts")
            .primary_keys(["host".into()])
            .table_name(name)
            .build()
            .unwrap()
            .into();
        build_raw_table_info_from_expr(&create_table)
    }

    #[tokio::test]
    async fn test_on_prepare() {
        // Drops if exists
        let datanode_manager = Arc::new(MockDatanodeManager::new(()));
        let ctx = new_ddl_context(datanode_manager);
        let executor = DropTableExecutor::new(
            TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "my_table"),
            1024,
            true,
        );
        let ctrl = executor.on_prepare(&ctx).await.unwrap();
        assert!(ctrl.stop());

        // Drops a non-exists table
        let executor = DropTableExecutor::new(
            TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "my_table"),
            1024,
            false,
        );
        let err = executor.on_prepare(&ctx).await.unwrap_err();
        assert_matches!(err, error::Error::TableNotFound { .. });

        // Drops a exists table
        let executor = DropTableExecutor::new(
            TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "my_table"),
            1024,
            false,
        );
        let raw_table_info = test_create_raw_table_info("my_table");
        ctx.table_metadata_manager
            .create_table_metadata(
                raw_table_info,
                TableRouteValue::physical(vec![]),
                HashMap::new(),
            )
            .await
            .unwrap();
        let ctrl = executor.on_prepare(&ctx).await.unwrap();
        assert!(!ctrl.stop());
    }
}
