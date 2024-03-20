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

use common_procedure::Status;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;
use table::metadata::TableId;

use super::cursor::DropDatabaseCursor;
use super::{DropDatabaseContext, DropTableTarget};
use crate::ddl::drop_database::State;
use crate::ddl::drop_table::executor::DropTableExecutor;
use crate::ddl::DdlContext;
use crate::error::{self, Result};
use crate::key::table_info::TableInfoValue;
use crate::key::table_route::TableRouteValue;
use crate::key::DeserializedValueWithBytes;
use crate::region_keeper::OperatingRegionGuard;
use crate::rpc::router::operating_leader_regions;
use crate::table_name::TableName;

#[derive(Debug, Serialize, Deserialize)]
pub struct DropDatabaseExecutor {
    table_name: TableName,
    table_id: TableId,
    table_info_value: DeserializedValueWithBytes<TableInfoValue>,
    table_route_value: DeserializedValueWithBytes<TableRouteValue>,
    target: DropTableTarget,
    #[serde(skip)]
    dropping_regions: Vec<OperatingRegionGuard>,
}

impl DropDatabaseExecutor {
    /// Returns a new [DropDatabaseExecutor].
    pub fn new(
        table_name: TableName,
        table_id: TableId,
        table_info_value: DeserializedValueWithBytes<TableInfoValue>,
        table_route_value: DeserializedValueWithBytes<TableRouteValue>,
        target: DropTableTarget,
    ) -> Self {
        Self {
            table_name,
            table_id,
            table_info_value,
            table_route_value,
            target,
            dropping_regions: vec![],
        }
    }
}

impl DropDatabaseExecutor {
    fn register_dropping_regions(&mut self, ddl_ctx: &DdlContext) -> Result<()> {
        let region_routes = self.table_route_value.region_routes()?;
        let dropping_regions = operating_leader_regions(region_routes);
        let mut dropping_region_guards = Vec::with_capacity(dropping_regions.len());
        for (region_id, datanode_id) in dropping_regions {
            let guard = ddl_ctx
                .memory_region_keeper
                .register(datanode_id, region_id)
                .context(error::RegionOperatingRaceSnafu {
                    region_id,
                    peer_id: datanode_id,
                })?;
            dropping_region_guards.push(guard);
        }
        self.dropping_regions = dropping_region_guards;
        Ok(())
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for DropDatabaseExecutor {
    async fn next(
        &mut self,
        ddl_ctx: &DdlContext,
        _ctx: &mut DropDatabaseContext,
    ) -> Result<(Box<dyn State>, Status)> {
        self.register_dropping_regions(ddl_ctx)?;
        let executor = DropTableExecutor::new(self.table_name.clone(), self.table_id, true);
        // Removes metadata of the table.
        executor
            .on_remove_metadata(ddl_ctx, &self.table_info_value, &self.table_route_value)
            .await?;
        // Invalidates cache.
        executor.invalidate_table_cache(ddl_ctx).await?;
        // Drops the region on the datanode.
        executor
            .on_drop_regions(ddl_ctx, &self.table_route_value)
            .await?;

        Ok((
            Box::new(DropDatabaseCursor::new(self.target)),
            Status::executing(false),
        ))
    }
}
