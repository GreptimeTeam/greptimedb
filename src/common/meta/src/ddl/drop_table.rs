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
use async_trait::async_trait;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, LockKey, Procedure, Result as ProcedureResult, Status,
};
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{debug, info};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::RegionId;
use strum::AsRefStr;
use table::engine::TableReference;
use table::metadata::{RawTableInfo, TableId};

use super::utils::handle_retry_error;
use crate::cache_invalidator::Context;
use crate::ddl::utils::handle_operate_region_error;
use crate::ddl::DdlContext;
use crate::error::{self, Result};
use crate::key::table_info::TableInfoValue;
use crate::key::table_name::TableNameKey;
use crate::key::table_route::TableRouteValue;
use crate::key::DeserializedValueWithBytes;
use crate::metrics;
use crate::region_keeper::OperatingRegionGuard;
use crate::rpc::ddl::DropTableTask;
use crate::rpc::router::{
    find_leader_regions, find_leaders, operating_leader_regions, RegionRoute,
};

pub struct DropTableProcedure {
    /// The context of procedure runtime.
    pub context: DdlContext,
    /// The serializable data.
    pub data: DropTableData,
    /// The guards of opening regions.
    pub dropping_regions: Vec<OperatingRegionGuard>,
}

#[allow(dead_code)]
impl DropTableProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::DropTable";

    pub fn new(
        cluster_id: u64,
        task: DropTableTask,
        table_route_value: DeserializedValueWithBytes<TableRouteValue>,
        table_info_value: DeserializedValueWithBytes<TableInfoValue>,
        context: DdlContext,
    ) -> Self {
        Self {
            context,
            data: DropTableData::new(cluster_id, task, table_route_value, table_info_value),
            dropping_regions: vec![],
        }
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;
        Ok(Self {
            context,
            data,
            dropping_regions: vec![],
        })
    }

    async fn on_prepare(&mut self) -> Result<Status> {
        let table_ref = &self.data.table_ref();

        let exist = self
            .context
            .table_metadata_manager
            .table_name_manager()
            .exists(TableNameKey::new(
                table_ref.catalog,
                table_ref.schema,
                table_ref.table,
            ))
            .await?;

        if !exist && self.data.task.drop_if_exists {
            return Ok(Status::Done);
        }

        ensure!(
            exist,
            error::TableNotFoundSnafu {
                table_name: table_ref.to_string()
            }
        );

        self.data.state = DropTableState::RemoveMetadata;

        Ok(Status::executing(true))
    }

    /// Register dropping regions if doesn't exist.
    fn register_dropping_regions(&mut self) -> Result<()> {
        let region_routes = self.data.region_routes()?;

        let dropping_regions = operating_leader_regions(region_routes);

        if self.dropping_regions.len() == dropping_regions.len() {
            return Ok(());
        }

        let mut dropping_region_guards = Vec::with_capacity(dropping_regions.len());

        for (region_id, datanode_id) in dropping_regions {
            let guard = self
                .context
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

    /// Removes the table metadata.
    async fn on_remove_metadata(&mut self) -> Result<Status> {
        // NOTES: If the meta server is crashed after the `RemoveMetadata`,
        // Corresponding regions of this table on the Datanode will be closed automatically.
        // Then any future dropping operation will fail.

        // TODO(weny): Considers introducing a RegionStatus to indicate the region is dropping.

        let table_metadata_manager = &self.context.table_metadata_manager;
        let table_info_value = &self.data.table_info_value;
        let table_route_value = &self.data.table_route_value;
        let table_id = self.data.table_id();

        table_metadata_manager
            .delete_table_metadata(table_info_value, table_route_value)
            .await?;

        info!("Deleted table metadata for table {table_id}");

        self.data.state = DropTableState::InvalidateTableCache;

        Ok(Status::executing(true))
    }

    /// Broadcasts invalidate table cache instruction.
    async fn on_broadcast(&mut self) -> Result<Status> {
        let ctx = Context {
            subject: Some("Invalidate table cache by dropping table".to_string()),
        };

        let cache_invalidator = &self.context.cache_invalidator;

        cache_invalidator
            .invalidate_table_name(&ctx, self.data.table_ref().into())
            .await?;

        cache_invalidator
            .invalidate_table_id(&ctx, self.data.table_id())
            .await?;

        self.data.state = DropTableState::DatanodeDropRegions;

        Ok(Status::executing(true))
    }

    pub async fn on_datanode_drop_regions(&self) -> Result<Status> {
        let table_id = self.data.table_id();

        let region_routes = &self.data.region_routes()?;
        let leaders = find_leaders(region_routes);
        let mut drop_region_tasks = Vec::with_capacity(leaders.len());

        for datanode in leaders {
            let requester = self.context.datanode_manager.datanode(&datanode).await;

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
                            return Err(handle_operate_region_error(datanode)(err));
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

        Ok(Status::Done)
    }
}

#[async_trait]
impl Procedure for DropTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &self.data.state;

        let _timer = metrics::METRIC_META_PROCEDURE_DROP_TABLE
            .with_label_values(&[state.as_ref()])
            .start_timer();

        match self.data.state {
            DropTableState::Prepare => self.on_prepare().await,
            DropTableState::RemoveMetadata => self.on_remove_metadata().await,
            DropTableState::InvalidateTableCache => self.on_broadcast().await,
            DropTableState::DatanodeDropRegions => self.on_datanode_drop_regions().await,
        }
        .map_err(handle_retry_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let table_ref = &self.data.table_ref();
        let key = common_catalog::format_full_table_name(
            table_ref.catalog,
            table_ref.schema,
            table_ref.table,
        );

        LockKey::single_exclusive(key)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DropTableData {
    pub state: DropTableState,
    pub cluster_id: u64,
    pub task: DropTableTask,
    pub table_route_value: DeserializedValueWithBytes<TableRouteValue>,
    pub table_info_value: DeserializedValueWithBytes<TableInfoValue>,
}

impl DropTableData {
    pub fn new(
        cluster_id: u64,
        task: DropTableTask,
        table_route_value: DeserializedValueWithBytes<TableRouteValue>,
        table_info_value: DeserializedValueWithBytes<TableInfoValue>,
    ) -> Self {
        Self {
            state: DropTableState::Prepare,
            cluster_id,
            task,
            table_info_value,
            table_route_value,
        }
    }

    fn table_ref(&self) -> TableReference {
        self.task.table_ref()
    }

    fn region_routes(&self) -> Result<&Vec<RegionRoute>> {
        self.table_route_value.region_routes()
    }

    fn table_info(&self) -> &RawTableInfo {
        &self.table_info_value.table_info
    }

    fn table_id(&self) -> TableId {
        self.table_info().ident.table_id
    }
}

#[derive(Debug, Serialize, Deserialize, AsRefStr)]
pub enum DropTableState {
    /// Prepares to drop the table
    Prepare,
    /// Removes metadata
    RemoveMetadata,
    /// Invalidates Table Cache
    InvalidateTableCache,
    /// Drops regions on Datanode
    DatanodeDropRegions,
}
