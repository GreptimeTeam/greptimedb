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
    region_request, RegionRequest, RegionRequestHeader, TruncateRequest as PbTruncateRegionRequest,
};
use async_trait::async_trait;
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, LockKey, Procedure, Result as ProcedureResult, Status,
};
use common_telemetry::debug;
use common_telemetry::tracing_context::TracingContext;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use store_api::storage::RegionId;
use strum::AsRefStr;
use table::engine::TableReference;
use table::metadata::{RawTableInfo, TableId};

use super::utils::handle_retry_error;
use crate::ddl::utils::handle_operate_region_error;
use crate::ddl::DdlContext;
use crate::error::{Result, TableNotFoundSnafu};
use crate::key::table_info::TableInfoValue;
use crate::key::table_name::TableNameKey;
use crate::key::DeserializedValueWithBytes;
use crate::metrics;
use crate::rpc::ddl::TruncateTableTask;
use crate::rpc::router::{find_leader_regions, find_leaders, RegionRoute};
use crate::table_name::TableName;

pub struct TruncateTableProcedure {
    context: DdlContext,
    data: TruncateTableData,
}

#[async_trait]
impl Procedure for TruncateTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &self.data.state;

        let _timer = metrics::METRIC_META_PROCEDURE_TRUNCATE_TABLE
            .with_label_values(&[state.as_ref()])
            .start_timer();

        match self.data.state {
            TruncateTableState::Prepare => self.on_prepare().await,
            TruncateTableState::DatanodeTruncateRegions => {
                self.on_datanode_truncate_regions().await
            }
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

impl TruncateTableProcedure {
    pub(crate) const TYPE_NAME: &'static str = "metasrv-procedure::TruncateTable";

    pub(crate) fn new(
        cluster_id: u64,
        task: TruncateTableTask,
        table_info_value: DeserializedValueWithBytes<TableInfoValue>,
        region_routes: Vec<RegionRoute>,
        context: DdlContext,
    ) -> Self {
        Self {
            context,
            data: TruncateTableData::new(cluster_id, task, table_info_value, region_routes),
        }
    }

    pub(crate) fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;
        Ok(Self { context, data })
    }

    // Checks whether the table exists.
    async fn on_prepare(&mut self) -> Result<Status> {
        let table_ref = &self.data.table_ref();

        let manager = &self.context.table_metadata_manager;

        let exist = manager
            .table_name_manager()
            .exists(TableNameKey::new(
                table_ref.catalog,
                table_ref.schema,
                table_ref.table,
            ))
            .await?;

        ensure!(
            exist,
            TableNotFoundSnafu {
                table_name: table_ref.to_string()
            }
        );

        self.data.state = TruncateTableState::DatanodeTruncateRegions;

        Ok(Status::executing(true))
    }

    async fn on_datanode_truncate_regions(&mut self) -> Result<Status> {
        let table_id = self.data.table_id();

        let region_routes = &self.data.region_routes;
        let leaders = find_leaders(region_routes);
        let mut truncate_region_tasks = Vec::with_capacity(leaders.len());

        for datanode in leaders {
            let requester = self.context.datanode_manager.datanode(&datanode).await;
            let regions = find_leader_regions(region_routes, &datanode);

            for region in regions {
                let region_id = RegionId::new(table_id, region);
                debug!(
                    "Truncating table {} region {} on Datanode {:?}",
                    self.data.table_ref(),
                    region_id,
                    datanode
                );

                let request = RegionRequest {
                    header: Some(RegionRequestHeader {
                        tracing_context: TracingContext::from_current_span().to_w3c(),
                        ..Default::default()
                    }),
                    body: Some(region_request::Body::Truncate(PbTruncateRegionRequest {
                        region_id: region_id.as_u64(),
                    })),
                };

                let datanode = datanode.clone();
                let requester = requester.clone();

                truncate_region_tasks.push(async move {
                    if let Err(err) = requester.handle(request).await {
                        return Err(handle_operate_region_error(datanode)(err));
                    }
                    Ok(())
                });
            }
        }

        join_all(truncate_region_tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        Ok(Status::Done)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TruncateTableData {
    state: TruncateTableState,
    cluster_id: u64,
    task: TruncateTableTask,
    table_info_value: DeserializedValueWithBytes<TableInfoValue>,
    region_routes: Vec<RegionRoute>,
}

impl TruncateTableData {
    pub fn new(
        cluster_id: u64,
        task: TruncateTableTask,
        table_info_value: DeserializedValueWithBytes<TableInfoValue>,
        region_routes: Vec<RegionRoute>,
    ) -> Self {
        Self {
            state: TruncateTableState::Prepare,
            cluster_id,
            task,
            table_info_value,
            region_routes,
        }
    }

    pub fn table_ref(&self) -> TableReference {
        self.task.table_ref()
    }

    pub fn table_name(&self) -> TableName {
        self.task.table_name()
    }

    fn table_info(&self) -> &RawTableInfo {
        &self.table_info_value.table_info
    }

    fn table_id(&self) -> TableId {
        self.table_info().ident.table_id
    }
}

#[derive(Debug, Serialize, Deserialize, AsRefStr)]
enum TruncateTableState {
    /// Prepares to truncate the table
    Prepare,
    /// Truncates regions on Datanode
    DatanodeTruncateRegions,
}
