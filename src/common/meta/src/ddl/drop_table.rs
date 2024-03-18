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

pub mod executor;

use async_trait::async_trait;
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, LockKey, Procedure, Result as ProcedureResult, Status,
};
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use strum::AsRefStr;
use table::metadata::{RawTableInfo, TableId};
use table::table_reference::TableReference;

use self::executor::DropTableExecutor;
use super::utils::handle_retry_error;
use crate::ddl::DdlContext;
use crate::error::{self, Result};
use crate::key::table_info::TableInfoValue;
use crate::key::table_route::TableRouteValue;
use crate::key::DeserializedValueWithBytes;
use crate::lock_key::{CatalogLock, SchemaLock, TableLock};
use crate::metrics;
use crate::region_keeper::OperatingRegionGuard;
use crate::rpc::ddl::DropTableTask;
use crate::rpc::router::{operating_leader_regions, RegionRoute};

pub struct DropTableProcedure {
    /// The context of procedure runtime.
    pub context: DdlContext,
    /// The serializable data.
    pub data: DropTableData,
    /// The guards of opening regions.
    pub dropping_regions: Vec<OperatingRegionGuard>,
}

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

    async fn on_prepare<'a>(&mut self, executor: &DropTableExecutor) -> Result<Status> {
        if executor.on_prepare(&self.context).await?.stop() {
            return Ok(Status::done());
        }
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
    async fn on_remove_metadata(&mut self, executor: &DropTableExecutor) -> Result<Status> {
        self.register_dropping_regions()?;
        // NOTES: If the meta server is crashed after the `RemoveMetadata`,
        // Corresponding regions of this table on the Datanode will be closed automatically.
        // Then any future dropping operation will fail.

        // TODO(weny): Considers introducing a RegionStatus to indicate the region is dropping.
        let table_id = self.data.table_id();
        executor
            .on_remove_metadata(
                &self.context,
                &self.data.table_info_value,
                &self.data.table_route_value,
            )
            .await?;
        info!("Deleted table metadata for table {table_id}");
        self.data.state = DropTableState::InvalidateTableCache;
        Ok(Status::executing(true))
    }

    /// Broadcasts invalidate table cache instruction.
    async fn on_broadcast(&mut self, executor: &DropTableExecutor) -> Result<Status> {
        executor.invalidate_table_cache(&self.context).await?;
        self.data.state = DropTableState::DatanodeDropRegions;

        Ok(Status::executing(true))
    }

    pub async fn on_datanode_drop_regions(&self, executor: &DropTableExecutor) -> Result<Status> {
        executor
            .on_drop_regions(&self.context, &self.data.table_route_value)
            .await?;
        Ok(Status::done())
    }
}

#[async_trait]
impl Procedure for DropTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let executor = DropTableExecutor::new(
            self.data.task.table_name(),
            self.data.table_id(),
            self.data.task.drop_if_exists,
        );
        let state = &self.data.state;
        let _timer = metrics::METRIC_META_PROCEDURE_DROP_TABLE
            .with_label_values(&[state.as_ref()])
            .start_timer();

        match self.data.state {
            DropTableState::Prepare => self.on_prepare(&executor).await,
            DropTableState::RemoveMetadata => self.on_remove_metadata(&executor).await,
            DropTableState::InvalidateTableCache => self.on_broadcast(&executor).await,
            DropTableState::DatanodeDropRegions => self.on_datanode_drop_regions(&executor).await,
        }
        .map_err(handle_retry_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let table_ref = &self.data.table_ref();
        let table_id = self.data.table_id();
        let lock_key = vec![
            CatalogLock::Read(table_ref.catalog).into(),
            SchemaLock::read(table_ref.catalog, table_ref.schema).into(),
            TableLock::Write(table_id).into(),
        ];

        LockKey::new(lock_key)
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
