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
mod metadata;

use std::collections::HashMap;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_procedure::error::{ExternalSnafu, FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, LockKey, Procedure,
    Result as ProcedureResult, Status,
};
use common_telemetry::info;
use common_telemetry::tracing::warn;
use common_wal::options::WalOptions;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionNumber;
use strum::AsRefStr;
use table::metadata::TableId;
use table::table_reference::TableReference;

use self::executor::DropTableExecutor;
use crate::ddl::DdlContext;
use crate::ddl::utils::map_to_procedure_error;
use crate::error::{self, Result};
use crate::key::table_route::TableRouteValue;
use crate::lock_key::{CatalogLock, SchemaLock, TableLock};
use crate::metrics;
use crate::region_keeper::OperatingRegionGuard;
use crate::rpc::ddl::DropTableTask;
use crate::rpc::router::{RegionRoute, operating_leader_regions};

pub struct DropTableProcedure {
    /// The context of procedure runtime.
    pub context: DdlContext,
    /// The serializable data.
    pub data: DropTableData,
    /// The guards of opening regions.
    pub(crate) dropping_regions: Vec<OperatingRegionGuard>,
    /// The drop table executor.
    executor: DropTableExecutor,
}

impl DropTableProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::DropTable";

    pub fn new(task: DropTableTask, context: DdlContext) -> Self {
        let data = DropTableData::new(task);
        let executor = data.build_executor();
        Self {
            context,
            data,
            dropping_regions: vec![],
            executor,
        }
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data: DropTableData = serde_json::from_str(json).context(FromJsonSnafu)?;
        let executor = data.build_executor();

        Ok(Self {
            context,
            data,
            dropping_regions: vec![],
            executor,
        })
    }

    pub(crate) async fn on_prepare(&mut self) -> Result<Status> {
        if self.executor.on_prepare(&self.context).await?.stop() {
            return Ok(Status::done());
        }
        self.fill_table_metadata().await?;
        self.data.state = DropTableState::DeleteMetadata;

        Ok(Status::executing(true))
    }

    /// Register dropping regions if doesn't exist.
    fn register_dropping_regions(&mut self) -> Result<()> {
        let dropping_regions = operating_leader_regions(&self.data.physical_region_routes);

        if !self.dropping_regions.is_empty() {
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
    pub(crate) async fn on_delete_metadata(&mut self) -> Result<Status> {
        self.register_dropping_regions()?;
        // NOTES: If the meta server is crashed after the `RemoveMetadata`,
        // Corresponding regions of this table on the Datanode will be closed automatically.
        // Then any future dropping operation will fail.

        // TODO(weny): Considers introducing a RegionStatus to indicate the region is dropping.
        let table_id = self.data.table_id();
        let table_route_value = &TableRouteValue::new(
            self.data.task.table_id,
            // Safety: checked
            self.data.physical_table_id.unwrap(),
            self.data.physical_region_routes.clone(),
        );
        // Deletes table metadata logically.
        self.executor
            .on_delete_metadata(
                &self.context,
                table_route_value,
                &self.data.region_wal_options,
            )
            .await?;
        info!("Deleted table metadata for table {table_id}");
        self.data.state = DropTableState::InvalidateTableCache;
        Ok(Status::executing(true))
    }

    /// Broadcasts invalidate table cache instruction.
    async fn on_broadcast(&mut self) -> Result<Status> {
        self.executor.invalidate_table_cache(&self.context).await?;
        self.data.state = DropTableState::DatanodeDropRegions;

        Ok(Status::executing(true))
    }

    pub async fn on_datanode_drop_regions(&mut self) -> Result<Status> {
        self.executor
            .on_drop_regions(
                &self.context.node_manager,
                &self.context.leader_region_registry,
                &self.data.physical_region_routes,
                false,
                false,
                false,
            )
            .await?;
        self.data.state = DropTableState::DeleteTombstone;
        Ok(Status::executing(true))
    }

    /// Deletes metadata tombstone.
    async fn on_delete_metadata_tombstone(&mut self) -> Result<Status> {
        let table_route_value = &TableRouteValue::new(
            self.data.task.table_id,
            // Safety: checked
            self.data.physical_table_id.unwrap(),
            self.data.physical_region_routes.clone(),
        );
        self.executor
            .on_delete_metadata_tombstone(
                &self.context,
                table_route_value,
                &self.data.region_wal_options,
            )
            .await?;

        self.dropping_regions.clear();
        Ok(Status::done())
    }
}

#[async_trait]
impl Procedure for DropTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    fn recover(&mut self) -> ProcedureResult<()> {
        // Only registers regions if the metadata is deleted.
        let register_operating_regions = matches!(
            self.data.state,
            DropTableState::DeleteMetadata
                | DropTableState::InvalidateTableCache
                | DropTableState::DatanodeDropRegions
        );
        if register_operating_regions {
            self.register_dropping_regions()
                .map_err(BoxedError::new)
                .context(ExternalSnafu {
                    clean_poisons: false,
                })?;
        }

        Ok(())
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &self.data.state;
        let _timer = metrics::METRIC_META_PROCEDURE_DROP_TABLE
            .with_label_values(&[state.as_ref()])
            .start_timer();

        match self.data.state {
            DropTableState::Prepare => self.on_prepare().await,
            DropTableState::DeleteMetadata => self.on_delete_metadata().await,
            DropTableState::InvalidateTableCache => self.on_broadcast().await,
            DropTableState::DatanodeDropRegions => self.on_datanode_drop_regions().await,
            DropTableState::DeleteTombstone => self.on_delete_metadata_tombstone().await,
        }
        .map_err(map_to_procedure_error)
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

    fn rollback_supported(&self) -> bool {
        !matches!(self.data.state, DropTableState::Prepare) && self.data.allow_rollback
    }

    async fn rollback(&mut self, _: &ProcedureContext) -> ProcedureResult<()> {
        warn!(
            "Rolling back the drop table procedure, table: {}",
            self.data.table_id()
        );

        let table_route_value = &TableRouteValue::new(
            self.data.task.table_id,
            // Safety: checked
            self.data.physical_table_id.unwrap(),
            self.data.physical_region_routes.clone(),
        );
        self.executor
            .on_restore_metadata(
                &self.context,
                table_route_value,
                &self.data.region_wal_options,
            )
            .await
            .map_err(ProcedureError::external)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DropTableData {
    pub state: DropTableState,
    pub task: DropTableTask,
    pub physical_region_routes: Vec<RegionRoute>,
    pub physical_table_id: Option<TableId>,
    #[serde(default)]
    pub region_wal_options: HashMap<RegionNumber, WalOptions>,
    #[serde(default)]
    pub allow_rollback: bool,
}

impl DropTableData {
    pub fn new(task: DropTableTask) -> Self {
        Self {
            state: DropTableState::Prepare,
            task,
            physical_region_routes: vec![],
            physical_table_id: None,
            region_wal_options: HashMap::new(),
            allow_rollback: false,
        }
    }

    fn table_ref(&self) -> TableReference<'_> {
        self.task.table_ref()
    }

    fn table_id(&self) -> TableId {
        self.task.table_id
    }

    fn build_executor(&self) -> DropTableExecutor {
        DropTableExecutor::new(
            self.task.table_name(),
            self.task.table_id,
            self.task.drop_if_exists,
        )
    }
}

/// The state of drop table.
#[derive(Debug, Serialize, Deserialize, AsRefStr, PartialEq)]
pub enum DropTableState {
    /// Prepares to drop the table
    Prepare,
    /// Deletes metadata logically
    DeleteMetadata,
    /// Invalidates Table Cache
    InvalidateTableCache,
    /// Drops regions on Datanode
    DatanodeDropRegions,
    /// Deletes metadata tombstone permanently
    DeleteTombstone,
}
