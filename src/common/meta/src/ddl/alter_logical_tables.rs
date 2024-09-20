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

mod check;
mod metadata;
mod region_request;
mod table_cache_keys;
mod update_metadata;

use async_trait::async_trait;
use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{Context, LockKey, Procedure, Status};
use common_telemetry::{info, warn};
use futures_util::future;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::ALTER_PHYSICAL_EXTENSION_KEY;
use strum::AsRefStr;
use table::metadata::TableId;

use crate::ddl::utils::add_peer_context_if_needed;
use crate::ddl::DdlContext;
use crate::error::{DecodeJsonSnafu, Error, MetadataCorruptionSnafu, Result};
use crate::key::table_info::TableInfoValue;
use crate::key::table_route::PhysicalTableRouteValue;
use crate::key::DeserializedValueWithBytes;
use crate::lock_key::{CatalogLock, SchemaLock, TableLock};
use crate::rpc::ddl::AlterTableTask;
use crate::rpc::router::find_leaders;
use crate::{metrics, ClusterId};

pub struct AlterLogicalTablesProcedure {
    pub context: DdlContext,
    pub data: AlterTablesData,
}

impl AlterLogicalTablesProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::AlterLogicalTables";

    pub fn new(
        cluster_id: ClusterId,
        tasks: Vec<AlterTableTask>,
        physical_table_id: TableId,
        context: DdlContext,
    ) -> Self {
        Self {
            context,
            data: AlterTablesData {
                cluster_id,
                state: AlterTablesState::Prepare,
                tasks,
                table_info_values: vec![],
                physical_table_id,
                physical_table_info: None,
                physical_table_route: None,
                physical_columns: vec![],
            },
        }
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;
        Ok(Self { context, data })
    }

    pub(crate) async fn on_prepare(&mut self) -> Result<Status> {
        // Checks all the tasks
        self.check_input_tasks()?;
        // Fills the table info values
        self.fill_table_info_values().await?;
        // Checks the physical table, must after [fill_table_info_values]
        self.check_physical_table().await?;
        // Fills the physical table info
        self.fill_physical_table_info().await?;
        // Filter the finished tasks
        let finished_tasks = self.check_finished_tasks()?;
        let already_finished_count = finished_tasks
            .iter()
            .map(|x| if *x { 1 } else { 0 })
            .sum::<usize>();
        let apply_tasks_count = self.data.tasks.len();
        if already_finished_count == apply_tasks_count {
            info!("All the alter tasks are finished, will skip the procedure.");
            // Re-invalidate the table cache
            self.data.state = AlterTablesState::InvalidateTableCache;
            return Ok(Status::executing(true));
        } else if already_finished_count > 0 {
            info!(
                "There are {} alter tasks, {} of them were already finished.",
                apply_tasks_count, already_finished_count
            );
        }
        self.filter_task(&finished_tasks)?;

        // Next state
        self.data.state = AlterTablesState::SubmitAlterRegionRequests;
        Ok(Status::executing(true))
    }

    pub(crate) async fn on_submit_alter_region_requests(&mut self) -> Result<Status> {
        // Safety: we have checked the state in on_prepare
        let physical_table_route = &self.data.physical_table_route.as_ref().unwrap();
        let leaders = find_leaders(&physical_table_route.region_routes);
        let mut alter_region_tasks = Vec::with_capacity(leaders.len());

        for peer in leaders {
            let requester = self.context.node_manager.datanode(&peer).await;
            let request = self.make_request(&peer, &physical_table_route.region_routes)?;

            alter_region_tasks.push(async move {
                requester
                    .handle(request)
                    .await
                    .map_err(add_peer_context_if_needed(peer))
            });
        }

        // Collects responses from datanodes.
        let phy_raw_schemas = future::join_all(alter_region_tasks)
            .await
            .into_iter()
            .map(|res| res.map(|mut res| res.extensions.remove(ALTER_PHYSICAL_EXTENSION_KEY)))
            .collect::<Result<Vec<_>>>()?;

        if phy_raw_schemas.is_empty() {
            self.data.state = AlterTablesState::UpdateMetadata;
            return Ok(Status::executing(true));
        }

        // Verify all the physical schemas are the same
        // Safety: previous check ensures this vec is not empty
        let first = phy_raw_schemas.first().unwrap();
        ensure!(
            phy_raw_schemas.iter().all(|x| x == first),
            MetadataCorruptionSnafu {
                err_msg: "The physical schemas from datanodes are not the same."
            }
        );

        // Decodes the physical raw schemas
        if let Some(phy_raw_schema) = first {
            self.data.physical_columns =
                ColumnMetadata::decode_list(phy_raw_schema).context(DecodeJsonSnafu)?;
        } else {
            warn!("altering logical table result doesn't contains extension key `{ALTER_PHYSICAL_EXTENSION_KEY}`,leaving the physical table's schema unchanged");
        }

        self.data.state = AlterTablesState::UpdateMetadata;
        Ok(Status::executing(true))
    }

    pub(crate) async fn on_update_metadata(&mut self) -> Result<Status> {
        self.update_physical_table_metadata().await?;
        self.update_logical_tables_metadata().await?;

        self.data.state = AlterTablesState::InvalidateTableCache;
        Ok(Status::executing(true))
    }

    pub(crate) async fn on_invalidate_table_cache(&mut self) -> Result<Status> {
        let to_invalidate = self.build_table_cache_keys_to_invalidate();

        self.context
            .cache_invalidator
            .invalidate(&Default::default(), &to_invalidate)
            .await?;
        Ok(Status::done())
    }
}

#[async_trait]
impl Procedure for AlterLogicalTablesProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &Context) -> ProcedureResult<Status> {
        let error_handler = |e: Error| {
            if e.is_retry_later() {
                common_procedure::Error::retry_later(e)
            } else {
                common_procedure::Error::external(e)
            }
        };

        let state = &self.data.state;

        let step = state.as_ref();

        let _timer = metrics::METRIC_META_PROCEDURE_ALTER_TABLE
            .with_label_values(&[step])
            .start_timer();

        match state {
            AlterTablesState::Prepare => self.on_prepare().await,
            AlterTablesState::SubmitAlterRegionRequests => {
                self.on_submit_alter_region_requests().await
            }
            AlterTablesState::UpdateMetadata => self.on_update_metadata().await,
            AlterTablesState::InvalidateTableCache => self.on_invalidate_table_cache().await,
        }
        .map_err(error_handler)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        // CatalogLock, SchemaLock,
        // TableLock
        // TableNameLock(s)
        let mut lock_key = Vec::with_capacity(2 + 1 + self.data.tasks.len());
        let table_ref = self.data.tasks[0].table_ref();
        lock_key.push(CatalogLock::Read(table_ref.catalog).into());
        lock_key.push(SchemaLock::read(table_ref.catalog, table_ref.schema).into());
        lock_key.push(TableLock::Write(self.data.physical_table_id).into());
        lock_key.extend(
            self.data
                .table_info_values
                .iter()
                .map(|table| TableLock::Write(table.table_info.ident.table_id).into()),
        );

        LockKey::new(lock_key)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AlterTablesData {
    cluster_id: ClusterId,
    state: AlterTablesState,
    tasks: Vec<AlterTableTask>,
    /// Table info values before the alter operation.
    /// Corresponding one-to-one with the AlterTableTask in tasks.
    table_info_values: Vec<DeserializedValueWithBytes<TableInfoValue>>,
    /// Physical table info
    physical_table_id: TableId,
    physical_table_info: Option<DeserializedValueWithBytes<TableInfoValue>>,
    physical_table_route: Option<PhysicalTableRouteValue>,
    physical_columns: Vec<ColumnMetadata>,
}

#[derive(Debug, Serialize, Deserialize, AsRefStr)]
enum AlterTablesState {
    /// Prepares to alter the table
    Prepare,
    SubmitAlterRegionRequests,
    /// Updates table metadata.
    UpdateMetadata,
    /// Broadcasts the invalidating table cache instruction.
    InvalidateTableCache,
}
