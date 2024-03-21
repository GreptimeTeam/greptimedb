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
mod update_metadata;

use async_trait::async_trait;
use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{Context, LockKey, Procedure, Status};
use futures_util::future;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use strum::AsRefStr;
use table::metadata::TableId;

use crate::ddl::utils::add_peer_context_if_needed;
use crate::ddl::DdlContext;
use crate::error::{Error, Result};
use crate::instruction::CacheIdent;
use crate::key::table_info::TableInfoValue;
use crate::key::table_route::PhysicalTableRouteValue;
use crate::lock_key::{CatalogLock, SchemaLock, TableLock, TableNameLock};
use crate::rpc::ddl::AlterTableTask;
use crate::rpc::router::{find_leader_regions, find_leaders};
use crate::{cache_invalidator, metrics, ClusterId};

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
                physical_table_route: None,
                cache_invalidate_keys: vec![],
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
        self.fill_physical_table_route().await?;
        // Filter the tasks
        let finished_tasks = self.check_finished_tasks()?;
        if finished_tasks.iter().all(|x| *x) {
            return Ok(Status::done());
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
            let requester = self.context.datanode_manager.datanode(&peer).await;
            let region_numbers = find_leader_regions(&physical_table_route.region_routes, &peer);

            for region_number in region_numbers {
                let request = self.make_request(region_number)?;
                let peer = peer.clone();
                let requester = requester.clone();

                alter_region_tasks.push(async move {
                    requester
                        .handle(request)
                        .await
                        .map_err(add_peer_context_if_needed(peer))
                });
            }
        }

        future::join_all(alter_region_tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        self.data.state = AlterTablesState::UpdateMetadata;

        Ok(Status::executing(true))
    }

    pub(crate) async fn on_update_metadata(&mut self) -> Result<Status> {
        let table_info_values = self.build_update_metadata()?;
        let manager = &self.context.table_metadata_manager;
        let chunk_size = manager.batch_update_table_info_value_chunk_size();
        if table_info_values.len() > chunk_size {
            let chunks = table_info_values
                .into_iter()
                .chunks(chunk_size)
                .into_iter()
                .map(|check| check.collect::<Vec<_>>())
                .collect::<Vec<_>>();
            for chunk in chunks {
                manager.batch_update_table_info_values(chunk).await?;
            }
        } else {
            manager
                .batch_update_table_info_values(table_info_values)
                .await?;
        }

        self.data.state = AlterTablesState::InvalidateTableCache;
        Ok(Status::executing(true))
    }

    pub(crate) async fn on_invalidate_table_cache(&mut self) -> Result<Status> {
        let to_invalidate = self
            .data
            .cache_invalidate_keys
            .drain(..)
            .map(CacheIdent::TableId)
            .collect::<Vec<_>>();
        self.context
            .cache_invalidator
            .invalidate(&cache_invalidator::Context::default(), to_invalidate)
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

        for task in &self.data.tasks {
            lock_key.push(
                TableNameLock::new(
                    &task.alter_table.catalog_name,
                    &task.alter_table.schema_name,
                    &task.alter_table.table_name,
                )
                .into(),
            );
        }
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
    table_info_values: Vec<TableInfoValue>,
    /// Physical table info
    physical_table_id: TableId,
    physical_table_route: Option<PhysicalTableRouteValue>,
    cache_invalidate_keys: Vec<TableId>,
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
