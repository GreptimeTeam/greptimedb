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

use std::vec;

use api::v1::alter_table_expr::Kind;
use api::v1::RenameTable;
use async_trait::async_trait;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, LockKey, Procedure, Status, StringKey,
};
use common_telemetry::{debug, info};
use futures::future;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::RegionId;
use strum::AsRefStr;
use table::metadata::{RawTableInfo, TableId};
use table::table_reference::TableReference;

use crate::cache_invalidator::Context;
use crate::ddl::utils::add_peer_context_if_needed;
use crate::ddl::DdlContext;
use crate::error::{Error, Result};
use crate::instruction::CacheIdent;
use crate::key::table_info::TableInfoValue;
use crate::key::{DeserializedValueWithBytes, RegionDistribution};
use crate::lock_key::{CatalogLock, SchemaLock, TableLock, TableNameLock};
use crate::rpc::ddl::AlterTableTask;
use crate::rpc::router::{find_leader_regions, find_leaders, region_distribution};
use crate::{metrics, ClusterId};

/// The alter table procedure
pub struct AlterTableProcedure {
    // The runtime context.
    context: DdlContext,
    // The serialized data.
    data: AlterTableData,
}

impl AlterTableProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::AlterTable";

    pub fn new(
        cluster_id: ClusterId,
        table_id: TableId,
        task: AlterTableTask,
        context: DdlContext,
    ) -> Result<Self> {
        task.validate()?;
        Ok(Self {
            context,
            data: AlterTableData::new(task, table_id, cluster_id),
        })
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data: AlterTableData = serde_json::from_str(json).context(FromJsonSnafu)?;
        Ok(AlterTableProcedure { context, data })
    }

    // Checks whether the table exists.
    pub(crate) async fn on_prepare(&mut self) -> Result<Status> {
        self.check_alter().await?;
        self.fill_table_info().await?;
        // Safety: Checked in `AlterTableProcedure::new`.
        let alter_kind = self.data.task.alter_table.kind.as_ref().unwrap();
        if matches!(alter_kind, Kind::RenameTable { .. }) {
            self.data.state = AlterTableState::UpdateMetadata;
        } else {
            self.data.state = AlterTableState::SubmitAlterRegionRequests;
        };
        Ok(Status::executing(true))
    }

    pub async fn submit_alter_region_requests(&mut self) -> Result<Status> {
        let table_id = self.data.table_id();
        let (_, physical_table_route) = self
            .context
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(table_id)
            .await?;

        self.data.region_distribution =
            Some(region_distribution(&physical_table_route.region_routes));

        let leaders = find_leaders(&physical_table_route.region_routes);
        let mut alter_region_tasks = Vec::with_capacity(leaders.len());

        for datanode in leaders {
            let requester = self.context.node_manager.datanode(&datanode).await;
            let regions = find_leader_regions(&physical_table_route.region_routes, &datanode);

            for region in regions {
                let region_id = RegionId::new(table_id, region);
                let request = self.make_alter_region_request(region_id)?;
                debug!("Submitting {request:?} to {datanode}");

                let datanode = datanode.clone();
                let requester = requester.clone();

                alter_region_tasks.push(async move {
                    if let Err(err) = requester.handle(request).await {
                        if err.status_code() != StatusCode::RequestOutdated {
                            // Treat request outdated as success.
                            // The engine will throw this code when the schema version not match.
                            // As this procedure has locked the table, the only reason for this error
                            // is procedure is succeeded before and is retrying.
                            return Err(add_peer_context_if_needed(datanode)(err));
                        }
                    }
                    Ok(())
                });
            }
        }

        future::join_all(alter_region_tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        self.data.state = AlterTableState::UpdateMetadata;

        Ok(Status::executing(true))
    }

    /// Update table metadata.
    pub(crate) async fn on_update_metadata(&mut self) -> Result<Status> {
        let table_id = self.data.table_id();
        let table_ref = self.data.table_ref();
        // Safety: checked before.
        let table_info_value = self.data.table_info_value.as_ref().unwrap();
        let new_info = self.build_new_table_info(&table_info_value.table_info)?;

        debug!(
            "Starting update table: {} metadata, new table info {:?}",
            table_ref.to_string(),
            new_info
        );

        // Safety: Checked in `AlterTableProcedure::new`.
        let alter_kind = self.data.task.alter_table.kind.as_ref().unwrap();
        if let Kind::RenameTable(RenameTable { new_table_name }) = alter_kind {
            self.on_update_metadata_for_rename(new_table_name.to_string(), table_info_value)
                .await?;
        } else {
            // region distribution is set in submit_alter_region_requests
            let region_distribution = self.data.region_distribution.as_ref().unwrap().clone();
            self.on_update_metadata_for_alter(
                new_info.into(),
                region_distribution,
                table_info_value,
            )
            .await?;
        }

        info!("Updated table metadata for table {table_ref}, table_id: {table_id}");
        self.data.state = AlterTableState::InvalidateTableCache;
        Ok(Status::executing(true))
    }

    /// Broadcasts the invalidating table cache instructions.
    async fn on_broadcast(&mut self) -> Result<Status> {
        let cache_invalidator = &self.context.cache_invalidator;

        cache_invalidator
            .invalidate(
                &Context::default(),
                &[
                    CacheIdent::TableId(self.data.table_id()),
                    CacheIdent::TableName(self.data.table_ref().into()),
                ],
            )
            .await?;

        Ok(Status::done())
    }

    fn lock_key_inner(&self) -> Vec<StringKey> {
        let mut lock_key = vec![];
        let table_ref = self.data.table_ref();
        let table_id = self.data.table_id();
        lock_key.push(CatalogLock::Read(table_ref.catalog).into());
        lock_key.push(SchemaLock::read(table_ref.catalog, table_ref.schema).into());
        lock_key.push(TableLock::Write(table_id).into());

        // Safety: Checked in `AlterTableProcedure::new`.
        let alter_kind = self.data.task.alter_table.kind.as_ref().unwrap();
        if let Kind::RenameTable(RenameTable { new_table_name }) = alter_kind {
            lock_key.push(
                TableNameLock::new(table_ref.catalog, table_ref.schema, new_table_name).into(),
            )
        }

        lock_key
    }
}

#[async_trait]
impl Procedure for AlterTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let error_handler = |e: Error| {
            if e.is_retry_later() {
                ProcedureError::retry_later(e)
            } else {
                ProcedureError::external(e)
            }
        };

        let state = &self.data.state;

        let step = state.as_ref();

        let _timer = metrics::METRIC_META_PROCEDURE_ALTER_TABLE
            .with_label_values(&[step])
            .start_timer();

        match state {
            AlterTableState::Prepare => self.on_prepare().await,
            AlterTableState::SubmitAlterRegionRequests => self.submit_alter_region_requests().await,
            AlterTableState::UpdateMetadata => self.on_update_metadata().await,
            AlterTableState::InvalidateTableCache => self.on_broadcast().await,
        }
        .map_err(error_handler)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let key = self.lock_key_inner();

        LockKey::new(key)
    }
}

#[derive(Debug, Serialize, Deserialize, AsRefStr)]
enum AlterTableState {
    /// Prepares to alter the table.
    Prepare,
    /// Sends alter region requests to Datanode.
    SubmitAlterRegionRequests,
    /// Updates table metadata.
    UpdateMetadata,
    /// Broadcasts the invalidating table cache instruction.
    InvalidateTableCache,
}

// The serialized data of alter table.
#[derive(Debug, Serialize, Deserialize)]
pub struct AlterTableData {
    cluster_id: ClusterId,
    state: AlterTableState,
    task: AlterTableTask,
    table_id: TableId,
    /// Table info value before alteration.
    table_info_value: Option<DeserializedValueWithBytes<TableInfoValue>>,
    /// Region distribution for table in case we need to update region options.
    region_distribution: Option<RegionDistribution>,
}

impl AlterTableData {
    pub fn new(task: AlterTableTask, table_id: TableId, cluster_id: u64) -> Self {
        Self {
            state: AlterTableState::Prepare,
            task,
            table_id,
            cluster_id,
            table_info_value: None,
            region_distribution: None,
        }
    }

    fn table_ref(&self) -> TableReference {
        self.task.table_ref()
    }

    fn table_id(&self) -> TableId {
        self.table_id
    }

    fn table_info(&self) -> Option<&RawTableInfo> {
        self.table_info_value
            .as_ref()
            .map(|value| &value.table_info)
    }
}
