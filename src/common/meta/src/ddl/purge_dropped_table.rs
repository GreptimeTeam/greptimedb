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

use std::collections::HashMap;

use async_trait::async_trait;
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, LockKey, Procedure, Result as ProcedureResult, Status,
};
use common_wal::options::WalOptions;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::{RegionNumber, TableId};
use strum::AsRefStr;
use table::metadata::TableInfo;
use table::table_name::TableName;

use crate::ddl::DdlContext;
use crate::ddl::drop_table::executor::DropTableExecutor;
use crate::ddl::undrop_table::open_regions;
use crate::ddl::utils::map_to_procedure_error;
use crate::error::Result;
use crate::key::table_route::TableRouteValue;
use crate::lock_key::{CatalogLock, SchemaLock, TableLock, TableNameLock};
use crate::rpc::ddl::PurgeDroppedTableTask;
use crate::rpc::router::RegionRoute;

pub struct PurgeDroppedTableProcedure {
    context: DdlContext,
    data: PurgeDroppedTableData,
}

impl PurgeDroppedTableProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::PurgeDroppedTable";

    pub fn new(task: PurgeDroppedTableTask, context: DdlContext) -> Self {
        Self {
            context,
            data: PurgeDroppedTableData::new(task),
        }
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data: PurgeDroppedTableData = serde_json::from_str(json).context(FromJsonSnafu)?;
        Ok(Self { context, data })
    }

    async fn on_prepare(&mut self) -> Result<Status> {
        let dropped_table = if let Some(table_id) = self.data.task.table_id {
            self.context
                .table_metadata_manager
                .get_dropped_table_by_id(table_id)
                .await?
        } else {
            self.context
                .table_metadata_manager
                .get_dropped_table(&self.data.task.table_name())
                .await?
        };

        let Some(dropped_table) = dropped_table else {
            return Ok(Status::done());
        };
        self.data.table_id = Some(dropped_table.table_id);
        self.data.table_name = Some(dropped_table.table_name);
        self.data.table_info = Some(dropped_table.table_info_value.table_info);
        self.data.table_route_value = Some(dropped_table.table_route_value);
        self.data.region_wal_options = dropped_table.region_wal_options;
        self.data.state = PurgeDroppedTableState::OpenRegions;
        Ok(Status::executing(true))
    }

    async fn on_open_regions(&mut self) -> Result<Status> {
        if let Some(region_routes) = self.data.physical_region_routes() {
            open_regions(
                &self.context,
                self.data.table_id(),
                self.data.table_name(),
                self.data.table_info(),
                region_routes,
                &self.data.region_wal_options,
            )
            .await?;
        }
        self.data.state = PurgeDroppedTableState::DropRegions;
        Ok(Status::executing(true))
    }

    async fn on_drop_regions(&mut self) -> Result<Status> {
        if let Some(region_routes) = self.data.physical_region_routes() {
            self.executor()
                .on_drop_regions(
                    &self.context.node_manager,
                    &self.context.leader_region_registry,
                    region_routes,
                    false,
                    false,
                    false,
                )
                .await?;
        }
        self.data.state = PurgeDroppedTableState::DeleteTombstone;
        Ok(Status::executing(true))
    }

    async fn on_delete_tombstone(&mut self) -> Result<Status> {
        self.context
            .table_metadata_manager
            .delete_table_metadata_tombstone(
                self.data.table_id(),
                self.data.table_name(),
                self.data.table_route_value(),
                &self.data.region_wal_options,
            )
            .await?;
        Ok(Status::done())
    }

    fn executor(&self) -> DropTableExecutor {
        DropTableExecutor::new(self.data.table_name().clone(), self.data.table_id(), false)
    }
}

#[async_trait]
impl Procedure for PurgeDroppedTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    fn recover(&mut self) -> ProcedureResult<()> {
        Ok(())
    }

    async fn execute(&mut self, _: &ProcedureContext) -> ProcedureResult<Status> {
        match self.data.state {
            PurgeDroppedTableState::Prepare => self.on_prepare().await,
            PurgeDroppedTableState::OpenRegions => self.on_open_regions().await,
            PurgeDroppedTableState::DropRegions => self.on_drop_regions().await,
            PurgeDroppedTableState::DeleteTombstone => self.on_delete_tombstone().await,
        }
        .map_err(map_to_procedure_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let table_ref = self.data.task.table_ref();
        let mut keys = vec![
            CatalogLock::Read(table_ref.catalog).into(),
            SchemaLock::read(table_ref.catalog, table_ref.schema).into(),
            TableNameLock::new(table_ref.catalog, table_ref.schema, table_ref.table).into(),
        ];
        if let Some(table_id) = self.data.task.table_id {
            keys.push(TableLock::Write(table_id).into());
        }
        LockKey::new(keys)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PurgeDroppedTableData {
    state: PurgeDroppedTableState,
    task: PurgeDroppedTableTask,
    table_id: Option<TableId>,
    table_name: Option<TableName>,
    table_info: Option<TableInfo>,
    table_route_value: Option<TableRouteValue>,
    #[serde(default)]
    region_wal_options: HashMap<RegionNumber, WalOptions>,
}

impl PurgeDroppedTableData {
    fn new(task: PurgeDroppedTableTask) -> Self {
        Self {
            state: PurgeDroppedTableState::Prepare,
            task,
            table_id: None,
            table_name: None,
            table_info: None,
            table_route_value: None,
            region_wal_options: HashMap::new(),
        }
    }

    fn table_id(&self) -> TableId {
        self.table_id.unwrap()
    }

    fn table_name(&self) -> &TableName {
        self.table_name.as_ref().unwrap()
    }

    fn table_info(&self) -> &TableInfo {
        self.table_info.as_ref().unwrap()
    }

    fn table_route_value(&self) -> &TableRouteValue {
        self.table_route_value.as_ref().unwrap()
    }

    fn physical_region_routes(&self) -> Option<&[RegionRoute]> {
        match self.table_route_value() {
            TableRouteValue::Physical(route) => Some(&route.region_routes),
            TableRouteValue::Logical(_) => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, AsRefStr, PartialEq)]
enum PurgeDroppedTableState {
    Prepare,
    OpenRegions,
    DropRegions,
    DeleteTombstone,
}
