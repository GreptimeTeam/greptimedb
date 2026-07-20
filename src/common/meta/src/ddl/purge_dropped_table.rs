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
use common_telemetry::info;
use common_time::util::current_time_millis;
use common_wal::options::WalOptions;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, ensure};
use store_api::storage::{RegionNumber, TableId};
use strum::AsRefStr;
use table::metadata::TableInfo;
use table::table_name::TableName;

use crate::ddl::DdlContext;
use crate::ddl::drop_table::executor::DropTableExecutor;
use crate::ddl::utils::{
    convert_region_routes_to_detecting_regions, is_metric_engine_logical_table,
    map_to_procedure_error,
};
use crate::error::{self, Result};
use crate::key::DroppedTableMetadata;
use crate::key::table_route::TableRouteValue;
use crate::lock_key::TableLock;
use crate::rpc::ddl::PurgeDroppedTableTask;
use crate::rpc::router::RegionRoute;

pub struct PurgeDroppedTableProcedure {
    context: DdlContext,
    data: PurgeDroppedTableData,
}

impl PurgeDroppedTableProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::PurgeDroppedTable";
    pub const EXPIRED_TYPE_NAME: &'static str = "metasrv-procedure::PurgeExpiredDroppedTable";

    pub fn new(task: PurgeDroppedTableTask, context: DdlContext) -> Self {
        Self {
            context,
            data: PurgeDroppedTableData::new(task),
        }
    }

    pub fn new_if_expired(task: PurgeDroppedTableTask, context: DdlContext) -> Self {
        Self {
            context,
            data: PurgeDroppedTableData::new_if_expired(task),
        }
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data: PurgeDroppedTableData = serde_json::from_str(json).context(FromJsonSnafu)?;
        Ok(Self { context, data })
    }

    async fn eligible_dropped_table(&self) -> Result<Option<DroppedTableMetadata>> {
        let dropped_table = self
            .context
            .table_metadata_manager
            .get_dropped_table_by_id(self.data.task.table_id)
            .await?;

        let Some(dropped_table) = dropped_table else {
            return Ok(None);
        };
        if self.data.check_expired {
            if !self.context.soft_drop_enabled {
                return Ok(None);
            }
            let retention_millis = self
                .context
                .soft_drop_retention
                .and_then(|retention| i64::try_from(retention.as_millis()).ok());
            let expires_at = dropped_table.retention_expires_at.or_else(|| {
                dropped_table.dropped_at.and_then(|dropped_at| {
                    retention_millis.and_then(|retention| dropped_at.checked_add(retention))
                })
            });
            if expires_at.is_none_or(|expires_at| expires_at > current_time_millis()) {
                return Ok(None);
            }
        }
        ensure!(
            !is_metric_engine_logical_table(
                &dropped_table.table_info_value.table_info,
                &dropped_table.table_route_value
            ),
            error::UnsupportedSnafu {
                operation: "purging metric logical tables".to_string()
            }
        );
        Ok(Some(dropped_table))
    }

    fn update_dropped_table(&mut self, dropped_table: DroppedTableMetadata) {
        self.data.table_id = Some(dropped_table.table_id);
        self.data.table_name = Some(dropped_table.table_name);
        self.data.table_info = Some(dropped_table.table_info_value.table_info);
        self.data.table_route_value = Some(dropped_table.table_route_value);
        self.data.region_wal_options = dropped_table.region_wal_options;
        self.data.drop_generation = dropped_table.drop_generation;
        self.data.drop_generation_loaded = true;
    }

    async fn on_prepare(&mut self) -> Result<Status> {
        let Some(dropped_table) = self.eligible_dropped_table().await? else {
            return Ok(Status::done());
        };
        self.update_dropped_table(dropped_table);
        info!(
            "Prepared purge dropped table procedure for {}",
            self.data.purge_target_info()
        );
        self.data.state = PurgeDroppedTableState::DropRegions;
        Ok(Status::executing(true))
    }

    async fn on_drop_regions(&mut self) -> Result<Status> {
        info!(
            "Purging dropped table regions for {}",
            self.data.purge_target_info()
        );
        let expected_generation = self.data.drop_generation.as_deref().unwrap_or_default();
        let purge_claim = self
            .context
            .table_metadata_manager
            .dropped_table_purge_claim(self.data.task.table_id)
            .await?;
        if let Some(purge_claim) = purge_claim {
            if purge_claim != expected_generation {
                return Ok(Status::done());
            }
            self.data.purging_claimed = true;
        } else {
            // Revalidate after recovery before crossing the durable cleanup boundary.
            let Some(dropped_table) = self.eligible_dropped_table().await? else {
                return Ok(Status::done());
            };
            if dropped_table.drop_generation != self.data.drop_generation {
                return Ok(Status::done());
            }
            self.update_dropped_table(dropped_table);
            self.context
                .table_metadata_manager
                .mark_dropped_table_purging(
                    self.data.task.table_id,
                    self.data.drop_generation.as_deref(),
                )
                .await?;
            self.data.purging_claimed = true;
        }
        if let Some(region_routes) = self.data.physical_region_routes() {
            self.executor()
                .on_cleanup_regions_offline(
                    &self.context.node_manager,
                    &self.context.leader_region_registry,
                    self.data.table_info(),
                    region_routes,
                    &self.data.region_wal_options,
                )
                .await?;
            self.context
                .deregister_failure_detectors(convert_region_routes_to_detecting_regions(
                    region_routes,
                ))
                .await;
        }
        self.data.state = PurgeDroppedTableState::DeleteTombstone;
        Ok(Status::executing(true))
    }

    async fn on_delete_tombstone(&mut self) -> Result<Status> {
        info!(
            "Deleting dropped table tombstone for {}",
            self.data.purge_target_info()
        );
        if self.data.purging_claimed {
            let expected_generation = self.data.drop_generation.as_deref().unwrap_or_default();
            if self
                .context
                .table_metadata_manager
                .dropped_table_purge_claim(self.data.task.table_id)
                .await?
                .as_deref()
                != Some(expected_generation)
            {
                return Ok(Status::done());
            }
        }
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
        if self.data.check_expired {
            Self::EXPIRED_TYPE_NAME
        } else {
            Self::TYPE_NAME
        }
    }

    fn recover(&mut self) -> ProcedureResult<()> {
        if self.data.state == PurgeDroppedTableState::DropRegions
            && !self.data.drop_generation_loaded
        {
            self.data.state = PurgeDroppedTableState::Prepare;
        }
        Ok(())
    }

    async fn execute(&mut self, _: &ProcedureContext) -> ProcedureResult<Status> {
        match self.data.state {
            PurgeDroppedTableState::Prepare => self.on_prepare().await,
            PurgeDroppedTableState::DropRegions => self.on_drop_regions().await,
            PurgeDroppedTableState::DeleteTombstone => self.on_delete_tombstone().await,
        }
        .map_err(map_to_procedure_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        LockKey::new(vec![TableLock::Write(self.data.task.table_id).into()])
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
    #[serde(default)]
    check_expired: bool,
    #[serde(default)]
    drop_generation: Option<String>,
    #[serde(default)]
    drop_generation_loaded: bool,
    #[serde(default)]
    purging_claimed: bool,
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
            check_expired: false,
            drop_generation: None,
            drop_generation_loaded: false,
            purging_claimed: false,
        }
    }

    fn new_if_expired(task: PurgeDroppedTableTask) -> Self {
        Self {
            check_expired: true,
            ..Self::new(task)
        }
    }

    fn table_id(&self) -> TableId {
        self.table_id.unwrap()
    }

    fn table_name(&self) -> &TableName {
        self.table_name.as_ref().unwrap()
    }

    fn table_route_value(&self) -> &TableRouteValue {
        self.table_route_value.as_ref().unwrap()
    }

    fn table_info(&self) -> &TableInfo {
        self.table_info.as_ref().unwrap()
    }

    fn purge_target_info(&self) -> String {
        match (self.table_name.as_ref(), self.table_id) {
            (Some(table_name), Some(table_id)) => format!(
                "catalog={}, schema={}, table={}, table_id={}",
                table_name.catalog_name, table_name.schema_name, table_name.table_name, table_id
            ),
            _ => format!("table_id={}", self.task.table_id),
        }
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
    DropRegions,
    DeleteTombstone,
}
