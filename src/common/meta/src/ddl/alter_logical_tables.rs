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

mod executor;
mod update_metadata;
mod validator;

use api::region::RegionResponse;
use async_trait::async_trait;
use common_catalog::format_full_table_name;
use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{Context, LockKey, Procedure, Status};
use common_telemetry::{debug, error, info, warn};
pub use executor::make_alter_region_request;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::ALTER_PHYSICAL_EXTENSION_KEY;
use strum::AsRefStr;
use table::metadata::TableId;

use crate::cache_invalidator::Context as CacheContext;
use crate::ddl::DdlContext;
use crate::ddl::alter_logical_tables::executor::AlterLogicalTablesExecutor;
use crate::ddl::alter_logical_tables::validator::{
    AlterLogicalTableValidator, ValidatorResult, retain_unskipped,
};
use crate::ddl::utils::{extract_column_metadatas, map_to_procedure_error, sync_follower_regions};
use crate::error::Result;
use crate::instruction::CacheIdent;
use crate::key::DeserializedValueWithBytes;
use crate::key::table_info::TableInfoValue;
use crate::key::table_route::PhysicalTableRouteValue;
use crate::lock_key::{CatalogLock, SchemaLock, TableLock};
use crate::metrics;
use crate::rpc::ddl::AlterTableTask;
use crate::rpc::router::RegionRoute;

pub struct AlterLogicalTablesProcedure {
    pub context: DdlContext,
    pub data: AlterTablesData,
    /// Physical table route cache.
    pub physical_table_route: Option<PhysicalTableRouteValue>,
}

/// Builds the validator from the [`AlterTablesData`].
fn build_validator_from_alter_table_data<'a>(
    data: &'a AlterTablesData,
) -> AlterLogicalTableValidator<'a> {
    let phsycial_table_id = data.physical_table_id;
    let alters = data
        .tasks
        .iter()
        .map(|task| &task.alter_table)
        .collect::<Vec<_>>();
    AlterLogicalTableValidator::new(phsycial_table_id, alters)
}

/// Builds the executor from the [`AlterTablesData`].
fn build_executor_from_alter_expr<'a>(data: &'a AlterTablesData) -> AlterLogicalTablesExecutor<'a> {
    debug_assert_eq!(data.tasks.len(), data.table_info_values.len());
    let alters = data
        .tasks
        .iter()
        .zip(data.table_info_values.iter())
        .map(|(task, table_info)| (table_info.table_info.ident.table_id, &task.alter_table))
        .collect::<Vec<_>>();
    AlterLogicalTablesExecutor::new(alters)
}

impl AlterLogicalTablesProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::AlterLogicalTables";

    pub fn new(
        tasks: Vec<AlterTableTask>,
        physical_table_id: TableId,
        context: DdlContext,
    ) -> Self {
        Self {
            context,
            data: AlterTablesData {
                state: AlterTablesState::Prepare,
                tasks,
                table_info_values: vec![],
                physical_table_id,
                physical_table_info: None,
                physical_columns: vec![],
                table_cache_keys_to_invalidate: vec![],
            },
            physical_table_route: None,
        }
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;
        Ok(Self {
            context,
            data,
            physical_table_route: None,
        })
    }

    pub(crate) async fn on_prepare(&mut self) -> Result<Status> {
        let validator = build_validator_from_alter_table_data(&self.data);
        let ValidatorResult {
            num_skipped,
            skip_alter,
            table_info_values,
            physical_table_info,
            physical_table_route,
        } = validator
            .validate(&self.context.table_metadata_manager)
            .await?;

        let num_tasks = self.data.tasks.len();
        if num_skipped == num_tasks {
            info!("All the alter tasks are finished, will skip the procedure.");
            let cache_ident_keys = AlterLogicalTablesExecutor::build_cache_ident_keys(
                &physical_table_info,
                &table_info_values
                    .iter()
                    .map(|v| v.get_inner_ref())
                    .collect::<Vec<_>>(),
            );
            self.data.table_cache_keys_to_invalidate = cache_ident_keys;
            // Re-invalidate the table cache
            self.data.state = AlterTablesState::InvalidateTableCache;
            return Ok(Status::executing(true));
        } else if num_skipped > 0 {
            info!(
                "There are {} alter tasks, {} of them were already finished.",
                num_tasks, num_skipped
            );
        }

        // Updates the procedure state.
        retain_unskipped(&mut self.data.tasks, &skip_alter);
        self.data.physical_table_info = Some(physical_table_info);
        self.data.table_info_values = table_info_values;
        debug_assert_eq!(self.data.tasks.len(), self.data.table_info_values.len());
        self.physical_table_route = Some(physical_table_route);
        self.data.state = AlterTablesState::SubmitAlterRegionRequests;
        Ok(Status::executing(true))
    }

    pub(crate) async fn on_submit_alter_region_requests(&mut self) -> Result<Status> {
        self.fetch_physical_table_route_if_non_exist().await?;
        // Safety: fetched in `fetch_physical_table_route_if_non_exist`.
        let region_routes = &self.physical_table_route.as_ref().unwrap().region_routes;

        let executor = build_executor_from_alter_expr(&self.data);
        let mut results = executor
            .on_alter_regions(
                &self.context.region_rpc,
                // Avoid double-borrowing self by extracting the region_routes first
                region_routes,
            )
            .await?;

        if let Some(column_metadatas) =
            extract_column_metadatas(&mut results, ALTER_PHYSICAL_EXTENSION_KEY)?
        {
            self.data.physical_columns = column_metadatas;
        } else {
            warn!(
                "altering logical table result doesn't contains extension key `{ALTER_PHYSICAL_EXTENSION_KEY}`,leaving the physical table's schema unchanged"
            );
        }
        self.submit_sync_region_requests(results, region_routes)
            .await;
        self.data.state = AlterTablesState::UpdateMetadata;
        Ok(Status::executing(true))
    }

    async fn submit_sync_region_requests(
        &self,
        results: Vec<RegionResponse>,
        region_routes: &[RegionRoute],
    ) {
        let table_info = &self.data.physical_table_info.as_ref().unwrap().table_info;
        if let Err(err) = sync_follower_regions(
            &self.context,
            self.data.physical_table_id,
            &results,
            region_routes,
            table_info.meta.engine.as_str(),
        )
        .await
        {
            error!(err; "Failed to sync regions for table {}, table_id: {}",
                        format_full_table_name(&table_info.catalog_name, &table_info.schema_name, &table_info.name),
                        self.data.physical_table_id
            );
        }
    }

    pub(crate) async fn on_update_metadata(&mut self) -> Result<Status> {
        self.update_physical_table_metadata().await?;
        self.update_logical_tables_metadata().await?;

        let logical_table_info_values = self
            .data
            .table_info_values
            .iter()
            .map(|v| v.get_inner_ref())
            .collect::<Vec<_>>();

        let cache_ident_keys = AlterLogicalTablesExecutor::build_cache_ident_keys(
            self.data.physical_table_info.as_ref().unwrap(),
            &logical_table_info_values,
        );
        self.data.table_cache_keys_to_invalidate = cache_ident_keys;
        self.data.clear_metadata_fields();

        self.data.state = AlterTablesState::InvalidateTableCache;
        Ok(Status::executing(true))
    }

    pub(crate) async fn on_invalidate_table_cache(&mut self) -> Result<Status> {
        let to_invalidate = &self.data.table_cache_keys_to_invalidate;

        let ctx = CacheContext {
            subject: Some(format!(
                "Invalidate table cache by altering logical tables, physical_table_id: {}",
                self.data.physical_table_id,
            )),
        };

        self.context
            .cache_invalidator
            .invalidate(&ctx, to_invalidate)
            .await?;
        Ok(Status::done())
    }

    /// Fetches the physical table route if it is not already fetched.
    async fn fetch_physical_table_route_if_non_exist(&mut self) -> Result<()> {
        if self.physical_table_route.is_none() {
            let (_, physical_table_route) = self
                .context
                .table_metadata_manager
                .table_route_manager()
                .get_physical_table_route(self.data.physical_table_id)
                .await?;
            self.physical_table_route = Some(physical_table_route);
        }

        Ok(())
    }
}

#[async_trait]
impl Procedure for AlterLogicalTablesProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &Context) -> ProcedureResult<Status> {
        let state = &self.data.state;

        let step = state.as_ref();

        let _timer = metrics::METRIC_META_PROCEDURE_ALTER_TABLE
            .with_label_values(&[step])
            .start_timer();
        debug!(
            "Executing alter logical tables procedure, state: {:?}",
            state
        );

        match state {
            AlterTablesState::Prepare => self.on_prepare().await,
            AlterTablesState::SubmitAlterRegionRequests => {
                self.on_submit_alter_region_requests().await
            }
            AlterTablesState::UpdateMetadata => self.on_update_metadata().await,
            AlterTablesState::InvalidateTableCache => self.on_invalidate_table_cache().await,
        }
        .inspect_err(|_| {
            // Reset the physical table route cache.
            self.physical_table_route = None;
        })
        .map_err(map_to_procedure_error)
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
    state: AlterTablesState,
    tasks: Vec<AlterTableTask>,
    /// Table info values before the alter operation.
    /// Corresponding one-to-one with the AlterTableTask in tasks.
    table_info_values: Vec<DeserializedValueWithBytes<TableInfoValue>>,
    /// Physical table info
    physical_table_id: TableId,
    physical_table_info: Option<DeserializedValueWithBytes<TableInfoValue>>,
    physical_columns: Vec<ColumnMetadata>,
    table_cache_keys_to_invalidate: Vec<CacheIdent>,
}

impl AlterTablesData {
    /// Clears all data fields except `state` and `table_cache_keys_to_invalidate` after metadata update.
    /// This is done to avoid persisting unnecessary data after the update metadata step.
    fn clear_metadata_fields(&mut self) {
        self.tasks.clear();
        self.table_info_values.clear();
        self.physical_table_id = 0;
        self.physical_table_info = None;
        self.physical_columns.clear();
    }
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
