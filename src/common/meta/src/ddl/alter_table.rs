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
mod metadata;
mod region_request;

use std::vec;

use api::region::RegionResponse;
use api::v1::RenameTable;
use api::v1::alter_table_expr::Kind;
use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, ContextProvider, Error as ProcedureError, LockKey, PoisonKey,
    PoisonKeys, Procedure, ProcedureId, Status, StringKey,
};
use common_telemetry::{error, info, warn};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, ensure};
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::TABLE_COLUMN_METADATA_EXTENSION_KEY;
use strum::AsRefStr;
use table::metadata::{RawTableInfo, TableId, TableInfo};
use table::table_reference::TableReference;

use crate::ddl::DdlContext;
use crate::ddl::alter_table::executor::AlterTableExecutor;
use crate::ddl::utils::{
    MultipleResults, extract_column_metadatas, handle_multiple_results, map_to_procedure_error,
    sync_follower_regions,
};
use crate::error::{AbortProcedureSnafu, NoLeaderSnafu, PutPoisonSnafu, Result, RetryLaterSnafu};
use crate::key::table_info::TableInfoValue;
use crate::key::{DeserializedValueWithBytes, RegionDistribution};
use crate::lock_key::{CatalogLock, SchemaLock, TableLock, TableNameLock};
use crate::metrics;
use crate::poison_key::table_poison_key;
use crate::rpc::ddl::AlterTableTask;
use crate::rpc::router::{RegionRoute, find_leaders, region_distribution};

/// The alter table procedure
pub struct AlterTableProcedure {
    /// The runtime context.
    context: DdlContext,
    /// The serialized data.
    data: AlterTableData,
    /// Cached new table metadata in the prepare step.
    /// If we recover the procedure from json, then the table info value is not cached.
    /// But we already validated it in the prepare step.
    new_table_info: Option<TableInfo>,
    /// The alter table executor.
    executor: AlterTableExecutor,
}

/// Builds the executor from the [`AlterTableData`].
///
/// # Panics
/// - If the alter kind is not set.
fn build_executor_from_alter_expr(alter_data: &AlterTableData) -> AlterTableExecutor {
    let table_name = alter_data.table_ref().into();
    let table_id = alter_data.table_id;
    let alter_kind = alter_data.task.alter_table.kind.as_ref().unwrap();
    let new_table_name = if let Kind::RenameTable(RenameTable { new_table_name }) = alter_kind {
        Some(new_table_name.clone())
    } else {
        None
    };
    AlterTableExecutor::new(table_name, table_id, new_table_name)
}

impl AlterTableProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::AlterTable";

    pub fn new(table_id: TableId, task: AlterTableTask, context: DdlContext) -> Result<Self> {
        task.validate()?;
        let data = AlterTableData::new(task, table_id);
        let executor = build_executor_from_alter_expr(&data);
        Ok(Self {
            context,
            data,
            new_table_info: None,
            executor,
        })
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data: AlterTableData = serde_json::from_str(json).context(FromJsonSnafu)?;
        let executor = build_executor_from_alter_expr(&data);

        Ok(AlterTableProcedure {
            context,
            data,
            new_table_info: None,
            executor,
        })
    }

    // Checks whether the table exists.
    pub(crate) async fn on_prepare(&mut self) -> Result<Status> {
        self.executor
            .on_prepare(&self.context.table_metadata_manager)
            .await?;
        self.fill_table_info().await?;

        // Safety: filled in `fill_table_info`.
        let table_info_value = self.data.table_info_value.as_ref().unwrap();
        let new_table_info = AlterTableExecutor::validate_alter_table_expr(
            &table_info_value.table_info,
            self.data.task.alter_table.clone(),
        )?;
        self.new_table_info = Some(new_table_info);

        // Safety: Checked in `AlterTableProcedure::new`.
        let alter_kind = self.data.task.alter_table.kind.as_ref().unwrap();
        if matches!(alter_kind, Kind::RenameTable { .. }) {
            self.data.state = AlterTableState::UpdateMetadata;
        } else {
            self.data.state = AlterTableState::SubmitAlterRegionRequests;
        };
        Ok(Status::executing(true))
    }

    fn table_poison_key(&self) -> PoisonKey {
        table_poison_key(self.data.table_id())
    }

    async fn put_poison(
        &self,
        ctx_provider: &dyn ContextProvider,
        procedure_id: ProcedureId,
    ) -> Result<()> {
        let poison_key = self.table_poison_key();
        ctx_provider
            .try_put_poison(&poison_key, procedure_id)
            .await
            .context(PutPoisonSnafu)
    }

    pub async fn submit_alter_region_requests(
        &mut self,
        procedure_id: ProcedureId,
        ctx_provider: &dyn ContextProvider,
    ) -> Result<Status> {
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
        let alter_kind = self.make_region_alter_kind()?;

        info!(
            "Submitting alter region requests for table {}, table_id: {}, alter_kind: {:?}",
            self.data.table_ref(),
            table_id,
            alter_kind,
        );

        ensure!(!leaders.is_empty(), NoLeaderSnafu { table_id });
        // Puts the poison before submitting alter region requests to datanodes.
        self.put_poison(ctx_provider, procedure_id).await?;
        let results = self
            .executor
            .on_alter_regions(
                &self.context.node_manager,
                &physical_table_route.region_routes,
                alter_kind,
            )
            .await;

        match handle_multiple_results(results) {
            MultipleResults::PartialRetryable(error) => {
                // Just returns the error, and wait for the next try.
                Err(error)
            }
            MultipleResults::PartialNonRetryable(error) => {
                error!(error; "Partial non-retryable errors occurred during alter table, table {}, table_id: {}", self.data.table_ref(), self.data.table_id());
                // No retry will be done.
                Ok(Status::poisoned(
                    Some(self.table_poison_key()),
                    ProcedureError::external(error),
                ))
            }
            MultipleResults::AllRetryable(error) => {
                // Just returns the error, and wait for the next try.
                let err = BoxedError::new(error);
                Err(err).context(RetryLaterSnafu {
                    clean_poisons: true,
                })
            }
            MultipleResults::Ok(results) => {
                self.submit_sync_region_requests(&results, &physical_table_route.region_routes)
                    .await;
                self.handle_alter_region_response(results)?;
                Ok(Status::executing_with_clean_poisons(true))
            }
            MultipleResults::AllNonRetryable(error) => {
                error!(error; "All alter requests returned non-retryable errors for table {}, table_id: {}", self.data.table_ref(), self.data.table_id());
                // It assumes the metadata on datanode is not changed.
                // Case: The alter region request is sent but not applied. (e.g., InvalidArgument)

                let err = BoxedError::new(error);
                Err(err).context(AbortProcedureSnafu {
                    clean_poisons: true,
                })
            }
        }
    }

    fn handle_alter_region_response(&mut self, mut results: Vec<RegionResponse>) -> Result<()> {
        if let Some(column_metadatas) =
            extract_column_metadatas(&mut results, TABLE_COLUMN_METADATA_EXTENSION_KEY)?
        {
            self.data.column_metadatas = column_metadatas;
        } else {
            warn!(
                "altering table result doesn't contains extension key `{TABLE_COLUMN_METADATA_EXTENSION_KEY}`,leaving the table's column metadata unchanged"
            );
        }
        self.data.state = AlterTableState::UpdateMetadata;
        Ok(())
    }

    async fn submit_sync_region_requests(
        &mut self,
        results: &[RegionResponse],
        region_routes: &[RegionRoute],
    ) {
        // Safety: filled in `prepare` step.
        let table_info = self.data.table_info().unwrap();
        if let Err(err) = sync_follower_regions(
            &self.context,
            self.data.table_id(),
            results,
            region_routes,
            table_info.meta.engine.as_str(),
        )
        .await
        {
            error!(err; "Failed to sync regions for table {}, table_id: {}", self.data.table_ref(), self.data.table_id());
        }
    }

    /// Update table metadata.
    pub(crate) async fn on_update_metadata(&mut self) -> Result<Status> {
        let table_id = self.data.table_id();
        let table_ref = self.data.table_ref();
        // Safety: filled in `fill_table_info`.
        let table_info_value = self.data.table_info_value.as_ref().unwrap();
        // Safety: Checked in `AlterTableProcedure::new`.
        let alter_kind = self.data.task.alter_table.kind.as_ref().unwrap();

        // Gets the table info from the cache or builds it.
        let  new_info = match &self.new_table_info {
            Some(cached) => cached.clone(),
            None => AlterTableExecutor::validate_alter_table_expr(
                &table_info_value.table_info,
                self.data.task.alter_table.clone(),
               )
                .inspect_err(|e| {
                    // We already check the table info in the prepare step so this should not happen.
                    error!(e; "Unable to build info for table {} in update metadata step, table_id: {}", table_ref, table_id);
                })?,
        };

        // Safety: region distribution is set in `submit_alter_region_requests`.
        // Check if skip_wal changed and update WAL options if needed
        let current_skip_wal = table_info_value.table_info.meta.options.skip_wal;
        let new_skip_wal = new_info.meta.options.skip_wal;
        let new_region_wal_options = if current_skip_wal != new_skip_wal
            && self.data.region_distribution.is_some()
        {
            // Get region numbers from region_distribution
            let region_distribution = self.data.region_distribution.as_ref().unwrap();
            let region_numbers: Vec<_> = region_distribution
                .values()
                .flat_map(|region_role_set| {
                    region_role_set
                        .leader_regions
                        .iter()
                        .chain(region_role_set.follower_regions.iter())
                        .copied()
                })
                .collect();
            // Allocate new WAL options based on skip_wal
            Some(
                self.context
                    .table_metadata_allocator
                    .wal_options_allocator()
                    .allocate(&region_numbers, new_skip_wal)
                    .await?,
            )
        } else {
            None
        };

        self.executor
            .on_alter_metadata(
                &self.context.table_metadata_manager,
                table_info_value,
                self.data.region_distribution.as_ref(),
                new_info.into(),
                &self.data.column_metadatas,
                new_region_wal_options,
            )
            .await?;

        info!(
            "Updated table metadata for table {table_ref}, table_id: {table_id}, kind: {alter_kind:?}"
        );
        self.data.state = AlterTableState::InvalidateTableCache;
        Ok(Status::executing(true))
    }

    /// Broadcasts the invalidating table cache instructions.
    async fn on_broadcast(&mut self) -> Result<Status> {
        self.executor
            .invalidate_table_cache(&self.context.cache_invalidator)
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

    #[cfg(test)]
    pub(crate) fn data(&self) -> &AlterTableData {
        &self.data
    }

    #[cfg(test)]
    pub(crate) fn mut_data(&mut self) -> &mut AlterTableData {
        &mut self.data
    }
}

#[async_trait]
impl Procedure for AlterTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &self.data.state;

        let step = state.as_ref();

        let _timer = metrics::METRIC_META_PROCEDURE_ALTER_TABLE
            .with_label_values(&[step])
            .start_timer();

        match state {
            AlterTableState::Prepare => self.on_prepare().await,
            AlterTableState::SubmitAlterRegionRequests => {
                self.submit_alter_region_requests(ctx.procedure_id, ctx.provider.as_ref())
                    .await
            }
            AlterTableState::UpdateMetadata => self.on_update_metadata().await,
            AlterTableState::InvalidateTableCache => self.on_broadcast().await,
        }
        .map_err(map_to_procedure_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let key = self.lock_key_inner();

        LockKey::new(key)
    }

    fn poison_keys(&self) -> PoisonKeys {
        PoisonKeys::new(vec![self.table_poison_key()])
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
    state: AlterTableState,
    task: AlterTableTask,
    table_id: TableId,
    #[serde(default)]
    column_metadatas: Vec<ColumnMetadata>,
    /// Table info value before alteration.
    table_info_value: Option<DeserializedValueWithBytes<TableInfoValue>>,
    /// Region distribution for table in case we need to update region options.
    region_distribution: Option<RegionDistribution>,
}

impl AlterTableData {
    pub fn new(task: AlterTableTask, table_id: TableId) -> Self {
        Self {
            state: AlterTableState::Prepare,
            task,
            table_id,
            column_metadatas: vec![],
            table_info_value: None,
            region_distribution: None,
        }
    }

    fn table_ref(&self) -> TableReference<'_> {
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

    #[cfg(test)]
    pub(crate) fn column_metadatas(&self) -> &[ColumnMetadata] {
        &self.column_metadatas
    }

    #[cfg(test)]
    pub(crate) fn set_column_metadatas(&mut self, column_metadatas: Vec<ColumnMetadata>) {
        self.column_metadatas = column_metadatas;
    }
}
