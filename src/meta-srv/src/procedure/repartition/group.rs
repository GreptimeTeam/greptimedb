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

pub(crate) mod apply_staging_manifest;
pub(crate) mod enter_staging_region;
pub(crate) mod remap_manifest;
pub(crate) mod repartition_end;
pub(crate) mod repartition_start;
pub(crate) mod sync_region;
pub(crate) mod update_metadata;
pub(crate) mod utils;

use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::time::{Duration, Instant};

use common_error::ext::BoxedError;
use common_meta::cache_invalidator::CacheInvalidatorRef;
use common_meta::ddl::DdlContext;
use common_meta::instruction::CacheIdent;
use common_meta::key::datanode_table::{DatanodeTableValue, RegionInfo};
use common_meta::key::table_route::TableRouteValue;
use common_meta::key::{DeserializedValueWithBytes, TableMetadataManagerRef};
use common_meta::lock_key::{CatalogLock, RegionLock, SchemaLock};
use common_meta::peer::Peer;
use common_meta::rpc::router::RegionRoute;
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, LockKey, Procedure,
    Result as ProcedureResult, Status, StringKey, UserMetadata,
};
use common_telemetry::{error, info, warn};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::storage::{RegionId, TableId};
use uuid::Uuid;

use crate::error::{self, Result};
use crate::procedure::repartition::group::apply_staging_manifest::ApplyStagingManifest;
use crate::procedure::repartition::group::enter_staging_region::EnterStagingRegion;
use crate::procedure::repartition::group::remap_manifest::RemapManifest;
use crate::procedure::repartition::group::repartition_end::RepartitionEnd;
use crate::procedure::repartition::group::repartition_start::RepartitionStart;
use crate::procedure::repartition::group::update_metadata::UpdateMetadata;
use crate::procedure::repartition::plan::RegionDescriptor;
use crate::procedure::repartition::utils::get_datanode_table_value;
use crate::procedure::repartition::{self};
use crate::service::mailbox::MailboxRef;

#[derive(Debug, Clone, Default)]
pub struct Metrics {
    /// Elapsed time of flushing pending deallocate regions.
    flush_pending_deallocate_regions_elapsed: Duration,
    /// Elapsed time of entering staging region.
    enter_staging_region_elapsed: Duration,
    /// Elapsed time of applying staging manifest.
    apply_staging_manifest_elapsed: Duration,
    /// Elapsed time of remapping manifest.
    remap_manifest_elapsed: Duration,
    /// Elapsed time of updating metadata.
    update_metadata_elapsed: Duration,
}

impl Display for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let total = self.flush_pending_deallocate_regions_elapsed
            + self.enter_staging_region_elapsed
            + self.apply_staging_manifest_elapsed
            + self.remap_manifest_elapsed
            + self.update_metadata_elapsed;
        write!(f, "total: {:?}", total)?;
        let mut parts = Vec::with_capacity(5);
        if self.flush_pending_deallocate_regions_elapsed > Duration::ZERO {
            parts.push(format!(
                "flush_pending_deallocate_regions_elapsed: {:?}",
                self.flush_pending_deallocate_regions_elapsed
            ));
        }
        if self.enter_staging_region_elapsed > Duration::ZERO {
            parts.push(format!(
                "enter_staging_region_elapsed: {:?}",
                self.enter_staging_region_elapsed
            ));
        }
        if self.apply_staging_manifest_elapsed > Duration::ZERO {
            parts.push(format!(
                "apply_staging_manifest_elapsed: {:?}",
                self.apply_staging_manifest_elapsed
            ));
        }
        if self.remap_manifest_elapsed > Duration::ZERO {
            parts.push(format!(
                "remap_manifest_elapsed: {:?}",
                self.remap_manifest_elapsed
            ));
        }
        if self.update_metadata_elapsed > Duration::ZERO {
            parts.push(format!(
                "update_metadata_elapsed: {:?}",
                self.update_metadata_elapsed
            ));
        }

        if !parts.is_empty() {
            write!(f, ", {}", parts.join(", "))?;
        }
        Ok(())
    }
}

impl Metrics {
    /// Updates the elapsed time of entering staging region.
    pub fn update_enter_staging_region_elapsed(&mut self, elapsed: Duration) {
        self.enter_staging_region_elapsed += elapsed;
    }

    pub fn update_flush_pending_deallocate_regions_elapsed(&mut self, elapsed: Duration) {
        self.flush_pending_deallocate_regions_elapsed += elapsed;
    }

    /// Updates the elapsed time of applying staging manifest.
    pub fn update_apply_staging_manifest_elapsed(&mut self, elapsed: Duration) {
        self.apply_staging_manifest_elapsed += elapsed;
    }

    /// Updates the elapsed time of remapping manifest.
    pub fn update_remap_manifest_elapsed(&mut self, elapsed: Duration) {
        self.remap_manifest_elapsed += elapsed;
    }

    /// Updates the elapsed time of updating metadata.
    pub fn update_update_metadata_elapsed(&mut self, elapsed: Duration) {
        self.update_metadata_elapsed += elapsed;
    }
}

pub type GroupId = Uuid;

pub struct RepartitionGroupProcedure {
    state: Box<dyn State>,
    context: Context,
}

#[derive(Debug, Serialize)]
struct RepartitionGroupData<'a> {
    persistent_ctx: &'a PersistentContext,
    state: &'a dyn State,
}

#[derive(Debug, Deserialize)]
struct RepartitionGroupDataOwned {
    persistent_ctx: PersistentContext,
    state: Box<dyn State>,
}

impl RepartitionGroupProcedure {
    pub(crate) const TYPE_NAME: &'static str = "metasrv-procedure::RepartitionGroup";

    pub fn new(persistent_context: PersistentContext, context: &repartition::Context) -> Self {
        let state = Box::new(RepartitionStart);

        Self {
            state,
            context: Context {
                persistent_ctx: persistent_context,
                cache_invalidator: context.cache_invalidator.clone(),
                table_metadata_manager: context.table_metadata_manager.clone(),
                mailbox: context.mailbox.clone(),
                server_addr: context.server_addr.clone(),
                start_time: Instant::now(),
                volatile_ctx: VolatileContext::default(),
            },
        }
    }

    pub fn from_json<F>(json: &str, ctx_factory: F) -> ProcedureResult<Self>
    where
        F: FnOnce(PersistentContext) -> Context,
    {
        let RepartitionGroupDataOwned {
            state,
            persistent_ctx,
        } = serde_json::from_str(json).context(FromJsonSnafu)?;
        let context = ctx_factory(persistent_ctx);

        Ok(Self { state, context })
    }

    async fn rollback_inner(&mut self, procedure_ctx: &ProcedureContext) -> Result<()> {
        if !self.should_rollback_metadata() {
            return Ok(());
        }

        let table_lock =
            common_meta::lock_key::TableLock::Write(self.context.persistent_ctx.table_id).into();
        let _guard = procedure_ctx.provider.acquire_lock(&table_lock).await;
        UpdateMetadata::RollbackStaging
            .rollback_staging_regions(&mut self.context)
            .await?;

        if let Err(err) = self.context.invalidate_table_cache().await {
            warn!(
                err;
                "Failed to broadcast the invalidate table cache message during repartition group rollback"
            );
        }

        self.state = Box::new(RepartitionEnd);
        Ok(())
    }

    /// Returns whether group rollback should revert staging metadata.
    ///
    /// This uses an "after metadata apply, before exit staging" semantic.
    /// Once execution reaches `UpdateMetadata::ApplyStaging` or any later staging state,
    /// rollback must restore table-route metadata back to the pre-apply view.
    ///
    /// State flow:
    /// `RepartitionStart -> SyncRegion -> UpdateMetadata::ApplyStaging -> EnterStagingRegion`
    /// `                 -> RemapManifest -> ApplyStagingManifest -> UpdateMetadata::ExitStaging -> RepartitionEnd`
    /// `                               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^`
    /// `                               rollback staging metadata`
    ///
    /// Notes:
    /// - `RepartitionStart` / `SyncRegion`: no-op, metadata has not been staged yet.
    /// - `UpdateMetadata::ApplyStaging` / `EnterStagingRegion` / `RemapManifest` /
    ///   `ApplyStagingManifest` / `UpdateMetadata::RollbackStaging`: rollback-active.
    /// - `UpdateMetadata::ExitStaging` / `RepartitionEnd`: excluded, because metadata has
    ///   already moved into the post-commit exit path.
    fn should_rollback_metadata(&self) -> bool {
        self.state.as_any().is::<EnterStagingRegion>()
            || self.state.as_any().is::<RemapManifest>()
            || self.state.as_any().is::<ApplyStagingManifest>()
            || self
                .state
                .as_any()
                .downcast_ref::<UpdateMetadata>()
                .is_some_and(|state| {
                    matches!(
                        state,
                        UpdateMetadata::ApplyStaging | UpdateMetadata::RollbackStaging
                    )
                })
    }
}

#[async_trait::async_trait]
impl Procedure for RepartitionGroupProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn rollback(&mut self, ctx: &ProcedureContext) -> ProcedureResult<()> {
        self.rollback_inner(ctx)
            .await
            .map_err(ProcedureError::external)
    }

    #[tracing::instrument(skip_all, fields(
        state = %self.state.name(),
        table_id = self.context.persistent_ctx.table_id,
        group_id = %self.context.persistent_ctx.group_id,
    ))]
    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &mut self.state;
        let state_name = state.name();
        // Log state transition
        common_telemetry::info!(
            "Repartition group procedure executing state: {}, group id: {}, table id: {}",
            state_name,
            self.context.persistent_ctx.group_id,
            self.context.persistent_ctx.table_id
        );

        match state.next(&mut self.context, _ctx).await {
            Ok((next, status)) => {
                *state = next;
                Ok(status)
            }
            Err(e) => {
                if e.is_retryable() {
                    Err(ProcedureError::retry_later(e))
                } else {
                    error!(
                        e;
                        "Repartition group procedure failed, group id: {}, table id: {}",
                        self.context.persistent_ctx.group_id,
                        self.context.persistent_ctx.table_id,
                    );
                    Err(ProcedureError::external(e))
                }
            }
        }
    }

    fn rollback_supported(&self) -> bool {
        true
    }

    fn dump(&self) -> ProcedureResult<String> {
        let data = RepartitionGroupData {
            persistent_ctx: &self.context.persistent_ctx,
            state: self.state.as_ref(),
        };
        serde_json::to_string(&data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        LockKey::new(self.context.persistent_ctx.lock_key())
    }

    fn user_metadata(&self) -> Option<UserMetadata> {
        // TODO(weny): support user metadata.
        None
    }
}

pub struct Context {
    pub persistent_ctx: PersistentContext,

    pub cache_invalidator: CacheInvalidatorRef,

    pub table_metadata_manager: TableMetadataManagerRef,

    pub mailbox: MailboxRef,

    pub server_addr: String,

    pub start_time: Instant,

    pub volatile_ctx: VolatileContext,
}

#[derive(Debug, Clone, Default)]
pub struct VolatileContext {
    pub metrics: Metrics,
}

impl Context {
    pub fn new(
        ddl_ctx: &DdlContext,
        mailbox: MailboxRef,
        server_addr: String,
        persistent_ctx: PersistentContext,
    ) -> Self {
        Self {
            persistent_ctx,
            cache_invalidator: ddl_ctx.cache_invalidator.clone(),
            table_metadata_manager: ddl_ctx.table_metadata_manager.clone(),
            mailbox,
            server_addr,
            start_time: Instant::now(),
            volatile_ctx: VolatileContext::default(),
        }
    }
}

/// The result of the group preparation phase, containing validated region routes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GroupPrepareResult {
    /// The validated source region routes.
    pub source_routes: Vec<RegionRoute>,
    /// The validated target region routes.
    pub target_routes: Vec<RegionRoute>,
    /// The primary source region id (first source region), used for retrieving region options.
    pub central_region: RegionId,
    /// The peer where the primary source region is located.
    pub central_region_datanode: Peer,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistentContext {
    pub group_id: GroupId,
    /// The table id of the repartition group.
    pub table_id: TableId,
    /// The catalog name of the repartition group.
    pub catalog_name: String,
    /// The schema name of the repartition group.
    pub schema_name: String,
    /// The source regions of the repartition group.
    pub sources: Vec<RegionDescriptor>,
    /// The target regions of the repartition group.
    pub targets: Vec<RegionDescriptor>,
    /// For each `source region`, the corresponding
    /// `target regions` that overlap with it.
    pub region_mapping: HashMap<RegionId, Vec<RegionId>>,
    /// The result of group prepare.
    /// The value will be set in [RepartitionStart](crate::procedure::repartition::group::repartition_start::RepartitionStart) state.
    pub group_prepare_result: Option<GroupPrepareResult>,
    /// The staging manifest paths of the repartition group.
    /// The value will be set in [RemapManifest](crate::procedure::repartition::group::remap_manifest::RemapManifest) state.
    pub staging_manifest_paths: HashMap<RegionId, String>,
    /// Whether sync region is needed for this group.
    pub sync_region: bool,
    /// The region ids of the newly allocated regions.
    pub allocated_region_ids: Vec<RegionId>,
    /// The region ids of the regions that are pending deallocation.
    pub pending_deallocate_region_ids: Vec<RegionId>,
    /// The timeout for repartition operations.
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
}

impl PersistentContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        group_id: GroupId,
        table_id: TableId,
        catalog_name: String,
        schema_name: String,
        sources: Vec<RegionDescriptor>,
        targets: Vec<RegionDescriptor>,
        region_mapping: HashMap<RegionId, Vec<RegionId>>,
        sync_region: bool,
        allocated_region_ids: Vec<RegionId>,
        pending_deallocate_region_ids: Vec<RegionId>,
        timeout: Duration,
    ) -> Self {
        Self {
            group_id,
            table_id,
            catalog_name,
            schema_name,
            sources,
            targets,
            region_mapping,
            group_prepare_result: None,
            staging_manifest_paths: HashMap::new(),
            sync_region,
            allocated_region_ids,
            pending_deallocate_region_ids,
            timeout,
        }
    }

    pub fn lock_key(&self) -> Vec<StringKey> {
        let mut lock_keys = Vec::with_capacity(2 + self.sources.len());
        lock_keys.extend([
            CatalogLock::Read(&self.catalog_name).into(),
            SchemaLock::read(&self.catalog_name, &self.schema_name).into(),
        ]);
        for source in &self.sources {
            lock_keys.push(RegionLock::Write(source.region_id).into());
        }
        lock_keys
    }
}

impl Context {
    /// Retrieves the table route value for the given table id.
    ///
    /// Retry:
    /// - Failed to retrieve the metadata of table.
    ///
    /// Abort:
    /// - Table route not found.
    pub async fn get_table_route_value(
        &self,
    ) -> Result<DeserializedValueWithBytes<TableRouteValue>> {
        let table_id = self.persistent_ctx.table_id;
        let group_id = self.persistent_ctx.group_id;
        let table_route_value = self
            .table_metadata_manager
            .table_route_manager()
            .table_route_storage()
            .get_with_raw_bytes(table_id)
            .await
            .map_err(BoxedError::new)
            .with_context(|_| error::RetryLaterWithSourceSnafu {
                reason: format!(
                    "Failed to get table route for table: {}, repartition group: {}",
                    table_id, group_id
                ),
            })?
            .context(error::TableRouteNotFoundSnafu { table_id })?;

        Ok(table_route_value)
    }

    /// Returns the `datanode_table_value`
    ///
    /// Retry:
    /// - Failed to retrieve the metadata of datanode table.
    pub async fn get_datanode_table_value(
        &self,
        table_id: TableId,
        datanode_id: u64,
    ) -> Result<DatanodeTableValue> {
        get_datanode_table_value(&self.table_metadata_manager, table_id, datanode_id).await
    }

    /// Broadcasts the invalidate table cache message.
    pub async fn invalidate_table_cache(&self) -> Result<()> {
        let table_id = self.persistent_ctx.table_id;
        let group_id = self.persistent_ctx.group_id;
        let subject = format!(
            "Invalidate table cache for repartition table, group: {}, table: {}",
            group_id, table_id,
        );
        let ctx = common_meta::cache_invalidator::Context {
            subject: Some(subject),
        };
        let _ = self
            .cache_invalidator
            .invalidate(&ctx, &[CacheIdent::TableId(table_id)])
            .await;
        Ok(())
    }

    /// Updates the table route.
    ///
    /// Retry:
    /// - Failed to retrieve the metadata of datanode table.
    ///
    /// Abort:
    /// - Table route not found.
    /// - Failed to update the table route.
    pub async fn update_table_route(
        &self,
        current_table_route_value: &DeserializedValueWithBytes<TableRouteValue>,
        new_region_routes: Vec<RegionRoute>,
    ) -> Result<()> {
        let table_id = self.persistent_ctx.table_id;
        let group_id = self.persistent_ctx.group_id;
        // Safety: prepare result is set in [RepartitionStart] state.
        let prepare_result = self.persistent_ctx.group_prepare_result.as_ref().unwrap();
        let central_region_datanode_table_value = self
            .get_datanode_table_value(table_id, prepare_result.central_region_datanode.id)
            .await?;
        let RegionInfo {
            region_options,
            region_wal_options,
            ..
        } = &central_region_datanode_table_value.region_info;

        info!(
            "Updating table route for table: {}, group_id: {}, new region routes: {:?}",
            table_id, group_id, new_region_routes
        );
        self.table_metadata_manager
            .update_table_route(
                table_id,
                central_region_datanode_table_value.region_info.clone(),
                current_table_route_value,
                new_region_routes,
                region_options,
                region_wal_options,
            )
            .await
            .context(error::TableMetadataManagerSnafu)
    }

    /// Updates the table repart mapping.
    pub async fn update_table_repart_mapping(&self) -> Result<()> {
        info!(
            "Updating table repart mapping for table: {}, group_id: {}, region mapping: {:?}",
            self.persistent_ctx.table_id,
            self.persistent_ctx.group_id,
            self.persistent_ctx.region_mapping
        );

        self.table_metadata_manager
            .table_repart_manager()
            .update_mappings(
                self.persistent_ctx.table_id,
                &self.persistent_ctx.region_mapping,
            )
            .await
            .context(error::TableMetadataManagerSnafu)
    }

    /// Returns the next operation timeout.
    ///
    /// If the next operation timeout is not set, it will return `None`.
    pub fn next_operation_timeout(&self) -> Option<Duration> {
        self.persistent_ctx
            .timeout
            .checked_sub(self.start_time.elapsed())
    }

    /// Updates the elapsed time of entering staging region.
    pub fn update_enter_staging_region_elapsed(&mut self, elapsed: Duration) {
        self.volatile_ctx
            .metrics
            .update_enter_staging_region_elapsed(elapsed);
    }

    /// Updates the elapsed time of flushing pending deallocate regions.
    pub fn update_flush_pending_deallocate_regions_elapsed(&mut self, elapsed: Duration) {
        self.volatile_ctx
            .metrics
            .update_flush_pending_deallocate_regions_elapsed(elapsed);
    }

    /// Updates the elapsed time of applying staging manifest.
    pub fn update_apply_staging_manifest_elapsed(&mut self, elapsed: Duration) {
        self.volatile_ctx
            .metrics
            .update_apply_staging_manifest_elapsed(elapsed);
    }

    /// Updates the elapsed time of remapping manifest.
    pub fn update_remap_manifest_elapsed(&mut self, elapsed: Duration) {
        self.volatile_ctx
            .metrics
            .update_remap_manifest_elapsed(elapsed);
    }

    /// Updates the elapsed time of updating metadata.
    pub fn update_update_metadata_elapsed(&mut self, elapsed: Duration) {
        self.volatile_ctx
            .metrics
            .update_update_metadata_elapsed(elapsed);
    }
}

/// Returns the region routes of the given table route value.
///
/// Abort:
/// - Table route value is not physical.
pub fn region_routes(
    table_id: TableId,
    table_route_value: &TableRouteValue,
) -> Result<&Vec<RegionRoute>> {
    table_route_value
        .region_routes()
        .with_context(|_| error::UnexpectedLogicalRouteTableSnafu {
            err_msg: format!(
                "TableRoute({:?}) is a non-physical TableRouteValue.",
                table_id
            ),
        })
}

#[async_trait::async_trait]
#[typetag::serde(tag = "repartition_group_state")]
pub(crate) trait State: Sync + Send + Debug {
    fn name(&self) -> &'static str {
        let type_name = std::any::type_name::<Self>();
        // short name
        type_name.split("::").last().unwrap_or(type_name)
    }

    /// Yields the next [State] and [Status].
    async fn next(
        &mut self,
        ctx: &mut Context,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)>;

    fn as_any(&self) -> &dyn Any;
}

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use std::sync::Arc;
    use std::time::Duration;

    use common_meta::key::TableMetadataManager;
    use common_meta::kv_backend::test_util::MockKvBackendBuilder;
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use common_procedure::{Context as ProcedureContext, Procedure, ProcedureId};
    use common_procedure_test::MockContextProvider;
    use partition::expr::PartitionExpr;
    use store_api::storage::RegionId;

    use super::{
        Context, PersistentContext, RepartitionGroupProcedure, RepartitionStart, State,
        region_routes,
    };
    use crate::error::Error;
    use crate::procedure::repartition::dispatch::build_region_mapping;
    use crate::procedure::repartition::group::apply_staging_manifest::ApplyStagingManifest;
    use crate::procedure::repartition::group::enter_staging_region::EnterStagingRegion;
    use crate::procedure::repartition::group::remap_manifest::RemapManifest;
    use crate::procedure::repartition::group::repartition_start::RepartitionStart as GroupRepartitionStart;
    use crate::procedure::repartition::group::sync_region::SyncRegion;
    use crate::procedure::repartition::group::update_metadata::UpdateMetadata;
    use crate::procedure::repartition::plan;
    use crate::procedure::repartition::repartition_start::RepartitionStart as ParentRepartitionStart;
    use crate::procedure::repartition::test_util::{
        TestingEnv, new_persistent_context, range_expr,
    };

    struct GroupRollbackFixture {
        context: Context,
        original_region_routes: Vec<RegionRoute>,
        next_state: Option<Box<dyn State>>,
    }

    async fn new_group_rollback_fixture(
        original_region_routes: Vec<RegionRoute>,
        from_exprs: Vec<PartitionExpr>,
        to_exprs: Vec<PartitionExpr>,
        sync_region: bool,
    ) -> GroupRollbackFixture {
        let env = TestingEnv::new();
        let procedure_ctx = TestingEnv::procedure_context();
        let table_id = 1024;
        let mut next_region_number = 10;

        env.create_physical_table_metadata(table_id, original_region_routes.clone())
            .await;

        let (_, physical_route) = env
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(table_id)
            .await
            .unwrap();
        let allocation_plans =
            ParentRepartitionStart::build_plan(&physical_route, &from_exprs, &to_exprs).unwrap();
        assert_eq!(allocation_plans.len(), 1);

        let repartition_plan = plan::convert_allocation_plan_to_repartition_plan(
            table_id,
            &mut next_region_number,
            &allocation_plans[0],
        );
        let region_mapping = build_region_mapping(
            &repartition_plan.source_regions,
            &repartition_plan.target_regions,
            &repartition_plan.transition_map,
        );
        let persistent_context = PersistentContext::new(
            repartition_plan.group_id,
            table_id,
            "test_catalog".to_string(),
            "test_schema".to_string(),
            repartition_plan.source_regions,
            repartition_plan.target_regions,
            region_mapping,
            sync_region,
            repartition_plan.allocated_region_ids,
            repartition_plan.pending_deallocate_region_ids,
            Duration::from_secs(120),
        );
        let mut context = env.create_context(persistent_context);
        let (next_state, _) = GroupRepartitionStart
            .next(&mut context, &procedure_ctx)
            .await
            .unwrap();

        GroupRollbackFixture {
            context,
            original_region_routes,
            next_state: Some(next_state),
        }
    }

    async fn new_split_group_rollback_fixture(sync_region: bool) -> GroupRollbackFixture {
        new_group_rollback_fixture(
            vec![
                new_region_route(RegionId::new(1024, 1), Some(range_expr("x", 0, 100))),
                new_region_route(RegionId::new(1024, 2), Some(range_expr("x", 100, 200))),
                new_region_route(RegionId::new(1024, 10), None),
            ],
            vec![range_expr("x", 0, 100)],
            vec![range_expr("x", 0, 50), range_expr("x", 50, 100)],
            sync_region,
        )
        .await
    }

    async fn new_merge_group_rollback_fixture(sync_region: bool) -> GroupRollbackFixture {
        new_group_rollback_fixture(
            vec![
                new_region_route(RegionId::new(1024, 1), Some(range_expr("x", 0, 100))),
                new_region_route(RegionId::new(1024, 2), Some(range_expr("x", 100, 200))),
                new_region_route(RegionId::new(1024, 3), Some(range_expr("x", 200, 300))),
            ],
            vec![range_expr("x", 0, 100), range_expr("x", 100, 200)],
            vec![range_expr("x", 0, 200)],
            sync_region,
        )
        .await
    }

    async fn stage_metadata(context: &mut Context) {
        UpdateMetadata::ApplyStaging
            .apply_staging_regions(context)
            .await
            .unwrap();
    }

    fn new_region_route(region_id: RegionId, partition_expr: Option<PartitionExpr>) -> RegionRoute {
        RegionRoute {
            region: Region {
                id: region_id,
                partition_expr: partition_expr
                    .map(|expr| expr.as_json_str().unwrap())
                    .unwrap_or_default(),
                ..Default::default()
            },
            leader_peer: Some(Peer::empty(1)),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_get_table_route_value_not_found_error() {
        let env = TestingEnv::new();
        let persistent_context = new_persistent_context(1024, vec![], vec![]);
        let ctx = env.create_context(persistent_context);
        let err = ctx.get_table_route_value().await.unwrap_err();
        assert_matches!(err, Error::TableRouteNotFound { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_get_table_route_value_retry_error() {
        let kv = MockKvBackendBuilder::default()
            .range_fn(Arc::new(|_| {
                common_meta::error::UnexpectedSnafu {
                    err_msg: "mock err",
                }
                .fail()
            }))
            .build()
            .unwrap();
        let mut env = TestingEnv::new();
        env.table_metadata_manager = Arc::new(TableMetadataManager::new(Arc::new(kv)));
        let persistent_context = new_persistent_context(1024, vec![], vec![]);
        let ctx = env.create_context(persistent_context);
        let err = ctx.get_table_route_value().await.unwrap_err();
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn test_get_datanode_table_value_retry_error() {
        let kv = MockKvBackendBuilder::default()
            .range_fn(Arc::new(|_| {
                common_meta::error::UnexpectedSnafu {
                    err_msg: "mock err",
                }
                .fail()
            }))
            .build()
            .unwrap();
        let mut env = TestingEnv::new();
        env.table_metadata_manager = Arc::new(TableMetadataManager::new(Arc::new(kv)));
        let persistent_context = new_persistent_context(1024, vec![], vec![]);
        let ctx = env.create_context(persistent_context);
        let err = ctx.get_datanode_table_value(1024, 1).await.unwrap_err();
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn test_group_rollback_supported() {
        let env = TestingEnv::new();
        let persistent_context = new_persistent_context(1024, vec![], vec![]);
        let procedure = RepartitionGroupProcedure {
            state: Box::new(RepartitionStart),
            context: env.create_context(persistent_context),
        };

        assert!(procedure.rollback_supported());
    }

    #[tokio::test]
    async fn test_group_rollback_is_noop_before_apply_staging() {
        let env = TestingEnv::new();
        let persistent_context = new_persistent_context(1024, vec![], vec![]);
        let ctx = env.create_context(persistent_context.clone());
        let mut procedure = RepartitionGroupProcedure {
            state: Box::new(RepartitionStart),
            context: ctx,
        };
        let provider = Arc::new(MockContextProvider::new(Default::default()));
        let procedure_ctx = ProcedureContext {
            procedure_id: ProcedureId::random(),
            provider,
        };

        procedure.rollback(&procedure_ctx).await.unwrap();

        assert!(procedure.state.as_any().is::<RepartitionStart>());
        assert_eq!(procedure.context.persistent_ctx, persistent_context);
    }

    async fn assert_noop_rollback(
        fixture: GroupRollbackFixture,
        state: Box<dyn State>,
        assert_state: impl FnOnce(&dyn State),
    ) {
        let original_region_routes = fixture.original_region_routes.clone();
        let procedure_ctx = TestingEnv::procedure_context();
        let mut procedure = RepartitionGroupProcedure {
            state,
            context: fixture.context,
        };

        procedure.rollback(&procedure_ctx).await.unwrap();

        assert_state(&*procedure.state);
        let table_route_value = procedure
            .context
            .get_table_route_value()
            .await
            .unwrap()
            .into_inner();
        let region_routes = region_routes(
            procedure.context.persistent_ctx.table_id,
            &table_route_value,
        )
        .unwrap();
        assert_eq!(region_routes.clone(), original_region_routes);
    }

    async fn assert_metadata_rollback_restores_table_route(
        mut fixture: GroupRollbackFixture,
        state: Box<dyn State>,
    ) {
        let original_region_routes = fixture.original_region_routes.clone();
        let procedure_ctx = TestingEnv::procedure_context();
        stage_metadata(&mut fixture.context).await;
        let mut procedure = RepartitionGroupProcedure {
            state,
            context: fixture.context,
        };

        procedure.rollback(&procedure_ctx).await.unwrap();

        let table_route_value = procedure
            .context
            .get_table_route_value()
            .await
            .unwrap()
            .into_inner();
        let region_routes = region_routes(
            procedure.context.persistent_ctx.table_id,
            &table_route_value,
        )
        .unwrap();
        assert_eq!(region_routes.clone(), original_region_routes);
    }

    #[tokio::test]
    async fn test_group_rollback_is_noop_in_sync_region() {
        let mut fixture = new_split_group_rollback_fixture(true).await;
        assert!(
            fixture
                .next_state
                .as_ref()
                .unwrap()
                .as_any()
                .is::<SyncRegion>()
        );
        let state = fixture.next_state.take().unwrap();

        assert_noop_rollback(fixture, state, |state| {
            assert!(state.as_any().is::<SyncRegion>());
        })
        .await;
    }

    #[tokio::test]
    async fn test_group_rollback_is_noop_in_exit_staging() {
        let fixture = new_split_group_rollback_fixture(false).await;

        assert_noop_rollback(fixture, Box::new(UpdateMetadata::ExitStaging), |state| {
            assert!(state.as_any().is::<UpdateMetadata>());
            assert!(matches!(
                state.as_any().downcast_ref::<UpdateMetadata>(),
                Some(UpdateMetadata::ExitStaging)
            ));
        })
        .await;
    }

    #[tokio::test]
    async fn test_group_rollback_restores_split_routes_from_apply_staging() {
        let fixture = new_split_group_rollback_fixture(false).await;
        assert_metadata_rollback_restores_table_route(
            fixture,
            Box::new(UpdateMetadata::ApplyStaging),
        )
        .await;
    }

    #[tokio::test]
    async fn test_group_rollback_restores_split_routes_from_enter_staging_region() {
        let fixture = new_split_group_rollback_fixture(false).await;
        assert_metadata_rollback_restores_table_route(fixture, Box::new(EnterStagingRegion)).await;
    }

    #[tokio::test]
    async fn test_group_rollback_restores_split_routes_from_remap_manifest() {
        let fixture = new_split_group_rollback_fixture(false).await;
        assert_metadata_rollback_restores_table_route(fixture, Box::new(RemapManifest)).await;
    }

    #[tokio::test]
    async fn test_group_rollback_restores_split_routes_from_apply_staging_manifest() {
        let fixture = new_split_group_rollback_fixture(false).await;
        assert_metadata_rollback_restores_table_route(fixture, Box::new(ApplyStagingManifest))
            .await;
    }

    #[tokio::test]
    async fn test_group_rollback_restores_merge_routes_and_is_idempotent() {
        let mut fixture = new_merge_group_rollback_fixture(false).await;
        let original_region_routes = fixture.original_region_routes.clone();
        let procedure_ctx = TestingEnv::procedure_context();
        stage_metadata(&mut fixture.context).await;
        let mut procedure = RepartitionGroupProcedure {
            state: Box::new(UpdateMetadata::ApplyStaging),
            context: fixture.context,
        };

        procedure.rollback(&procedure_ctx).await.unwrap();
        let table_route_value = procedure
            .context
            .get_table_route_value()
            .await
            .unwrap()
            .into_inner();
        let once = region_routes(
            procedure.context.persistent_ctx.table_id,
            &table_route_value,
        )
        .unwrap()
        .clone();
        procedure.rollback(&procedure_ctx).await.unwrap();
        let table_route_value = procedure
            .context
            .get_table_route_value()
            .await
            .unwrap()
            .into_inner();
        let twice = region_routes(
            procedure.context.persistent_ctx.table_id,
            &table_route_value,
        )
        .unwrap()
        .clone();

        assert_eq!(once, original_region_routes);
        assert_eq!(once, twice);
    }
}
