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

pub mod allocate_region;
pub mod collect;
pub mod deallocate_region;
pub mod dispatch;
pub mod group;
pub mod plan;
pub mod repartition_end;
pub mod repartition_start;
pub mod utils;

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::time::{Duration, Instant};

use common_error::ext::BoxedError;
use common_meta::cache_invalidator::CacheInvalidatorRef;
use common_meta::ddl::DdlContext;
use common_meta::ddl::allocator::region_routes::RegionRoutesAllocatorRef;
use common_meta::ddl::allocator::wal_options::WalOptionsAllocatorRef;
use common_meta::ddl_manager::RepartitionProcedureFactory;
use common_meta::instruction::CacheIdent;
use common_meta::key::datanode_table::RegionInfo;
use common_meta::key::table_info::TableInfoValue;
use common_meta::key::table_route::TableRouteValue;
use common_meta::key::{DeserializedValueWithBytes, TableMetadataManagerRef};
use common_meta::lock_key::{CatalogLock, SchemaLock, TableLock, TableNameLock};
use common_meta::node_manager::NodeManagerRef;
use common_meta::region_keeper::{MemoryRegionKeeperRef, OperatingRegionGuard};
use common_meta::region_registry::LeaderRegionRegistryRef;
use common_meta::rpc::router::{RegionRoute, operating_leader_region_roles};
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    BoxedProcedure, Context as ProcedureContext, Error as ProcedureError, LockKey, Procedure,
    ProcedureManagerRef, Result as ProcedureResult, Status, StringKey, UserMetadata,
};
use common_telemetry::{error, info, warn};
use partition::expr::PartitionExpr;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::storage::{RegionNumber, TableId};
use table::table_name::TableName;

use crate::error::{self, Result};
use crate::procedure::repartition::collect::ProcedureMeta;
use crate::procedure::repartition::deallocate_region::DeallocateRegion;
use crate::procedure::repartition::group::{
    Context as RepartitionGroupContext, RepartitionGroupProcedure,
};
use crate::procedure::repartition::plan::RepartitionPlanEntry;
use crate::procedure::repartition::repartition_start::RepartitionStart;
use crate::procedure::repartition::utils::get_datanode_table_value;
use crate::service::mailbox::MailboxRef;

#[cfg(test)]
pub mod test_util;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistentContext {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub table_id: TableId,
    pub plans: Vec<RepartitionPlanEntry>,
    /// Records failed sub-procedures for metadata rollback.
    #[serde(default)]
    pub failed_procedures: Vec<ProcedureMeta>,
    #[serde(default)]
    /// Records unknown sub-procedures for metadata rollback.
    pub unknown_procedures: Vec<ProcedureMeta>,
    /// The timeout for repartition operations.
    #[serde(with = "humantime_serde", default = "default_timeout")]
    pub timeout: Duration,
}

fn default_timeout() -> Duration {
    Duration::from_mins(2)
}

impl PersistentContext {
    /// Creates a new [PersistentContext] with the given table name, table id and timeout.
    ///
    /// If the timeout is not provided, the default timeout will be used.
    pub fn new(
        TableName {
            catalog_name,
            schema_name,
            table_name,
        }: TableName,
        table_id: TableId,
        timeout: Option<Duration>,
    ) -> Self {
        Self {
            catalog_name,
            schema_name,
            table_name,
            table_id,
            plans: vec![],
            failed_procedures: vec![],
            unknown_procedures: vec![],
            timeout: timeout.unwrap_or_else(default_timeout),
        }
    }

    pub fn lock_key(&self) -> Vec<StringKey> {
        vec![
            CatalogLock::Read(&self.catalog_name).into(),
            SchemaLock::read(&self.catalog_name, &self.schema_name).into(),
            TableLock::Write(self.table_id).into(),
            TableNameLock::new(&self.catalog_name, &self.schema_name, &self.table_name).into(),
        ]
    }
}

#[derive(Clone)]
pub struct Context {
    pub persistent_ctx: PersistentContext,
    pub volatile_ctx: VolatileContext,
    pub table_metadata_manager: TableMetadataManagerRef,
    pub memory_region_keeper: MemoryRegionKeeperRef,
    pub node_manager: NodeManagerRef,
    pub leader_region_registry: LeaderRegionRegistryRef,
    pub mailbox: MailboxRef,
    pub server_addr: String,
    pub cache_invalidator: CacheInvalidatorRef,
    pub region_routes_allocator: RegionRoutesAllocatorRef,
    pub wal_options_allocator: WalOptionsAllocatorRef,
    pub start_time: Instant,
}

#[derive(Debug, Clone, Default)]
pub struct VolatileContext {
    pub metrics: Metrics,
    pub dispatch_start_time: Option<Instant>,
}

/// Metrics of repartition.
#[derive(Debug, Clone, Default)]
pub struct Metrics {
    /// Elapsed time of building plan.
    build_plan_elapsed: Duration,
    /// Elapsed time of allocating region.
    allocate_region_elapsed: Duration,
    /// Elapsed time of finishing groups.
    finish_groups_elapsed: Duration,
    /// Elapsed time of deallocating region.
    deallocate_region_elapsed: Duration,
}

impl Display for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let total = self.build_plan_elapsed
            + self.allocate_region_elapsed
            + self.finish_groups_elapsed
            + self.deallocate_region_elapsed;
        write!(f, "total: {:?}", total)?;
        let mut parts = Vec::with_capacity(4);
        if self.build_plan_elapsed > Duration::ZERO {
            parts.push(format!("build_plan_elapsed: {:?}", self.build_plan_elapsed));
        }
        if self.allocate_region_elapsed > Duration::ZERO {
            parts.push(format!(
                "allocate_region_elapsed: {:?}",
                self.allocate_region_elapsed
            ));
        }
        if self.finish_groups_elapsed > Duration::ZERO {
            parts.push(format!(
                "finish_groups_elapsed: {:?}",
                self.finish_groups_elapsed
            ));
        }
        if self.deallocate_region_elapsed > Duration::ZERO {
            parts.push(format!(
                "deallocate_region_elapsed: {:?}",
                self.deallocate_region_elapsed
            ));
        }

        if !parts.is_empty() {
            write!(f, ", {}", parts.join(", "))?;
        }
        Ok(())
    }
}

impl Metrics {
    /// Updates the elapsed time of building plan.
    pub fn update_build_plan_elapsed(&mut self, elapsed: Duration) {
        self.build_plan_elapsed += elapsed;
    }

    /// Updates the elapsed time of allocating region.
    pub fn update_allocate_region_elapsed(&mut self, elapsed: Duration) {
        self.allocate_region_elapsed += elapsed;
    }

    /// Updates the elapsed time of finishing groups.
    pub fn update_finish_groups_elapsed(&mut self, elapsed: Duration) {
        self.finish_groups_elapsed += elapsed;
    }

    /// Updates the elapsed time of deallocating region.
    pub fn update_deallocate_region_elapsed(&mut self, elapsed: Duration) {
        self.deallocate_region_elapsed += elapsed;
    }
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
            table_metadata_manager: ddl_ctx.table_metadata_manager.clone(),
            memory_region_keeper: ddl_ctx.memory_region_keeper.clone(),
            node_manager: ddl_ctx.node_manager.clone(),
            leader_region_registry: ddl_ctx.leader_region_registry.clone(),
            mailbox,
            server_addr,
            cache_invalidator: ddl_ctx.cache_invalidator.clone(),
            region_routes_allocator: ddl_ctx.table_metadata_allocator.region_routes_allocator(),
            wal_options_allocator: ddl_ctx.table_metadata_allocator.wal_options_allocator(),
            start_time: Instant::now(),
            volatile_ctx: VolatileContext::default(),
        }
    }

    /// Returns the next operation's timeout.
    pub fn next_operation_timeout(&self) -> Option<Duration> {
        self.persistent_ctx
            .timeout
            .checked_sub(self.start_time.elapsed())
    }

    /// Updates the elapsed time of building plan.
    pub fn update_build_plan_elapsed(&mut self, elapsed: Duration) {
        self.volatile_ctx.metrics.update_build_plan_elapsed(elapsed);
    }

    /// Updates the elapsed time of allocating region.
    pub fn update_allocate_region_elapsed(&mut self, elapsed: Duration) {
        self.volatile_ctx
            .metrics
            .update_allocate_region_elapsed(elapsed);
    }

    /// Updates the elapsed time of finishing groups.
    pub fn update_finish_groups_elapsed(&mut self, elapsed: Duration) {
        self.volatile_ctx
            .metrics
            .update_finish_groups_elapsed(elapsed);
    }

    /// Updates the elapsed time of deallocating region.
    pub fn update_deallocate_region_elapsed(&mut self, elapsed: Duration) {
        self.volatile_ctx
            .metrics
            .update_deallocate_region_elapsed(elapsed);
    }

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
        let table_route_value = self
            .table_metadata_manager
            .table_route_manager()
            .table_route_storage()
            .get_with_raw_bytes(table_id)
            .await
            .map_err(BoxedError::new)
            .with_context(|_| error::RetryLaterWithSourceSnafu {
                reason: format!("Failed to get table route for table: {}", table_id),
            })?
            .context(error::TableRouteNotFoundSnafu { table_id })?;

        Ok(table_route_value)
    }

    /// Retrieves the table info value for the given table id.
    ///
    /// Retry:
    /// - Failed to retrieve the metadata of table.
    ///
    /// Abort:
    /// - Table info not found.
    pub async fn get_table_info_value(&self) -> Result<TableInfoValue> {
        let table_id = self.persistent_ctx.table_id;
        let table_info_value = self
            .table_metadata_manager
            .table_info_manager()
            .get(table_id)
            .await
            .map_err(BoxedError::new)
            .with_context(|_| error::RetryLaterWithSourceSnafu {
                reason: format!("Failed to get table info for table: {}", table_id),
            })?
            .context(error::TableInfoNotFoundSnafu { table_id })?
            .into_inner();
        Ok(table_info_value)
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
        new_region_wal_options: HashMap<RegionNumber, String>,
    ) -> Result<()> {
        let table_id = self.persistent_ctx.table_id;
        if new_region_routes.is_empty() {
            return error::UnexpectedSnafu {
                violated: format!("new_region_routes is empty for table: {}", table_id),
            }
            .fail();
        }
        let datanode_id = new_region_routes
            .first()
            .unwrap()
            .leader_peer
            .as_ref()
            .context(error::NoLeaderSnafu)?
            .id;
        let datanode_table_value =
            get_datanode_table_value(&self.table_metadata_manager, table_id, datanode_id).await?;

        let RegionInfo {
            region_options,
            region_wal_options,
            ..
        } = &datanode_table_value.region_info;

        // Merge and validate the new region wal options.
        let validated_region_wal_options =
            crate::procedure::repartition::utils::merge_and_validate_region_wal_options(
                region_wal_options,
                new_region_wal_options,
                &new_region_routes,
                table_id,
            )?;
        info!(
            "Updating table route for table: {}, new region routes: {:?}",
            table_id, new_region_routes
        );
        self.table_metadata_manager
            .update_table_route(
                table_id,
                datanode_table_value.region_info.clone(),
                current_table_route_value,
                new_region_routes,
                region_options,
                &validated_region_wal_options,
            )
            .await
            .context(error::TableMetadataManagerSnafu)
    }

    /// Broadcasts the invalidate table cache message.
    pub async fn invalidate_table_cache(&self) -> Result<()> {
        let table_id = self.persistent_ctx.table_id;
        let subject = format!(
            "Invalidate table cache for repartition table, table: {}",
            table_id,
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

    pub fn register_operating_regions(
        memory_region_keeper: &MemoryRegionKeeperRef,
        region_routes: &[RegionRoute],
    ) -> Result<Vec<OperatingRegionGuard>> {
        let mut operating_guards = Vec::with_capacity(region_routes.len());
        for (region_id, datanode_id, role) in operating_leader_region_roles(region_routes) {
            let guard = memory_region_keeper
                .register_with_role(datanode_id, region_id, role)
                .context(error::RegionOperatingRaceSnafu {
                    peer_id: datanode_id,
                    region_id,
                })?;
            operating_guards.push(guard);
        }
        Ok(operating_guards)
    }
}

#[async_trait::async_trait]
#[typetag::serde(tag = "repartition_state")]
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

pub struct RepartitionProcedure {
    state: Box<dyn State>,
    context: Context,
}

#[derive(Debug, Serialize)]
struct RepartitionData<'a> {
    state: &'a dyn State,
    persistent_ctx: &'a PersistentContext,
}

#[derive(Debug, Deserialize)]
struct RepartitionDataOwned {
    state: Box<dyn State>,
    persistent_ctx: PersistentContext,
}

impl RepartitionProcedure {
    const TYPE_NAME: &'static str = "metasrv-procedure::Repartition";

    pub fn new(
        from_exprs: Vec<PartitionExpr>,
        to_exprs: Vec<PartitionExpr>,
        context: Context,
    ) -> Self {
        let state = Box::new(RepartitionStart::new(from_exprs, to_exprs));

        Self { state, context }
    }

    pub fn from_json<F>(json: &str, ctx_factory: F) -> ProcedureResult<Self>
    where
        F: FnOnce(PersistentContext) -> Context,
    {
        let RepartitionDataOwned {
            state,
            persistent_ctx,
        } = serde_json::from_str(json).context(FromJsonSnafu)?;
        let context = ctx_factory(persistent_ctx);

        Ok(Self { state, context })
    }

    /// Returns whether parent rollback should remove this repartition's allocated regions.
    ///
    /// This uses an "after AllocateRegion" semantic: once execution reaches
    /// `AllocateRegion` or any later state, rollback must try to remove this round's
    /// `allocated_region_ids` from table-route metadata when they exist.
    ///
    /// State flow:
    /// `RepartitionStart -> AllocateRegion -> Dispatch -> Collect -> DeallocateRegion -> RepartitionEnd`
    ///                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ///                     rollback allocated regions in metadata
    ///
    /// Notes:
    /// - `RepartitionStart`: no-op, because allocation has not happened yet.
    /// - `AllocateRegion` / `Dispatch` / `Collect`  rollback-active.
    /// - `DeallocateRegion`: is not rollback-active.
    /// - `RepartitionEnd`: no-op.
    fn should_rollback_allocated_regions(&self) -> bool {
        self.state.as_any().is::<allocate_region::AllocateRegion>()
            || self.state.as_any().is::<dispatch::Dispatch>()
            || self.state.as_any().is::<collect::Collect>()
    }

    fn rollback_allocated_region_ids(&self) -> HashSet<store_api::storage::RegionId> {
        if self.state.as_any().is::<allocate_region::AllocateRegion>()
            || self.state.as_any().is::<dispatch::Dispatch>()
        {
            return self
                .context
                .persistent_ctx
                .plans
                .iter()
                .flat_map(|plan| plan.allocated_region_ids.iter().copied())
                .collect();
        }

        self.context
            .persistent_ctx
            .failed_procedures
            .iter()
            .chain(self.context.persistent_ctx.unknown_procedures.iter())
            .flat_map(|procedure_meta| {
                let plan_index = procedure_meta.plan_index;
                self.context.persistent_ctx.plans[plan_index]
                    .allocated_region_ids
                    .iter()
                    .copied()
            })
            .collect()
    }

    fn filter_allocated_region_routes(
        region_routes: &[RegionRoute],
        allocated_region_ids: &HashSet<store_api::storage::RegionId>,
    ) -> Vec<RegionRoute> {
        region_routes
            .iter()
            .filter(|route| !allocated_region_ids.contains(&route.region.id))
            .cloned()
            .collect()
    }

    async fn rollback_inner(&mut self, procedure_ctx: &ProcedureContext) -> Result<()> {
        if !self.should_rollback_allocated_regions() {
            return Ok(());
        }

        let table_id = self.context.persistent_ctx.table_id;
        let allocated_region_ids = self.rollback_allocated_region_ids();
        if allocated_region_ids.is_empty() {
            return Ok(());
        }

        let table_lock = TableLock::Write(table_id).into();
        let _guard = procedure_ctx.provider.acquire_lock(&table_lock).await;
        let table_route_value = self.context.get_table_route_value().await?;
        let current_region_routes = table_route_value.region_routes().unwrap();
        let allocated_region_routes = DeallocateRegion::filter_deallocatable_region_routes(
            table_id,
            current_region_routes,
            &allocated_region_ids,
        );
        if !allocated_region_routes.is_empty() {
            let table = TableName {
                catalog_name: self.context.persistent_ctx.catalog_name.clone(),
                schema_name: self.context.persistent_ctx.schema_name.clone(),
                table_name: self.context.persistent_ctx.table_name.clone(),
            };
            // Memory guards are not required here,
            // because the table metadata still contains routes for the deallocating regions.
            if let Err(err) = DeallocateRegion::deallocate_regions(
                &self.context.node_manager,
                &self.context.leader_region_registry,
                table,
                table_id,
                &allocated_region_routes,
            )
            .await
            {
                warn!(err; "Failed to drop allocated regions during repartition rollback, table_id: {}, regions: {:?}", table_id, allocated_region_ids);
            }
        }

        let new_region_routes =
            Self::filter_allocated_region_routes(current_region_routes, &allocated_region_ids);

        if new_region_routes.len() != current_region_routes.len() {
            self.context
                .update_table_route(&table_route_value, new_region_routes, HashMap::new())
                .await
                .map_err(BoxedError::new)
                .with_context(|_| error::RetryLaterWithSourceSnafu {
                    reason: format!(
                        "Failed to rollback allocated region routes for repartition table: {}",
                        table_id
                    ),
                })?;
        }

        if let Err(err) = self.context.invalidate_table_cache().await {
            warn!(err; "Failed to invalidate table cache during repartition rollback, table_id: {}", table_id);
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl Procedure for RepartitionProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    #[tracing::instrument(skip_all, fields(
        state = %self.state.name(),
        table_id = %self.context.persistent_ctx.table_id
    ))]
    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &mut self.state;
        let state_name = state.name();
        // Log state transition
        common_telemetry::info!(
            "Repartition procedure executing state: {}, table_id: {}",
            state_name,
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
                        "Repartition procedure failed, table id: {}",
                        self.context.persistent_ctx.table_id,
                    );
                    Err(ProcedureError::external(e))
                }
            }
        }
    }

    async fn rollback(&mut self, ctx: &ProcedureContext) -> ProcedureResult<()> {
        self.rollback_inner(ctx)
            .await
            .map_err(ProcedureError::external)
    }

    fn rollback_supported(&self) -> bool {
        true
    }

    fn dump(&self) -> ProcedureResult<String> {
        let data = RepartitionData {
            state: self.state.as_ref(),
            persistent_ctx: &self.context.persistent_ctx,
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

pub struct DefaultRepartitionProcedureFactory {
    mailbox: MailboxRef,
    server_addr: String,
}

impl DefaultRepartitionProcedureFactory {
    pub fn new(mailbox: MailboxRef, server_addr: String) -> Self {
        Self {
            mailbox,
            server_addr,
        }
    }
}

impl RepartitionProcedureFactory for DefaultRepartitionProcedureFactory {
    fn create(
        &self,
        ddl_ctx: &DdlContext,
        table_name: TableName,
        table_id: TableId,
        from_exprs: Vec<String>,
        to_exprs: Vec<String>,
        timeout: Option<Duration>,
    ) -> std::result::Result<BoxedProcedure, BoxedError> {
        let persistent_ctx = PersistentContext::new(table_name, table_id, timeout);
        let from_exprs = from_exprs
            .iter()
            .map(|e| {
                PartitionExpr::from_json_str(e)
                    .context(error::DeserializePartitionExprSnafu)?
                    .context(error::EmptyPartitionExprSnafu)
            })
            .collect::<Result<Vec<_>>>()
            .map_err(BoxedError::new)?;
        let to_exprs = to_exprs
            .iter()
            .map(|e| {
                PartitionExpr::from_json_str(e)
                    .context(error::DeserializePartitionExprSnafu)?
                    .context(error::EmptyPartitionExprSnafu)
            })
            .collect::<Result<Vec<_>>>()
            .map_err(BoxedError::new)?;

        let procedure = RepartitionProcedure::new(
            from_exprs,
            to_exprs,
            Context::new(
                ddl_ctx,
                self.mailbox.clone(),
                self.server_addr.clone(),
                persistent_ctx,
            ),
        );

        Ok(Box::new(procedure))
    }

    fn register_loaders(
        &self,
        ddl_ctx: &DdlContext,
        procedure_manager: &ProcedureManagerRef,
    ) -> std::result::Result<(), BoxedError> {
        // Registers the repartition procedure loader.
        let mailbox = self.mailbox.clone();
        let server_addr = self.server_addr.clone();
        let moved_ddl_ctx = ddl_ctx.clone();
        procedure_manager
            .register_loader(
                RepartitionProcedure::TYPE_NAME,
                Box::new(move |json| {
                    let mailbox = mailbox.clone();
                    let server_addr = server_addr.clone();
                    let ddl_ctx = moved_ddl_ctx.clone();
                    let factory = move |persistent_ctx| {
                        Context::new(&ddl_ctx, mailbox, server_addr, persistent_ctx)
                    };
                    RepartitionProcedure::from_json(json, factory).map(|p| Box::new(p) as _)
                }),
            )
            .map_err(BoxedError::new)?;

        // Registers the repartition group procedure loader.
        let mailbox = self.mailbox.clone();
        let server_addr = self.server_addr.clone();
        let moved_ddl_ctx = ddl_ctx.clone();
        procedure_manager
            .register_loader(
                RepartitionGroupProcedure::TYPE_NAME,
                Box::new(move |json| {
                    let mailbox = mailbox.clone();
                    let server_addr = server_addr.clone();
                    let ddl_ctx = moved_ddl_ctx.clone();
                    let factory = move |persistent_ctx| {
                        RepartitionGroupContext::new(&ddl_ctx, mailbox, server_addr, persistent_ctx)
                    };
                    RepartitionGroupProcedure::from_json(json, factory).map(|p| Box::new(p) as _)
                }),
            )
            .map_err(BoxedError::new)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use common_error::ext::BoxedError;
    use common_error::mock::MockError;
    use common_error::status_code::StatusCode;
    use common_meta::ddl::test_util::datanode_handler::{
        DatanodeWatcher, NaiveDatanodeHandler, UnexpectedErrorDatanodeHandler,
    };
    use common_meta::error;
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use common_meta::test_util::MockDatanodeManager;
    use common_procedure::{Error as ProcedureError, Procedure, ProcedureId, ProcedureState};
    use store_api::storage::RegionId;
    use table::table_name::TableName;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use super::*;
    use crate::procedure::repartition::allocate_region::AllocateRegion;
    use crate::procedure::repartition::collect::Collect;
    use crate::procedure::repartition::deallocate_region::DeallocateRegion;
    use crate::procedure::repartition::dispatch::Dispatch;
    use crate::procedure::repartition::plan::RegionDescriptor;
    use crate::procedure::repartition::repartition_end::RepartitionEnd;
    use crate::procedure::repartition::test_util::{
        TestingEnv, assert_parent_state, current_parent_region_routes, extract_subprocedure_ids,
        new_parent_context, procedure_context_with_receivers, procedure_state_receiver, range_expr,
        test_region_route, test_region_wal_options,
    };

    fn test_plan(table_id: TableId) -> RepartitionPlanEntry {
        RepartitionPlanEntry {
            group_id: uuid::Uuid::new_v4(),
            source_regions: vec![RegionDescriptor {
                region_id: RegionId::new(table_id, 1),
                partition_expr: range_expr("x", 0, 100),
            }],
            target_regions: vec![
                RegionDescriptor {
                    region_id: RegionId::new(table_id, 1),
                    partition_expr: range_expr("x", 0, 50),
                },
                RegionDescriptor {
                    region_id: RegionId::new(table_id, 3),
                    partition_expr: range_expr("x", 50, 100),
                },
            ],
            allocated_region_ids: vec![RegionId::new(table_id, 3)],
            pending_deallocate_region_ids: vec![],
            transition_map: vec![vec![0, 1]],
        }
    }

    fn test_procedure(state: Box<dyn State>, context: Context) -> RepartitionProcedure {
        RepartitionProcedure { state, context }
    }

    fn test_context(env: &TestingEnv, table_id: TableId) -> Context {
        let node_manager = Arc::new(MockDatanodeManager::new(UnexpectedErrorDatanodeHandler));
        let ddl_ctx = env.ddl_context(node_manager);
        let persistent_ctx = PersistentContext::new(
            TableName::new("test_catalog", "test_schema", "test_table"),
            table_id,
            None,
        );

        Context::new(
            &ddl_ctx,
            env.mailbox_ctx.mailbox().clone(),
            env.server_addr.clone(),
            persistent_ctx,
        )
    }

    #[test]
    fn test_filter_allocated_region_routes() {
        let table_id = 1024;
        let region_routes = vec![
            test_region_route(RegionId::new(table_id, 1), "a"),
            test_region_route(RegionId::new(table_id, 2), "b"),
        ];
        let allocated_region_ids = HashSet::from([RegionId::new(table_id, 2)]);

        let new_region_routes = RepartitionProcedure::filter_allocated_region_routes(
            &region_routes,
            &allocated_region_ids,
        );

        assert_eq!(new_region_routes.len(), 1);
        assert_eq!(new_region_routes[0].region.id, RegionId::new(table_id, 1));
    }

    #[test]
    fn test_should_rollback_allocated_regions() {
        let env = TestingEnv::new();
        let table_id = 1024;

        let procedure = test_procedure(
            Box::new(RepartitionStart::new(vec![], vec![])),
            test_context(&env, table_id),
        );
        assert!(!procedure.should_rollback_allocated_regions());

        let procedure = test_procedure(
            Box::new(AllocateRegion::new(vec![])),
            test_context(&env, table_id),
        );
        assert!(procedure.should_rollback_allocated_regions());

        let procedure = test_procedure(Box::new(Dispatch), test_context(&env, table_id));
        assert!(procedure.should_rollback_allocated_regions());

        let procedure =
            test_procedure(Box::new(Collect::new(vec![])), test_context(&env, table_id));
        assert!(procedure.should_rollback_allocated_regions());

        let procedure = test_procedure(Box::new(DeallocateRegion), test_context(&env, table_id));
        assert!(!procedure.should_rollback_allocated_regions());

        let procedure = test_procedure(Box::new(RepartitionEnd), test_context(&env, table_id));
        assert!(!procedure.should_rollback_allocated_regions());
    }

    #[tokio::test]
    async fn test_repartition_rollback_removes_allocated_routes_from_dispatch() {
        let env = TestingEnv::new();
        let table_id = 1024;
        let node_manager = Arc::new(MockDatanodeManager::new(UnexpectedErrorDatanodeHandler));
        let ddl_ctx = env.ddl_context(node_manager);
        let original_region_routes = vec![
            test_region_route(
                RegionId::new(table_id, 1),
                &range_expr("x", 0, 100).as_json_str().unwrap(),
            ),
            test_region_route(
                RegionId::new(table_id, 2),
                &range_expr("x", 50, 100).as_json_str().unwrap(),
            ),
            test_region_route(RegionId::new(table_id, 3), ""),
        ];
        env.create_physical_table_metadata_with_wal_options(
            table_id,
            original_region_routes,
            test_region_wal_options(&[1, 2]),
        )
        .await;

        let mut persistent_ctx = PersistentContext::new(
            TableName::new("test_catalog", "test_schema", "test_table"),
            table_id,
            None,
        );
        persistent_ctx.plans = vec![test_plan(table_id)];
        persistent_ctx.failed_procedures = vec![ProcedureMeta {
            plan_index: 0,
            group_id: Uuid::new_v4(),
            procedure_id: ProcedureId::random(),
        }];
        let context = Context::new(
            &ddl_ctx,
            env.mailbox_ctx.mailbox().clone(),
            env.server_addr.clone(),
            persistent_ctx,
        );
        let mut procedure = RepartitionProcedure {
            state: Box::new(Dispatch),
            context,
        };

        procedure
            .rollback(&TestingEnv::procedure_context())
            .await
            .unwrap();

        let region_routes = current_parent_region_routes(&procedure.context).await;
        assert_eq!(region_routes.len(), 2);
        assert_eq!(region_routes[0].region.id, RegionId::new(table_id, 1));
        assert_eq!(region_routes[1].region.id, RegionId::new(table_id, 2));
    }

    #[tokio::test]
    async fn test_repartition_rollback_removes_allocated_routes_from_allocate() {
        let env = TestingEnv::new();
        let table_id = 1024;
        let node_manager = Arc::new(MockDatanodeManager::new(UnexpectedErrorDatanodeHandler));
        let ddl_ctx = env.ddl_context(node_manager);
        let original_region_routes = vec![
            test_region_route(
                RegionId::new(table_id, 1),
                &range_expr("x", 0, 100).as_json_str().unwrap(),
            ),
            test_region_route(
                RegionId::new(table_id, 2),
                &range_expr("x", 50, 100).as_json_str().unwrap(),
            ),
            test_region_route(RegionId::new(table_id, 3), ""),
        ];
        env.create_physical_table_metadata_with_wal_options(
            table_id,
            original_region_routes,
            test_region_wal_options(&[1, 2]),
        )
        .await;

        let mut persistent_ctx = PersistentContext::new(
            TableName::new("test_catalog", "test_schema", "test_table"),
            table_id,
            None,
        );
        persistent_ctx.plans = vec![test_plan(table_id)];
        let context = Context::new(
            &ddl_ctx,
            env.mailbox_ctx.mailbox().clone(),
            env.server_addr.clone(),
            persistent_ctx,
        );
        let mut procedure = RepartitionProcedure {
            state: Box::new(AllocateRegion::new(vec![])),
            context,
        };

        procedure
            .rollback(&TestingEnv::procedure_context())
            .await
            .unwrap();

        let region_routes = current_parent_region_routes(&procedure.context).await;
        assert_eq!(region_routes.len(), 2);
        assert_eq!(region_routes[0].region.id, RegionId::new(table_id, 1));
        assert_eq!(region_routes[1].region.id, RegionId::new(table_id, 2));
    }

    #[tokio::test]
    async fn test_repartition_rollback_from_collect_only_removes_failed_allocated_routes() {
        let env = TestingEnv::new();
        let table_id = 1024;
        let node_manager = Arc::new(MockDatanodeManager::new(UnexpectedErrorDatanodeHandler));
        let ddl_ctx = env.ddl_context(node_manager);
        let original_region_routes = vec![
            test_region_route(
                RegionId::new(table_id, 1),
                &range_expr("x", 0, 100).as_json_str().unwrap(),
            ),
            test_region_route(
                RegionId::new(table_id, 2),
                &range_expr("x", 100, 200).as_json_str().unwrap(),
            ),
            test_region_route(RegionId::new(table_id, 3), ""),
            test_region_route(RegionId::new(table_id, 4), ""),
        ];
        env.create_physical_table_metadata_with_wal_options(
            table_id,
            original_region_routes,
            test_region_wal_options(&[1, 2, 3, 4]),
        )
        .await;

        let mut persistent_ctx = PersistentContext::new(
            TableName::new("test_catalog", "test_schema", "test_table"),
            table_id,
            None,
        );
        let failed_plan = test_plan(table_id);
        let succeeded_plan = RepartitionPlanEntry {
            group_id: Uuid::new_v4(),
            source_regions: vec![RegionDescriptor {
                region_id: RegionId::new(table_id, 2),
                partition_expr: range_expr("x", 100, 200),
            }],
            target_regions: vec![
                RegionDescriptor {
                    region_id: RegionId::new(table_id, 2),
                    partition_expr: range_expr("x", 100, 150),
                },
                RegionDescriptor {
                    region_id: RegionId::new(table_id, 4),
                    partition_expr: range_expr("x", 150, 200),
                },
            ],
            allocated_region_ids: vec![RegionId::new(table_id, 4)],
            pending_deallocate_region_ids: vec![],
            transition_map: vec![vec![0]],
        };
        persistent_ctx.plans = vec![failed_plan, succeeded_plan];
        persistent_ctx.failed_procedures = vec![ProcedureMeta {
            plan_index: 0,
            group_id: persistent_ctx.plans[0].group_id,
            procedure_id: ProcedureId::random(),
        }];

        let context = Context::new(
            &ddl_ctx,
            env.mailbox_ctx.mailbox().clone(),
            env.server_addr.clone(),
            persistent_ctx,
        );
        let mut procedure = RepartitionProcedure {
            state: Box::new(Collect::new(vec![])),
            context,
        };

        procedure
            .rollback(&TestingEnv::procedure_context())
            .await
            .unwrap();

        let region_routes = current_parent_region_routes(&procedure.context).await;
        assert_eq!(region_routes.len(), 3);
        assert_eq!(region_routes[0].region.id, RegionId::new(table_id, 1));
        assert_eq!(region_routes[1].region.id, RegionId::new(table_id, 2));
        assert_eq!(region_routes[2].region.id, RegionId::new(table_id, 4));
    }

    #[tokio::test]
    async fn test_repartition_rollback_is_idempotent() {
        let env = TestingEnv::new();
        let table_id = 1024;
        let node_manager = Arc::new(MockDatanodeManager::new(UnexpectedErrorDatanodeHandler));
        let ddl_ctx = env.ddl_context(node_manager);
        let original_region_routes = vec![
            test_region_route(
                RegionId::new(table_id, 1),
                &range_expr("x", 0, 100).as_json_str().unwrap(),
            ),
            test_region_route(
                RegionId::new(table_id, 2),
                &range_expr("x", 50, 100).as_json_str().unwrap(),
            ),
            test_region_route(RegionId::new(table_id, 3), ""),
        ];
        env.create_physical_table_metadata_with_wal_options(
            table_id,
            original_region_routes,
            test_region_wal_options(&[1, 2]),
        )
        .await;

        let mut persistent_ctx = PersistentContext::new(
            TableName::new("test_catalog", "test_schema", "test_table"),
            table_id,
            None,
        );
        persistent_ctx.plans = vec![test_plan(table_id)];
        persistent_ctx.failed_procedures = vec![ProcedureMeta {
            plan_index: 0,
            group_id: Uuid::new_v4(),
            procedure_id: ProcedureId::random(),
        }];
        let context = Context::new(
            &ddl_ctx,
            env.mailbox_ctx.mailbox().clone(),
            env.server_addr.clone(),
            persistent_ctx,
        );
        let mut procedure = RepartitionProcedure {
            state: Box::new(Dispatch),
            context,
        };

        procedure
            .rollback(&TestingEnv::procedure_context())
            .await
            .unwrap();
        let once = current_parent_region_routes(&procedure.context).await;

        procedure
            .rollback(&TestingEnv::procedure_context())
            .await
            .unwrap();
        let twice = current_parent_region_routes(&procedure.context).await;

        assert_eq!(once, twice);
        assert_eq!(once.len(), 2);
        assert_eq!(once[0].region.id, RegionId::new(table_id, 1));
        assert_eq!(once[1].region.id, RegionId::new(table_id, 2));
    }

    #[tokio::test]
    async fn test_repartition_procedure_flow_split_failed_and_full_rollback() {
        let env = TestingEnv::new();
        let table_id = 1024;
        let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));

        env.create_physical_table_metadata_for_repartition(
            table_id,
            vec![
                test_region_route(
                    RegionId::new(table_id, 1),
                    &range_expr("x", 0, 100).as_json_str().unwrap(),
                ),
                test_region_route(
                    RegionId::new(table_id, 2),
                    &range_expr("x", 100, 200).as_json_str().unwrap(),
                ),
            ],
            test_region_wal_options(&[1, 2]),
        )
        .await;

        let context = new_parent_context(&env, node_manager, table_id);
        let mut procedure = RepartitionProcedure::new(
            vec![range_expr("x", 0, 100)],
            vec![range_expr("x", 0, 50), range_expr("x", 50, 100)],
            context,
        );

        let start_status = procedure
            .execute(&TestingEnv::procedure_context())
            .await
            .unwrap();
        assert!(!start_status.need_persist());
        let start_status = procedure
            .execute(&TestingEnv::procedure_context())
            .await
            .unwrap();
        assert!(start_status.need_persist());
        assert_parent_state::<AllocateRegion>(&procedure);

        let allocate_status = procedure
            .execute(&TestingEnv::procedure_context())
            .await
            .unwrap();
        assert!(allocate_status.need_persist());
        assert_parent_state::<Dispatch>(&procedure);
        assert_eq!(procedure.context.persistent_ctx.plans.len(), 1);
        let plan = &procedure.context.persistent_ctx.plans[0];
        let expected_plan = test_plan(table_id);
        assert_eq!(plan.source_regions, expected_plan.source_regions);
        assert_eq!(plan.target_regions, expected_plan.target_regions);
        assert_eq!(
            plan.allocated_region_ids,
            expected_plan.allocated_region_ids
        );
        assert_eq!(
            plan.pending_deallocate_region_ids,
            expected_plan.pending_deallocate_region_ids
        );
        assert_eq!(plan.transition_map, expected_plan.transition_map);
        assert_eq!(
            current_parent_region_routes(&procedure.context).await,
            vec![
                test_region_route(
                    RegionId::new(table_id, 1),
                    &range_expr("x", 0, 100).as_json_str().unwrap(),
                ),
                test_region_route(
                    RegionId::new(table_id, 2),
                    &range_expr("x", 100, 200).as_json_str().unwrap(),
                ),
                RegionRoute {
                    region: Region {
                        id: RegionId::new(table_id, 3),
                        partition_expr: range_expr("x", 50, 100).as_json_str().unwrap(),
                        ..Default::default()
                    },
                    leader_peer: Some(Peer::empty(0)),
                    ..Default::default()
                },
            ]
        );

        let dispatch_status = procedure
            .execute(&TestingEnv::procedure_context())
            .await
            .unwrap();
        assert!(!dispatch_status.need_persist());
        let subprocedure_ids = extract_subprocedure_ids(dispatch_status);
        assert_eq!(subprocedure_ids.len(), 1);
        assert_parent_state::<Collect>(&procedure);

        let failed_state = ProcedureState::failed(Arc::new(ProcedureError::external(
            MockError::new(StatusCode::Internal),
        )));
        let collect_ctx = procedure_context_with_receivers(HashMap::from([(
            subprocedure_ids[0],
            procedure_state_receiver(failed_state),
        )]));

        let err = procedure.execute(&collect_ctx).await.unwrap_err();
        assert!(!err.is_retry_later());
        assert_parent_state::<Collect>(&procedure);

        procedure
            .rollback(&TestingEnv::procedure_context())
            .await
            .unwrap();

        let region_routes = current_parent_region_routes(&procedure.context).await;
        assert_eq!(
            region_routes,
            vec![
                test_region_route(
                    RegionId::new(table_id, 1),
                    &range_expr("x", 0, 100).as_json_str().unwrap(),
                ),
                test_region_route(
                    RegionId::new(table_id, 2),
                    &range_expr("x", 100, 200).as_json_str().unwrap(),
                ),
            ]
        );
    }

    #[tokio::test]
    async fn test_repartition_procedure_flow_split_allocate_retryable_then_resume() {
        common_telemetry::init_default_ut_logging();
        let env = TestingEnv::new();
        let table_id = 1024;
        let (tx, _rx) = mpsc::channel(8);
        let should_retry = Arc::new(AtomicBool::new(true));
        let datanode_handler = DatanodeWatcher::new(tx).with_handler(move |_, _| {
            if should_retry.swap(false, Ordering::SeqCst) {
                return Err(error::Error::RetryLater {
                    source: BoxedError::new(
                        error::UnexpectedSnafu {
                            err_msg: "retry later",
                        }
                        .build(),
                    ),
                    clean_poisons: false,
                });
            }

            Ok(api::region::RegionResponse::new(0))
        });
        let node_manager = Arc::new(MockDatanodeManager::new(datanode_handler));

        env.create_physical_table_metadata_for_repartition(
            table_id,
            vec![
                test_region_route(
                    RegionId::new(table_id, 1),
                    &range_expr("x", 0, 100).as_json_str().unwrap(),
                ),
                test_region_route(
                    RegionId::new(table_id, 2),
                    &range_expr("x", 100, 200).as_json_str().unwrap(),
                ),
            ],
            test_region_wal_options(&[1, 2]),
        )
        .await;

        let context = new_parent_context(&env, node_manager, table_id);
        let mut procedure = RepartitionProcedure::new(
            vec![range_expr("x", 0, 100)],
            vec![range_expr("x", 0, 50), range_expr("x", 50, 100)],
            context,
        );

        let start_status = procedure
            .execute(&TestingEnv::procedure_context())
            .await
            .unwrap();
        assert!(!start_status.need_persist());
        let start_status = procedure
            .execute(&TestingEnv::procedure_context())
            .await
            .unwrap();
        assert!(start_status.need_persist());
        assert_parent_state::<AllocateRegion>(&procedure);

        let err = procedure
            .execute(&TestingEnv::procedure_context())
            .await
            .unwrap_err();
        assert!(err.is_retry_later());
        assert_parent_state::<AllocateRegion>(&procedure);
        assert!(!procedure.context.persistent_ctx.plans.is_empty());
        assert_eq!(
            current_parent_region_routes(&procedure.context).await,
            vec![
                test_region_route(
                    RegionId::new(table_id, 1),
                    &range_expr("x", 0, 100).as_json_str().unwrap(),
                ),
                test_region_route(
                    RegionId::new(table_id, 2),
                    &range_expr("x", 100, 200).as_json_str().unwrap(),
                ),
            ]
        );

        let allocate_status = procedure
            .execute(&TestingEnv::procedure_context())
            .await
            .unwrap();
        assert!(allocate_status.need_persist());
        assert_parent_state::<Dispatch>(&procedure);

        assert_eq!(procedure.context.persistent_ctx.plans.len(), 1);
        let plan = &procedure.context.persistent_ctx.plans[0];
        let expected_plan = test_plan(table_id);
        assert_eq!(plan.source_regions, expected_plan.source_regions);
        assert_eq!(plan.target_regions, expected_plan.target_regions);
        assert_eq!(
            plan.allocated_region_ids,
            expected_plan.allocated_region_ids
        );
        assert_eq!(plan.transition_map, expected_plan.transition_map);
        assert_eq!(
            current_parent_region_routes(&procedure.context).await,
            vec![
                test_region_route(
                    RegionId::new(table_id, 1),
                    &range_expr("x", 0, 100).as_json_str().unwrap(),
                ),
                test_region_route(
                    RegionId::new(table_id, 2),
                    &range_expr("x", 100, 200).as_json_str().unwrap(),
                ),
                RegionRoute {
                    region: Region {
                        id: RegionId::new(table_id, 3),
                        partition_expr: range_expr("x", 50, 100).as_json_str().unwrap(),
                        ..Default::default()
                    },
                    leader_peer: Some(Peer::empty(0)),
                    ..Default::default()
                },
            ]
        );

        let dispatch_status = procedure
            .execute(&TestingEnv::procedure_context())
            .await
            .unwrap();
        assert!(!dispatch_status.need_persist());
        let subprocedure_ids = extract_subprocedure_ids(dispatch_status);
        assert_eq!(subprocedure_ids.len(), 1);
        assert_parent_state::<Collect>(&procedure);
    }
}
