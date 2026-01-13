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
use std::collections::HashMap;
use std::fmt::Debug;

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
use common_meta::region_keeper::MemoryRegionKeeperRef;
use common_meta::region_registry::LeaderRegionRegistryRef;
use common_meta::rpc::router::RegionRoute;
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    BoxedProcedure, Context as ProcedureContext, Error as ProcedureError, LockKey, Procedure,
    ProcedureManagerRef, Result as ProcedureResult, Status, StringKey, UserMetadata,
};
use common_telemetry::{error, info};
use partition::expr::PartitionExpr;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::storage::{RegionNumber, TableId};
use table::table_name::TableName;

use crate::error::{self, Result};
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
}

impl PersistentContext {
    pub fn new(
        TableName {
            catalog_name,
            schema_name,
            table_name,
        }: TableName,
        table_id: TableId,
    ) -> Self {
        Self {
            catalog_name,
            schema_name,
            table_name,
            table_id,
            plans: vec![],
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
    pub table_metadata_manager: TableMetadataManagerRef,
    pub memory_region_keeper: MemoryRegionKeeperRef,
    pub node_manager: NodeManagerRef,
    pub leader_region_registry: LeaderRegionRegistryRef,
    pub mailbox: MailboxRef,
    pub server_addr: String,
    pub cache_invalidator: CacheInvalidatorRef,
    pub region_routes_allocator: RegionRoutesAllocatorRef,
    pub wal_options_allocator: WalOptionsAllocatorRef,
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
        }
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

    /// Returns the next operation timeout.
    ///
    /// If the next operation timeout is not set, it will return `None`.
    pub fn next_operation_timeout(&self) -> Option<std::time::Duration> {
        Some(std::time::Duration::from_secs(10))
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
}

#[async_trait::async_trait]
impl Procedure for RepartitionProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

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

    fn rollback_supported(&self) -> bool {
        // TODO(weny): support rollback.
        false
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
    ) -> std::result::Result<BoxedProcedure, BoxedError> {
        let persistent_ctx = PersistentContext::new(table_name, table_id);
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
