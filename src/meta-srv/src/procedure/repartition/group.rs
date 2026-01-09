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
use std::fmt::Debug;
use std::time::Duration;

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
use common_telemetry::error;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::storage::{RegionId, TableId};
use uuid::Uuid;

use crate::error::{self, Result};
use crate::procedure::repartition::group::repartition_start::RepartitionStart;
use crate::procedure::repartition::plan::RegionDescriptor;
use crate::procedure::repartition::utils::get_datanode_table_value;
use crate::procedure::repartition::{self};
use crate::service::mailbox::MailboxRef;

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
}

#[async_trait::async_trait]
impl Procedure for RepartitionGroupProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

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
        false
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

    /// Returns the next operation timeout.
    ///
    /// If the next operation timeout is not set, it will return `None`.
    pub fn next_operation_timeout(&self) -> Option<Duration> {
        Some(Duration::from_secs(10))
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
    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use common_meta::key::TableMetadataManager;
    use common_meta::kv_backend::test_util::MockKvBackendBuilder;

    use crate::error::Error;
    use crate::procedure::repartition::test_util::{TestingEnv, new_persistent_context};

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
}
