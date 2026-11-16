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

use std::any::Any;
use std::collections::HashSet;
use std::pin::Pin;

use common_base::readable_size::ReadableSize;
use common_catalog::format_full_table_name;
use common_procedure::Status;
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ensure};
use table::metadata::{TableId, TableType};
use table::table_name::TableName;

use crate::cache_invalidator::Context;
use crate::ddl::DdlContext;
use crate::ddl::drop_database::executor::{DropDatabaseBatchExecutor, DropDatabaseExecutor};
use crate::ddl::drop_database::metadata::DropDatabaseRemoveMetadata;
use crate::ddl::drop_database::{DropDatabaseContext, DropTableTarget, State};
use crate::error::{Result, TableInfoNotFoundSnafu, UnexpectedSnafu};
use crate::instruction::CacheIdent;
use crate::key::table_route::TableRouteValue;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DropDatabaseCursor {
    pub(crate) target: DropTableTarget,
}

impl DropDatabaseCursor {
    /// Returns a new [DropDatabaseCursor].
    pub fn new(target: DropTableTarget) -> Self {
        Self { target }
    }

    /// Collects a bounded page without mutating metadata. Only the executor's batch is durable.
    async fn collect_logical_tables(
        &mut self,
        ddl_ctx: &DdlContext,
        ctx: &mut DropDatabaseContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let page = Self::collect_page(ctx).await?;
        if page.is_empty() {
            return self.handle_reach_end(ctx);
        }
        let tables = Self::logical_tables(ddl_ctx, ctx, page).await?;
        if tables.is_empty() {
            return Ok((Box::new(Self::new(self.target)), Status::executing(false)));
        }
        Ok((
            Box::new(DropDatabaseBatchExecutor { tables }),
            Status::executing(true),
        ))
    }

    // Leave headroom below etcd's default 1.5 MiB request limit.
    const MAX_BATCH_BYTES: usize = ReadableSize::mb(1).as_bytes() as usize;

    fn estimated_table_bytes(catalog: &str, schema: &str, name: &str) -> usize {
        // Allow for nested JSON escaping and fixed per-table metadata overhead.
        256 + 8 * (catalog.len() + schema.len() + name.len())
    }

    async fn collect_page(ctx: &mut DropDatabaseContext) -> Result<Vec<(String, TableId)>> {
        let mut page = Vec::new();
        let mut bytes: usize = 0;
        let tables = ctx.tables.as_mut().unwrap();
        loop {
            match Pin::new(&mut *tables).peek().await {
                Some(Ok((name, _))) => {
                    let size = Self::estimated_table_bytes(&ctx.catalog, &ctx.schema, name);
                    if !page.is_empty() && bytes.saturating_add(size) > Self::MAX_BATCH_BYTES {
                        break;
                    }
                    // A single oversized name must still make progress. Procedure storage
                    // already splits oversized values; never strand an existing table here.
                    bytes = bytes.saturating_add(size);
                }
                Some(Err(_)) => {} // Consume below to propagate the owned error.
                None => break,
            }
            let (name, value) = tables.try_next().await?.unwrap();
            page.push((name, value.table_id()));
        }
        Ok(page)
    }

    async fn logical_tables(
        ddl_ctx: &DdlContext,
        ctx: &mut DropDatabaseContext,
        page: Vec<(String, TableId)>,
    ) -> Result<Vec<(TableId, TableName)>> {
        let manager = &ddl_ctx.table_metadata_manager;
        let ids = page.iter().map(|(_, id)| *id).collect::<Vec<_>>();
        let infos = manager.table_info_manager().batch_get(&ids).await?;
        let routes = manager
            .table_route_manager()
            .table_route_storage()
            .batch_get(&ids)
            .await?;
        let physical_ids = routes
            .iter()
            .filter_map(|route| match route {
                Some(TableRouteValue::Logical(route)) => Some(route.physical_table_id()),
                _ => None,
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let physical_infos = manager
            .table_info_manager()
            .batch_get(&physical_ids)
            .await?;
        let mut tables = Vec::new();
        for ((name, id), route) in page.into_iter().zip(routes) {
            let info = infos.get(&id).with_context(|| TableInfoNotFoundSnafu {
                table: format_full_table_name(&ctx.catalog, &ctx.schema, &name),
            })?;
            if info.table_info.table_type == TableType::View {
                // Views are handled in the physical phase so collecting a batch has no side effects.
                continue;
            }
            if let Some(TableRouteValue::Logical(route)) = route {
                let physical = physical_infos
                    .get(&route.physical_table_id())
                    .with_context(|| TableInfoNotFoundSnafu {
                        table: route.physical_table_id().to_string(),
                    })?;
                // Metadata-only deletion is safe only when the backing table is dropped with us.
                ensure!(
                    physical.table_info.catalog_name == ctx.catalog
                        && physical.table_info.schema_name == ctx.schema,
                    UnexpectedSnafu {
                        err_msg: format!(
                            "Logical table {} references physical table {} (id {}) outside the dropped database",
                            format_full_table_name(&ctx.catalog, &ctx.schema, &name),
                            format_full_table_name(
                                &physical.table_info.catalog_name,
                                &physical.table_info.schema_name,
                                &physical.table_info.name,
                            ),
                            route.physical_table_id(),
                        ),
                    }
                );
                tables.push((id, TableName::new(&ctx.catalog, &ctx.schema, name)));
            }
        }
        Ok(tables)
    }

    fn handle_reach_end(
        &mut self,
        ctx: &mut DropDatabaseContext,
    ) -> Result<(Box<dyn State>, Status)> {
        // Consumes the tables stream.
        ctx.tables.take();
        match self.target {
            DropTableTarget::Logical => Ok((
                Box::new(DropDatabaseCursor::new(DropTableTarget::Physical)),
                Status::executing(true),
            )),
            DropTableTarget::Physical => Ok((
                Box::new(DropDatabaseRemoveMetadata),
                Status::executing(true),
            )),
        }
    }

    async fn handle_table(
        &mut self,
        ctx: &mut DropDatabaseContext,
        table_name: String,
        table_id: TableId,
        table_route_value: TableRouteValue,
    ) -> Result<(Box<dyn State>, Status)> {
        match (self.target, table_route_value) {
            (DropTableTarget::Physical, TableRouteValue::Physical(table_route)) => Ok((
                Box::new(DropDatabaseExecutor::new(
                    table_id,
                    table_id,
                    TableName::new(&ctx.catalog, &ctx.schema, &table_name),
                    table_route.region_routes,
                    self.target,
                )),
                Status::executing(true),
            )),
            _ => Ok((
                Box::new(DropDatabaseCursor::new(self.target)),
                Status::executing(false),
            )),
        }
    }
    async fn handle_view(
        &self,
        ddl_ctx: &DdlContext,
        ctx: &mut DropDatabaseContext,
        table_name: String,
        table_id: TableId,
    ) -> Result<(Box<dyn State>, Status)> {
        let view_name = TableName::new(&ctx.catalog, &ctx.schema, &table_name);
        ddl_ctx
            .table_metadata_manager
            .destroy_view_info(table_id, &view_name)
            .await?;

        let cache_invalidator = &ddl_ctx.cache_invalidator;
        let ctx = Context {
            subject: Some("Invalidate table cache by dropping table".to_string()),
        };

        cache_invalidator
            .invalidate(
                &ctx,
                &[
                    CacheIdent::TableName(view_name),
                    CacheIdent::TableId(table_id),
                ],
            )
            .await?;

        Ok((
            Box::new(DropDatabaseCursor::new(self.target)),
            Status::executing(false),
        ))
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for DropDatabaseCursor {
    async fn next(
        &mut self,
        ddl_ctx: &DdlContext,
        ctx: &mut DropDatabaseContext,
    ) -> Result<(Box<dyn State>, Status)> {
        if ctx.tables.is_none() {
            let tables = ddl_ctx
                .table_metadata_manager
                .table_name_manager()
                .tables(&ctx.catalog, &ctx.schema);
            ctx.tables = Some(tables.peekable());
        }
        let result = async {
            if self.target == DropTableTarget::Logical {
                return self.collect_logical_tables(ddl_ctx, ctx).await;
            }
            // Safety: must exist
            match ctx.tables.as_mut().unwrap().try_next().await? {
                Some((table_name, table_name_value)) => {
                    let table_id = table_name_value.table_id();

                    let table_info_value = ddl_ctx
                        .table_metadata_manager
                        .table_info_manager()
                        .get(table_id)
                        .await?
                        .with_context(|| TableInfoNotFoundSnafu {
                            table: format_full_table_name(&ctx.catalog, &ctx.schema, &table_name),
                        })?;

                    if table_info_value.table_info.table_type == TableType::View {
                        return self.handle_view(ddl_ctx, ctx, table_name, table_id).await;
                    }

                    match ddl_ctx
                        .table_metadata_manager
                        .table_route_manager()
                        .table_route_storage()
                        .get(table_id)
                        .await?
                    {
                        Some(table_route_value) => {
                            self.handle_table(ctx, table_name, table_id, table_route_value)
                                .await
                        }
                        None => Ok((
                            Box::new(DropDatabaseCursor::new(self.target)),
                            Status::executing(false),
                        )),
                    }
                }
                None => self.handle_reach_end(ctx),
            }
        }
        .await;
        if result.is_err() {
            // The entry was consumed but no deletion plan was persisted. Rescan on retry.
            ctx.tables.take();
        }
        result
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};

    use crate::ddl::drop_database::cursor::DropDatabaseCursor;
    use crate::ddl::drop_database::executor::{DropDatabaseBatchExecutor, DropDatabaseExecutor};
    use crate::ddl::drop_database::metadata::DropDatabaseRemoveMetadata;
    use crate::ddl::drop_database::{DropDatabaseContext, DropTableTarget, State};
    use crate::ddl::test_util::{create_logical_table, create_physical_table};
    use crate::test_util::{MockDatanodeManager, new_ddl_context};

    #[tokio::test]
    async fn test_cursor_read_failure_rescans() {
        use crate::key::table_name::TableNameValue;
        use futures::StreamExt;
        for target in [DropTableTarget::Logical, DropTableTarget::Physical] {
            let ddl = new_ddl_context(Arc::new(MockDatanodeManager::new(())));
            let physical = create_physical_table(&ddl, "physical").await;
            let mut ctx = DropDatabaseContext {
                catalog: DEFAULT_CATALOG_NAME.into(),
                schema: DEFAULT_SCHEMA_NAME.into(),
                drop_if_exists: false,
                retrying: false,
                tables: Some(
                    futures::stream::iter(vec![Ok(("missing".into(), TableNameValue::new(99999)))])
                        .boxed()
                        .peekable(),
                ),
            };
            let mut cursor = DropDatabaseCursor::new(target);
            assert!(cursor.next(&ddl, &mut ctx).await.is_err());
            assert!(ctx.tables.is_none());
            let (state, _) = cursor.next(&ddl, &mut ctx).await.unwrap();
            if target == DropTableTarget::Physical {
                assert!(state.as_any().is::<DropDatabaseExecutor>());
            } else {
                assert!(state.as_any().is::<DropDatabaseCursor>());
            }
            assert!(
                ddl.table_metadata_manager
                    .table_info_manager()
                    .get(physical)
                    .await
                    .unwrap()
                    .is_some()
            );
        }
    }

    #[tokio::test]
    async fn test_collect_page_oversized_singleton_makes_progress() {
        use crate::key::table_name::TableNameValue;
        use futures::StreamExt;
        let mut ctx = DropDatabaseContext {
            catalog: DEFAULT_CATALOG_NAME.into(),
            schema: DEFAULT_SCHEMA_NAME.into(),
            drop_if_exists: false,
            retrying: false,
            tables: Some(
                futures::stream::iter(vec![
                    Ok((
                        "x".repeat(DropDatabaseCursor::MAX_BATCH_BYTES),
                        TableNameValue::new(1),
                    )),
                    Ok(("small".into(), TableNameValue::new(2))),
                ])
                .boxed()
                .peekable(),
            ),
        };
        let page = DropDatabaseCursor::collect_page(&mut ctx).await.unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].1, 1);
        assert_eq!(
            DropDatabaseCursor::collect_page(&mut ctx).await.unwrap(),
            vec![("small".into(), 2)]
        );
        assert!(
            DropDatabaseCursor::collect_page(&mut ctx)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_collect_page_limits_and_request_headroom() {
        use common_procedure::store::ProcedureMessage;
        use futures::StreamExt;
        use table::table_name::TableName;

        use crate::ddl::drop_database::DropDatabaseProcedure;
        use crate::key::TableMetadataManager;
        use crate::key::table_name::TableNameValue;
        for name in [
            "short".to_string(),
            "x".repeat(64 * 1024),
            "\"\\\n\u{0001}中文".repeat(4096),
        ] {
            let ddl = new_ddl_context(Arc::new(MockDatanodeManager::new(())));
            let count = if name.len() < 1024 { 5000 } else { 65 };
            let entries = (0..count)
                .map(|id| Ok((name.clone(), TableNameValue::new(id))))
                .collect::<Vec<_>>();
            let mut ctx = DropDatabaseContext {
                catalog: DEFAULT_CATALOG_NAME.into(),
                schema: DEFAULT_SCHEMA_NAME.into(),
                drop_if_exists: false,
                tables: Some(futures::stream::iter(entries).boxed().peekable()),
                retrying: false,
            };
            let mut collected = Vec::new();
            loop {
                let page = DropDatabaseCursor::collect_page(&mut ctx).await.unwrap();
                if page.is_empty() {
                    break;
                }
                let estimated = page
                    .iter()
                    .map(|(name, _)| {
                        DropDatabaseCursor::estimated_table_bytes(&ctx.catalog, &ctx.schema, name)
                    })
                    .sum::<usize>();
                assert!(estimated <= DropDatabaseCursor::MAX_BATCH_BYTES);
                let tables = page
                    .iter()
                    .map(|(name, id)| (*id, TableName::new(&ctx.catalog, &ctx.schema, name)))
                    .collect::<Vec<_>>();
                // Each delete has one bytes field nested in two messages. Allow
                // 32 bytes per key for tags and length varints, plus a txn envelope.
                let delete_bytes = 64
                    + tables
                        .iter()
                        .flat_map(|(id, name)| TableMetadataManager::table_identity_keys(*id, name))
                        .map(|key| key.len() + 32)
                        .sum::<usize>();
                assert!(delete_bytes <= estimated);
                let mut procedure = DropDatabaseProcedure::new(
                    ctx.catalog.clone(),
                    ctx.schema.clone(),
                    false,
                    ddl.clone(),
                );
                procedure.state = Box::new(DropDatabaseBatchExecutor { tables });
                let message = ProcedureMessage {
                    type_name: DropDatabaseProcedure::TYPE_NAME.into(),
                    data: common_procedure::Procedure::dump(&procedure).unwrap(),
                    parent_id: None,
                    subprocedure_recovery: common_procedure::SubprocedureRecovery::Discard,
                    step: u32::MAX,
                    error: None,
                    context: Default::default(),
                };
                let serialized = serde_json::to_vec(&message).unwrap();
                assert!(serialized.len() <= estimated);
                assert!(serialized.len() + 512 * 1024 < 1536 * 1024);
                collected.extend(page.into_iter().map(|(_, id)| id));
            }
            assert_eq!(collected, (0..count).collect::<Vec<_>>());
        }
    }

    #[tokio::test]
    async fn test_next_without_logical_tables() {
        let node_manager = Arc::new(MockDatanodeManager::new(()));
        let ddl_context = new_ddl_context(node_manager);
        create_physical_table(&ddl_context, "phy").await;
        // It always starts from Logical
        let mut state = DropDatabaseCursor::new(DropTableTarget::Logical);
        let mut ctx = DropDatabaseContext {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            drop_if_exists: false,
            tables: None,
            retrying: false,
        };
        // Ticks
        let (mut state, status) = state.next(&ddl_context, &mut ctx).await.unwrap();
        assert!(!status.need_persist());
        let cursor = state.as_any().downcast_ref::<DropDatabaseCursor>().unwrap();
        assert_eq!(cursor.target, DropTableTarget::Logical);
        // Ticks
        let (mut state, status) = state.next(&ddl_context, &mut ctx).await.unwrap();
        assert!(status.need_persist());
        assert!(ctx.tables.is_none());
        let cursor = state.as_any().downcast_ref::<DropDatabaseCursor>().unwrap();
        assert_eq!(cursor.target, DropTableTarget::Physical);
        // Ticks
        let (state, status) = state.next(&ddl_context, &mut ctx).await.unwrap();
        assert!(status.need_persist());
        let executor = state
            .as_any()
            .downcast_ref::<DropDatabaseExecutor>()
            .unwrap();
        assert_eq!(executor.target, DropTableTarget::Physical);
    }

    #[tokio::test]
    async fn test_next_with_logical_tables() {
        let node_manager = Arc::new(MockDatanodeManager::new(()));
        let ddl_context = new_ddl_context(node_manager);
        let physical_table_id = create_physical_table(&ddl_context, "phy").await;
        create_logical_table(ddl_context.clone(), physical_table_id, "metric_0").await;
        // It always starts from Logical
        let mut state = DropDatabaseCursor::new(DropTableTarget::Logical);
        let mut ctx = DropDatabaseContext {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            drop_if_exists: false,
            tables: None,
            retrying: false,
        };
        // Ticks
        let (state, status) = state.next(&ddl_context, &mut ctx).await.unwrap();
        assert!(status.need_persist());
        let executor = state
            .as_any()
            .downcast_ref::<DropDatabaseBatchExecutor>()
            .unwrap();
        assert_eq!(executor.tables.len(), 1);
        assert_eq!(executor.tables[0].1.table_name, "metric_0");
    }

    #[tokio::test]
    async fn test_reach_the_end() {
        let node_manager = Arc::new(MockDatanodeManager::new(()));
        let ddl_context = new_ddl_context(node_manager);
        let mut state = DropDatabaseCursor::new(DropTableTarget::Physical);
        let mut ctx = DropDatabaseContext {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            drop_if_exists: false,
            tables: None,
            retrying: false,
        };
        // Ticks
        let (state, status) = state.next(&ddl_context, &mut ctx).await.unwrap();
        assert!(status.need_persist());
        state
            .as_any()
            .downcast_ref::<DropDatabaseRemoveMetadata>()
            .unwrap();
        assert!(ctx.tables.is_none());
    }
}
