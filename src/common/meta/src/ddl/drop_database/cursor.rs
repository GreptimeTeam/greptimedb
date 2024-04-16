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

use common_procedure::Status;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use table::metadata::TableId;

use super::executor::DropDatabaseExecutor;
use super::metadata::DropDatabaseRemoveMetadata;
use super::DropTableTarget;
use crate::ddl::drop_database::{DropDatabaseContext, State};
use crate::ddl::DdlContext;
use crate::error::Result;
use crate::key::table_route::TableRouteValue;
use crate::table_name::TableName;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DropDatabaseCursor {
    pub(crate) target: DropTableTarget,
}

impl DropDatabaseCursor {
    /// Returns a new [DropDatabaseCursor].
    pub fn new(target: DropTableTarget) -> Self {
        Self { target }
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
        ddl_ctx: &DdlContext,
        ctx: &mut DropDatabaseContext,
        table_name: String,
        table_id: TableId,
        table_route_value: TableRouteValue,
    ) -> Result<(Box<dyn State>, Status)> {
        match (self.target, table_route_value) {
            (DropTableTarget::Logical, TableRouteValue::Logical(route)) => {
                let physical_table_id = route.physical_table_id();

                let (_, table_route) = ddl_ctx
                    .table_metadata_manager
                    .table_route_manager()
                    .get_physical_table_route(physical_table_id)
                    .await?;
                Ok((
                    Box::new(DropDatabaseExecutor::new(
                        table_id,
                        table_id,
                        TableName::new(&ctx.catalog, &ctx.schema, &table_name),
                        table_route.region_routes,
                        self.target,
                    )),
                    Status::executing(true),
                ))
            }
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
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for DropDatabaseCursor {
    async fn next(
        &mut self,
        ddl_ctx: &DdlContext,
        ctx: &mut DropDatabaseContext,
    ) -> Result<(Box<dyn State>, Status)> {
        if ctx.tables.as_deref().is_none() {
            let tables = ddl_ctx
                .table_metadata_manager
                .table_name_manager()
                .tables(&ctx.catalog, &ctx.schema);
            ctx.tables = Some(tables);
        }
        // Safety: must exist
        match ctx.tables.as_mut().unwrap().try_next().await? {
            Some((table_name, table_name_value)) => {
                let table_id = table_name_value.table_id();
                match ddl_ctx
                    .table_metadata_manager
                    .table_route_manager()
                    .table_route_storage()
                    .get(table_id)
                    .await?
                {
                    Some(table_route_value) => {
                        self.handle_table(ddl_ctx, ctx, table_name, table_id, table_route_value)
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

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};

    use crate::ddl::drop_database::cursor::DropDatabaseCursor;
    use crate::ddl::drop_database::executor::DropDatabaseExecutor;
    use crate::ddl::drop_database::metadata::DropDatabaseRemoveMetadata;
    use crate::ddl::drop_database::{DropDatabaseContext, DropTableTarget, State};
    use crate::ddl::test_util::{create_logical_table, create_physical_table};
    use crate::test_util::{new_ddl_context, MockDatanodeManager};

    #[tokio::test]
    async fn test_next_without_logical_tables() {
        let datanode_manager = Arc::new(MockDatanodeManager::new(()));
        let ddl_context = new_ddl_context(datanode_manager);
        create_physical_table(ddl_context.clone(), 0, "phy").await;
        // It always starts from Logical
        let mut state = DropDatabaseCursor::new(DropTableTarget::Logical);
        let mut ctx = DropDatabaseContext {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            drop_if_exists: false,
            tables: None,
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
        let datanode_manager = Arc::new(MockDatanodeManager::new(()));
        let ddl_context = new_ddl_context(datanode_manager);
        let physical_table_id = create_physical_table(ddl_context.clone(), 0, "phy").await;
        create_logical_table(ddl_context.clone(), 0, physical_table_id, "metric_0").await;
        // It always starts from Logical
        let mut state = DropDatabaseCursor::new(DropTableTarget::Logical);
        let mut ctx = DropDatabaseContext {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            drop_if_exists: false,
            tables: None,
        };
        // Ticks
        let (state, status) = state.next(&ddl_context, &mut ctx).await.unwrap();
        assert!(status.need_persist());
        let executor = state
            .as_any()
            .downcast_ref::<DropDatabaseExecutor>()
            .unwrap();
        let (_, table_route) = ddl_context
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(physical_table_id)
            .await
            .unwrap();
        assert_eq!(table_route.region_routes, executor.region_routes);
        assert_eq!(executor.target, DropTableTarget::Logical);
    }

    #[tokio::test]
    async fn test_reach_the_end() {
        let datanode_manager = Arc::new(MockDatanodeManager::new(()));
        let ddl_context = new_ddl_context(datanode_manager);
        let mut state = DropDatabaseCursor::new(DropTableTarget::Physical);
        let mut ctx = DropDatabaseContext {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            drop_if_exists: false,
            tables: None,
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
