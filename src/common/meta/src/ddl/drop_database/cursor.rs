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
    target: DropTableTarget,
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
        match self.target {
            DropTableTarget::Logical => {
                // Consumes the tables stream.
                ctx.tables.take();

                Ok((
                    Box::new(DropDatabaseCursor::new(DropTableTarget::Physical)),
                    Status::executing(true),
                ))
            }
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
                let table_id = route.physical_table_id();

                let (_, table_route) = ddl_ctx
                    .table_metadata_manager
                    .table_route_manager()
                    .get_physical_table_route(table_id)
                    .await?;
                Ok((
                    Box::new(DropDatabaseExecutor::new(
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
