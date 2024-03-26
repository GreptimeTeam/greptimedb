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

use common_procedure::Status;
use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::ddl::drop_database::cursor::DropDatabaseCursor;
use crate::ddl::drop_database::end::DropDatabaseEnd;
use crate::ddl::drop_database::{DropDatabaseContext, DropTableTarget, State};
use crate::ddl::DdlContext;
use crate::error::{self, Result};
use crate::key::schema_name::SchemaNameKey;

#[derive(Debug, Serialize, Deserialize)]
pub struct DropDatabaseStart;

#[async_trait::async_trait]
#[typetag::serde]
impl State for DropDatabaseStart {
    /// Checks whether schema exists.
    /// - Early returns if schema not exists and `drop_if_exists` is `true`.
    /// - Throws an error if schema not exists and `drop_if_exists` is `false`.
    async fn next(
        &mut self,
        ddl_ctx: &DdlContext,
        ctx: &mut DropDatabaseContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let exists = ddl_ctx
            .table_metadata_manager
            .schema_manager()
            .exists(SchemaNameKey {
                catalog: &ctx.catalog,
                schema: &ctx.schema,
            })
            .await?;

        if !exists && ctx.drop_if_exists {
            return Ok((Box::new(DropDatabaseEnd), Status::done()));
        }

        ensure!(
            exists,
            error::SchemaNotFoundSnafu {
                table_schema: &ctx.schema,
            }
        );

        Ok((
            Box::new(DropDatabaseCursor::new(DropTableTarget::Logical)),
            Status::executing(true),
        ))
    }
}
