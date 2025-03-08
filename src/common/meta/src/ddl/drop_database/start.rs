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
use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::ddl::drop_database::cursor::DropDatabaseCursor;
use crate::ddl::drop_database::end::DropDatabaseEnd;
use crate::ddl::drop_database::{DropDatabaseContext, DropTableTarget, State};
use crate::ddl::DdlContext;
use crate::error::{self, Result};
use crate::key::schema_name::SchemaNameKey;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DropDatabaseStart;

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

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use crate::ddl::drop_database::cursor::DropDatabaseCursor;
    use crate::ddl::drop_database::end::DropDatabaseEnd;
    use crate::ddl::drop_database::start::DropDatabaseStart;
    use crate::ddl::drop_database::{DropDatabaseContext, State};
    use crate::error;
    use crate::key::schema_name::SchemaNameKey;
    use crate::test_util::{new_ddl_context, MockDatanodeManager};

    #[tokio::test]
    async fn test_schema_not_exists_err() {
        let node_manager = Arc::new(MockDatanodeManager::new(()));
        let ddl_context = new_ddl_context(node_manager);
        let mut step = DropDatabaseStart;
        let mut ctx = DropDatabaseContext {
            catalog: "foo".to_string(),
            schema: "bar".to_string(),
            drop_if_exists: false,
            tables: None,
        };
        let err = step.next(&ddl_context, &mut ctx).await.unwrap_err();
        assert_matches!(err, error::Error::SchemaNotFound { .. });
    }

    #[tokio::test]
    async fn test_schema_not_exists() {
        let node_manager = Arc::new(MockDatanodeManager::new(()));
        let ddl_context = new_ddl_context(node_manager);
        let mut state = DropDatabaseStart;
        let mut ctx = DropDatabaseContext {
            catalog: "foo".to_string(),
            schema: "bar".to_string(),
            drop_if_exists: true,
            tables: None,
        };
        let (state, status) = state.next(&ddl_context, &mut ctx).await.unwrap();
        state.as_any().downcast_ref::<DropDatabaseEnd>().unwrap();
        assert!(status.is_done());
    }

    #[tokio::test]
    async fn test_next() {
        let node_manager = Arc::new(MockDatanodeManager::new(()));
        let ddl_context = new_ddl_context(node_manager);
        ddl_context
            .table_metadata_manager
            .schema_manager()
            .create(SchemaNameKey::new("foo", "bar"), None, true)
            .await
            .unwrap();
        let mut state = DropDatabaseStart;
        let mut ctx = DropDatabaseContext {
            catalog: "foo".to_string(),
            schema: "bar".to_string(),
            drop_if_exists: false,
            tables: None,
        };
        let (state, status) = state.next(&ddl_context, &mut ctx).await.unwrap();
        state.as_any().downcast_ref::<DropDatabaseCursor>().unwrap();
        assert!(status.need_persist());
    }
}
