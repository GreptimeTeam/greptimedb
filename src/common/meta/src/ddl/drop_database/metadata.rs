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

use super::end::DropDatabaseEnd;
use crate::ddl::drop_database::{DropDatabaseContext, State};
use crate::ddl::DdlContext;
use crate::error::Result;
use crate::key::schema_name::SchemaNameKey;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DropDatabaseRemoveMetadata;

#[async_trait::async_trait]
#[typetag::serde]
impl State for DropDatabaseRemoveMetadata {
    async fn next(
        &mut self,
        ddl_ctx: &DdlContext,
        ctx: &mut DropDatabaseContext,
    ) -> Result<(Box<dyn State>, Status)> {
        ddl_ctx
            .table_metadata_manager
            .schema_manager()
            .delete(SchemaNameKey::new(&ctx.catalog, &ctx.schema))
            .await?;

        return Ok((Box::new(DropDatabaseEnd), Status::done()));
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::ddl::drop_database::end::DropDatabaseEnd;
    use crate::ddl::drop_database::metadata::DropDatabaseRemoveMetadata;
    use crate::ddl::drop_database::{DropDatabaseContext, State};
    use crate::key::schema_name::SchemaNameKey;
    use crate::test_util::{new_ddl_context, MockDatanodeManager};

    #[tokio::test]
    async fn test_next() {
        let datanode_manager = Arc::new(MockDatanodeManager::new(()));
        let ddl_context = new_ddl_context(datanode_manager);
        ddl_context
            .table_metadata_manager
            .schema_manager()
            .create(SchemaNameKey::new("foo", "bar"), None, true)
            .await
            .unwrap();
        let mut state = DropDatabaseRemoveMetadata;
        let mut ctx = DropDatabaseContext {
            catalog: "foo".to_string(),
            schema: "bar".to_string(),
            drop_if_exists: true,
            tables: None,
        };
        let (state, status) = state.next(&ddl_context, &mut ctx).await.unwrap();
        state.as_any().downcast_ref::<DropDatabaseEnd>().unwrap();
        assert!(status.is_done());
        assert!(!ddl_context
            .table_metadata_manager
            .schema_manager()
            .exists(SchemaNameKey::new("foo", "bar"))
            .await
            .unwrap());
        // Schema not exists
        let mut state = DropDatabaseRemoveMetadata;
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
}
