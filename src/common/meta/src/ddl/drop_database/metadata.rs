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
use crate::cache_invalidator::Context;
use crate::ddl::drop_database::{DropDatabaseContext, State};
use crate::ddl::DdlContext;
use crate::error::Result;
use crate::instruction::CacheIdent;
use crate::key::schema_name::{SchemaName, SchemaNameKey};

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

        return Ok((Box::new(DropMetadataBroadcast), Status::executing(true)));
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DropMetadataBroadcast;

impl DropMetadataBroadcast {
    /// Invalidates frontend caches
    async fn invalidate_schema_cache(
        &self,
        ddl_ctx: &DdlContext,
        db_ctx: &mut DropDatabaseContext,
    ) -> Result<()> {
        let cache_invalidator = &ddl_ctx.cache_invalidator;
        let ctx = Context {
            subject: Some("Invalidate schema cache by dropping database".to_string()),
        };

        cache_invalidator
            .invalidate(
                &ctx,
                &[CacheIdent::SchemaName(SchemaName {
                    catalog_name: db_ctx.catalog.clone(),
                    schema_name: db_ctx.schema.clone(),
                })],
            )
            .await?;

        Ok(())
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for DropMetadataBroadcast {
    async fn next(
        &mut self,
        ddl_ctx: &DdlContext,
        ctx: &mut DropDatabaseContext,
    ) -> Result<(Box<dyn State>, Status)> {
        self.invalidate_schema_cache(ddl_ctx, ctx).await?;
        Ok((Box::new(DropDatabaseEnd), Status::done()))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::ddl::drop_database::end::DropDatabaseEnd;
    use crate::ddl::drop_database::metadata::{DropDatabaseRemoveMetadata, DropMetadataBroadcast};
    use crate::ddl::drop_database::{DropDatabaseContext, State};
    use crate::key::schema_name::SchemaNameKey;
    use crate::test_util::{new_ddl_context, MockDatanodeManager};

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
        let mut state = DropDatabaseRemoveMetadata;
        let mut ctx = DropDatabaseContext {
            cluster_id: 0,
            catalog: "foo".to_string(),
            schema: "bar".to_string(),
            drop_if_exists: true,
            tables: None,
        };
        let (state, status) = state.next(&ddl_context, &mut ctx).await.unwrap();
        state
            .as_any()
            .downcast_ref::<DropMetadataBroadcast>()
            .unwrap();
        assert!(!status.is_done());
        assert!(!ddl_context
            .table_metadata_manager
            .schema_manager()
            .exists(SchemaNameKey::new("foo", "bar"))
            .await
            .unwrap());

        let mut state = DropMetadataBroadcast;
        let (state, status) = state.next(&ddl_context, &mut ctx).await.unwrap();
        state.as_any().downcast_ref::<DropDatabaseEnd>().unwrap();
        assert!(status.is_done());

        // Schema not exists
        let mut state = DropDatabaseRemoveMetadata;
        let mut ctx = DropDatabaseContext {
            cluster_id: 0,
            catalog: "foo".to_string(),
            schema: "bar".to_string(),
            drop_if_exists: true,
            tables: None,
        };
        let (state, status) = state.next(&ddl_context, &mut ctx).await.unwrap();
        state
            .as_any()
            .downcast_ref::<DropMetadataBroadcast>()
            .unwrap();
        assert!(!status.is_done());
    }
}
