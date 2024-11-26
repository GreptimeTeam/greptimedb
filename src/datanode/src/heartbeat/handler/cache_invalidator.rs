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

//! Schema cache invalidator handler

use std::sync::Arc;

use async_trait::async_trait;
use catalog::kvbackend::CachedKvBackend;
use common_meta::cache_invalidator::KvCacheInvalidator;
use common_meta::heartbeat::handler::{
    HandleControl, HeartbeatResponseHandler, HeartbeatResponseHandlerContext,
};
use common_meta::instruction::{CacheIdent, Instruction};
use common_meta::key::schema_name::SchemaNameKey;
use common_meta::key::MetadataKey;
use common_meta::kv_backend::KvBackend;
use common_telemetry::debug;

#[derive(Clone)]
pub(crate) struct InvalidateSchemaCacheHandler {
    cached_kv_backend: Arc<CachedKvBackend>,
}

#[async_trait]
impl HeartbeatResponseHandler for InvalidateSchemaCacheHandler {
    fn is_acceptable(&self, ctx: &HeartbeatResponseHandlerContext) -> bool {
        matches!(
            ctx.incoming_message.as_ref(),
            Some((_, Instruction::InvalidateCaches(_)))
        )
    }

    async fn handle(
        &self,
        ctx: &mut HeartbeatResponseHandlerContext,
    ) -> common_meta::error::Result<HandleControl> {
        let Some((_, Instruction::InvalidateCaches(caches))) = ctx.incoming_message.take() else {
            unreachable!("InvalidateSchemaCacheHandler: should be guarded by 'is_acceptable'")
        };

        debug!(
            "InvalidateSchemaCacheHandler: invalidating caches: {:?}",
            caches
        );

        for cache in caches {
            let CacheIdent::SchemaName(schema_name) = cache else {
                continue;
            };
            let key: SchemaNameKey = (&schema_name).into();
            let key_bytes = key.to_bytes();
            // invalidate and refill cache
            self.cached_kv_backend.invalidate_key(&key_bytes).await;
            let _ = self.cached_kv_backend.get(&key_bytes).await?;
        }

        Ok(HandleControl::Done)
    }
}

impl InvalidateSchemaCacheHandler {
    pub fn new(cached_kv_backend: Arc<CachedKvBackend>) -> Self {
        Self { cached_kv_backend }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use api::v1::meta::HeartbeatResponse;
    use catalog::kvbackend::CachedKvBackendBuilder;
    use common_meta::heartbeat::handler::{
        HandlerGroupExecutor, HeartbeatResponseHandlerContext, HeartbeatResponseHandlerExecutor,
    };
    use common_meta::heartbeat::mailbox::{HeartbeatMailbox, MessageMeta};
    use common_meta::instruction::{CacheIdent, Instruction};
    use common_meta::key::schema_name::{SchemaName, SchemaNameKey, SchemaNameValue};
    use common_meta::key::{MetadataKey, SchemaMetadataManager};
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::kv_backend::KvBackend;
    use common_meta::rpc::store::PutRequest;

    use crate::heartbeat::handler::cache_invalidator::InvalidateSchemaCacheHandler;

    #[tokio::test]
    async fn test_invalidate_schema_cache_handler() {
        let inner_kv = Arc::new(MemoryKvBackend::default());
        let cached_kv = Arc::new(CachedKvBackendBuilder::new(inner_kv.clone()).build());
        let schema_metadata_manager = SchemaMetadataManager::new(cached_kv.clone());

        let schema_name = "test_schema";
        let catalog_name = "test_catalog";
        schema_metadata_manager
            .register_region_table_info(
                1,
                "test_table",
                schema_name,
                catalog_name,
                Some(SchemaNameValue {
                    ttl: Some(Duration::from_secs(1)),
                }),
            )
            .await;

        schema_metadata_manager
            .get_schema_options_by_table_id(1)
            .await
            .unwrap();

        let schema_key = SchemaNameKey::new(catalog_name, schema_name).to_bytes();
        let new_schema_value = SchemaNameValue {
            ttl: Some(Duration::from_secs(3)),
        }
        .try_as_raw_value()
        .unwrap();
        inner_kv
            .put(PutRequest {
                key: schema_key.clone(),
                value: new_schema_value,
                prev_kv: false,
            })
            .await
            .unwrap();

        let executor = Arc::new(HandlerGroupExecutor::new(vec![Arc::new(
            InvalidateSchemaCacheHandler::new(cached_kv),
        )]));

        let (tx, _) = tokio::sync::mpsc::channel(8);
        let mailbox = Arc::new(HeartbeatMailbox::new(tx));

        // removes a valid key
        let response = HeartbeatResponse::default();
        let mut ctx: HeartbeatResponseHandlerContext =
            HeartbeatResponseHandlerContext::new(mailbox, response);
        ctx.incoming_message = Some((
            MessageMeta::new_test(1, "hi", "foo", "bar"),
            Instruction::InvalidateCaches(vec![CacheIdent::SchemaName(SchemaName {
                catalog_name: catalog_name.to_string(),
                schema_name: schema_name.to_string(),
            })]),
        ));
        executor.handle(ctx).await.unwrap();

        assert_eq!(
            Some(Duration::from_secs(3)),
            SchemaNameValue::try_from_raw_value(
                &inner_kv.get(&schema_key).await.unwrap().unwrap().value
            )
            .unwrap()
            .unwrap()
            .ttl
        );
    }
}
