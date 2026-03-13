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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use api::v1::meta::HeartbeatResponse;
use common_meta::cache_invalidator::KvCacheInvalidator;
use common_meta::heartbeat::handler::invalidate_table_cache::InvalidateCacheHandler;
use common_meta::heartbeat::handler::{
    HandlerGroupExecutor, HeartbeatResponseHandlerContext, HeartbeatResponseHandlerExecutor,
};
use common_meta::heartbeat::mailbox::{HeartbeatMailbox, MessageMeta};
use common_meta::instruction::{CacheIdent, Instruction};
use common_meta::key::MetadataKey;
use common_meta::key::schema_name::{SchemaName, SchemaNameKey};
use common_meta::key::table_info::TableInfoKey;
use common_telemetry::tracing_context::TracingContext;
use tokio::sync::mpsc;

#[derive(Default)]
pub struct MockKvCacheInvalidator {
    inner: Mutex<HashMap<Vec<u8>, i32>>,
}

#[async_trait::async_trait]
impl KvCacheInvalidator for MockKvCacheInvalidator {
    async fn invalidate_key(&self, key: &[u8]) {
        let _ = self.inner.lock().unwrap().remove(key);
    }
}

pub fn test_message_meta(id: u64, subject: &str, to: &str, from: &str) -> MessageMeta {
    MessageMeta {
        id,
        subject: subject.to_string(),
        to: to.to_string(),
        from: from.to_string(),
    }
}

async fn handle_instruction(
    executor: Arc<dyn HeartbeatResponseHandlerExecutor>,
    mailbox: Arc<HeartbeatMailbox>,
    instruction: Instruction,
) {
    let response = HeartbeatResponse::default();
    let mut ctx: HeartbeatResponseHandlerContext =
        HeartbeatResponseHandlerContext::new(mailbox, response);
    ctx.incoming_message = Some((
        test_message_meta(1, "hi", "foo", "bar"),
        TracingContext::new(),
        instruction,
    ));
    executor.handle(ctx).await.unwrap();
}

#[tokio::test]
async fn test_invalidate_table_cache_handler() {
    let table_id = 1;
    let table_info_key = TableInfoKey::new(table_id);
    let inner = HashMap::from([(table_info_key.to_bytes(), 1)]);
    let backend = Arc::new(MockKvCacheInvalidator {
        inner: Mutex::new(inner),
    });

    let executor = Arc::new(HandlerGroupExecutor::new(vec![Arc::new(
        InvalidateCacheHandler::new(backend.clone()),
    )]));

    let (tx, _) = mpsc::channel(8);
    let mailbox = Arc::new(HeartbeatMailbox::new(tx));

    // removes a valid key
    handle_instruction(
        executor.clone(),
        mailbox.clone(),
        Instruction::InvalidateCaches(vec![CacheIdent::TableId(table_id)]),
    )
    .await;

    assert!(
        !backend
            .inner
            .lock()
            .unwrap()
            .contains_key(&table_info_key.to_bytes())
    );

    // removes a invalid key
    handle_instruction(
        executor,
        mailbox,
        Instruction::InvalidateCaches(vec![CacheIdent::TableId(0)]),
    )
    .await;
}

#[tokio::test]
async fn test_invalidate_schema_key_handler() {
    let (catalog, schema) = ("foo", "bar");
    let schema_key = SchemaNameKey { catalog, schema };
    let inner = HashMap::from([(schema_key.to_bytes(), 1)]);
    let backend = Arc::new(MockKvCacheInvalidator {
        inner: Mutex::new(inner),
    });

    let executor = Arc::new(HandlerGroupExecutor::new(vec![Arc::new(
        InvalidateCacheHandler::new(backend.clone()),
    )]));

    let (tx, _) = mpsc::channel(8);
    let mailbox = Arc::new(HeartbeatMailbox::new(tx));

    // removes a valid key
    let valid_key = SchemaName {
        catalog_name: catalog.to_string(),
        schema_name: schema.to_string(),
    };
    handle_instruction(
        executor.clone(),
        mailbox.clone(),
        Instruction::InvalidateCaches(vec![CacheIdent::SchemaName(valid_key.clone())]),
    )
    .await;

    assert!(
        !backend
            .inner
            .lock()
            .unwrap()
            .contains_key(&schema_key.to_bytes())
    );

    // removes a invalid key
    handle_instruction(
        executor,
        mailbox,
        Instruction::InvalidateCaches(vec![CacheIdent::SchemaName(valid_key)]),
    )
    .await;
}
