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

use std::assert_matches::assert_matches;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use api::v1::meta::HeartbeatResponse;
use common_meta::cache_invalidator::KvCacheInvalidator;
use common_meta::heartbeat::handler::{
    HandlerGroupExecutor, HeartbeatResponseHandlerContext, HeartbeatResponseHandlerExecutor,
};
use common_meta::heartbeat::mailbox::{HeartbeatMailbox, MessageMeta};
use common_meta::instruction::{Instruction, InstructionReply, SimpleReply};
use common_meta::key::table_info::TableInfoKey;
use common_meta::key::TableMetaKey;
use partition::manager::TableRouteCacheInvalidator;
use table::metadata::TableId;
use tokio::sync::mpsc;

use super::invalidate_table_cache::InvalidateTableCacheHandler;

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

pub struct MockTableRouteCacheInvalidator {
    inner: Mutex<HashMap<TableId, i32>>,
}

#[async_trait::async_trait]
impl TableRouteCacheInvalidator for MockTableRouteCacheInvalidator {
    async fn invalidate_table_route(&self, table_id: TableId) {
        let _ = self.inner.lock().unwrap().remove(&table_id);
    }
}

#[tokio::test]
async fn test_invalidate_table_cache_handler() {
    let table_id = 1;
    let table_info_key = TableInfoKey::new(table_id);
    let inner = HashMap::from([(table_info_key.as_raw_key(), 1)]);
    let backend = Arc::new(MockKvCacheInvalidator {
        inner: Mutex::new(inner),
    });

    let executor = Arc::new(HandlerGroupExecutor::new(vec![Arc::new(
        InvalidateTableCacheHandler::new(backend.clone()),
    )]));

    let (tx, mut rx) = mpsc::channel(8);
    let mailbox = Arc::new(HeartbeatMailbox::new(tx));

    // removes a valid key
    handle_instruction(
        executor.clone(),
        mailbox.clone(),
        Instruction::InvalidateTableIdCache(table_id),
    )
    .await;

    let (_, reply) = rx.recv().await.unwrap();
    assert_matches!(
        reply,
        InstructionReply::InvalidateTableCache(SimpleReply { result: true, .. })
    );
    assert!(!backend
        .inner
        .lock()
        .unwrap()
        .contains_key(&table_info_key.as_raw_key()));

    // removes a invalid key
    handle_instruction(executor, mailbox, Instruction::InvalidateTableIdCache(0)).await;

    let (_, reply) = rx.recv().await.unwrap();
    assert_matches!(
        reply,
        InstructionReply::InvalidateTableCache(SimpleReply { result: true, .. })
    );
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
    ctx.incoming_message = Some((test_message_meta(1, "hi", "foo", "bar"), instruction));
    executor.handle(ctx).await.unwrap();
}
