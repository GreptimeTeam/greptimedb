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

use async_trait::async_trait;
use common_meta::cache_invalidator::{CacheInvalidatorRef, Context};
use common_meta::error::Result as MetaResult;
use common_meta::heartbeat::handler::{
    HandleControl, HeartbeatResponseHandler, HeartbeatResponseHandlerContext,
};
use common_meta::instruction::{Instruction, InstructionReply, SimpleReply};
use common_telemetry::error;
use futures::future::Either;

#[derive(Clone)]
pub struct InvalidateTableCacheHandler {
    cache_invalidator: CacheInvalidatorRef,
}

#[async_trait]
impl HeartbeatResponseHandler for InvalidateTableCacheHandler {
    fn is_acceptable(&self, ctx: &HeartbeatResponseHandlerContext) -> bool {
        matches!(
            ctx.incoming_message.as_ref(),
            Some((_, Instruction::InvalidateTableIdCache { .. }))
                | Some((_, Instruction::InvalidateTableNameCache { .. }))
        )
    }

    async fn handle(&self, ctx: &mut HeartbeatResponseHandlerContext) -> MetaResult<HandleControl> {
        let mailbox = ctx.mailbox.clone();
        let cache_invalidator = self.cache_invalidator.clone();

        let (meta, invalidator) = match ctx.incoming_message.take() {
            Some((meta, Instruction::InvalidateTableIdCache(table_id))) => (
                meta,
                Either::Left(async move {
                    cache_invalidator
                        .invalidate_table_id(&Context::default(), table_id)
                        .await
                }),
            ),
            Some((meta, Instruction::InvalidateTableNameCache(table_name))) => (
                meta,
                Either::Right(async move {
                    cache_invalidator
                        .invalidate_table_name(&Context::default(), table_name)
                        .await
                }),
            ),
            _ => unreachable!("InvalidateTableCacheHandler: should be guarded by 'is_acceptable'"),
        };

        let _handle = common_runtime::spawn_bg(async move {
            // Local cache invalidation always succeeds.
            let _ = invalidator.await;

            if let Err(e) = mailbox
                .send((
                    meta,
                    InstructionReply::InvalidateTableCache(SimpleReply {
                        result: true,
                        error: None,
                    }),
                ))
                .await
            {
                error!(e; "Failed to send reply to mailbox");
            }
        });

        Ok(HandleControl::Done)
    }
}

impl InvalidateTableCacheHandler {
    pub fn new(cache_invalidator: CacheInvalidatorRef) -> Self {
        Self { cache_invalidator }
    }
}
