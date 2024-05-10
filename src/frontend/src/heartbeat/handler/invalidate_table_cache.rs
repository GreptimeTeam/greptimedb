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
use common_meta::instruction::Instruction;
use common_telemetry::debug;

#[derive(Clone)]
pub struct InvalidateTableCacheHandler {
    cache_invalidator: CacheInvalidatorRef,
}

#[async_trait]
impl HeartbeatResponseHandler for InvalidateTableCacheHandler {
    fn is_acceptable(&self, ctx: &HeartbeatResponseHandlerContext) -> bool {
        matches!(
            ctx.incoming_message.as_ref(),
            Some((_, Instruction::InvalidateCaches(_)))
        )
    }

    async fn handle(&self, ctx: &mut HeartbeatResponseHandlerContext) -> MetaResult<HandleControl> {
        let Some((_, Instruction::InvalidateCaches(caches))) = ctx.incoming_message.take() else {
            unreachable!("InvalidateTableCacheHandler: should be guarded by 'is_acceptable'")
        };

        debug!(
            "InvalidateTableCacheHandler: invalidating caches: {:?}",
            caches
        );

        // Invalidate local cache always success
        let _ = self
            .cache_invalidator
            .invalidate(&Context::default(), &caches)
            .await?;

        Ok(HandleControl::Done)
    }
}

impl InvalidateTableCacheHandler {
    pub fn new(cache_invalidator: CacheInvalidatorRef) -> Self {
        Self { cache_invalidator }
    }
}
