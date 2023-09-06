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
use catalog::remote::KvCacheInvalidatorRef;
use common_meta::error::Result as MetaResult;
use common_meta::heartbeat::handler::{
    HandleControl, HeartbeatResponseHandler, HeartbeatResponseHandlerContext,
};
use common_meta::ident::TableIdent;
use common_meta::instruction::{Instruction, InstructionReply, SimpleReply};
use common_meta::key::table_info::TableInfoKey;
use common_meta::key::table_name::TableNameKey;
use common_meta::key::table_route::NextTableRouteKey;
use common_meta::key::TableMetaKey;
use common_telemetry::error;

#[derive(Clone)]
pub struct InvalidateTableCacheHandler {
    backend_cache_invalidator: KvCacheInvalidatorRef,
}

#[async_trait]
impl HeartbeatResponseHandler for InvalidateTableCacheHandler {
    fn is_acceptable(&self, ctx: &HeartbeatResponseHandlerContext) -> bool {
        matches!(
            ctx.incoming_message.as_ref(),
            Some((_, Instruction::InvalidateTableCache { .. }))
        )
    }

    async fn handle(&self, ctx: &mut HeartbeatResponseHandlerContext) -> MetaResult<HandleControl> {
        // TODO(weny): considers introducing a macro
        let Some((meta, Instruction::InvalidateTableCache(table_ident))) =
            ctx.incoming_message.take()
        else {
            unreachable!("InvalidateTableCacheHandler: should be guarded by 'is_acceptable'");
        };

        let mailbox = ctx.mailbox.clone();
        let self_ref = self.clone();
        let _handle = common_runtime::spawn_bg(async move {
            self_ref.invalidate_table_cache(table_ident).await;

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
    pub fn new(backend_cache_invalidator: KvCacheInvalidatorRef) -> Self {
        Self {
            backend_cache_invalidator,
        }
    }

    async fn invalidate_table_cache(&self, table_ident: TableIdent) {
        let table_id = table_ident.table_id;
        self.backend_cache_invalidator
            .invalidate_key(&TableInfoKey::new(table_id).as_raw_key())
            .await;

        self.backend_cache_invalidator
            .invalidate_key(
                &TableNameKey::new(
                    &table_ident.catalog,
                    &table_ident.schema,
                    &table_ident.table,
                )
                .as_raw_key(),
            )
            .await;

        self.backend_cache_invalidator
            .invalidate_key(&NextTableRouteKey { table_id }.as_raw_key())
            .await;
    }
}
