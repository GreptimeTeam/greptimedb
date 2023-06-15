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

use catalog::helper::TableGlobalKey;
use catalog::remote::KvCacheInvalidatorRef;
use common_meta::error::Result as MetaResult;
use common_meta::heartbeat::handler::{
    HandleControl, HeartbeatResponseHandler, HeartbeatResponseHandlerContext,
};
use common_meta::ident::TableIdent;
use common_meta::instruction::{Instruction, InstructionReply, SimpleReply};
use common_meta::table_name::TableName;
use common_telemetry::{error, info};
use partition::manager::TableRouteCacheInvalidatorRef;

#[derive(Clone)]
pub struct InvalidateTableCacheHandler {
    backend_cache_invalidator: KvCacheInvalidatorRef,
    table_route_cache_invalidator: TableRouteCacheInvalidatorRef,
}

impl HeartbeatResponseHandler for InvalidateTableCacheHandler {
    fn is_acceptable(&self, ctx: &HeartbeatResponseHandlerContext) -> bool {
        matches!(
            ctx.incoming_message.as_ref(),
            Some((_, Instruction::InvalidateTableCache { .. }))
        )
    }

    fn handle(&self, ctx: &mut HeartbeatResponseHandlerContext) -> MetaResult<HandleControl> {
        // TODO(weny): considers introducing a macro
        let Some((meta, Instruction::InvalidateTableCache(table_ident))) = ctx.incoming_message.take() else {
            unreachable!("InvalidateTableCacheHandler: should be guarded by 'is_acceptable'");
        };

        let mailbox = ctx.mailbox.clone();
        let self_ref = self.clone();
        let TableIdent {
            catalog,
            schema,
            table,
            ..
        } = table_ident;

        common_runtime::spawn_bg(async move {
            self_ref.invalidate_table(&catalog, &schema, &table).await;

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
    pub fn new(
        backend_cache_invalidator: KvCacheInvalidatorRef,
        table_route_cache_invalidator: TableRouteCacheInvalidatorRef,
    ) -> Self {
        Self {
            backend_cache_invalidator,
            table_route_cache_invalidator,
        }
    }

    async fn invalidate_table(&self, catalog_name: &str, schema_name: &str, table_name: &str) {
        let tg_key = TableGlobalKey {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
        }
        .to_string();
        info!("invalidate table cache: {}", tg_key);
        let tg_key = tg_key.as_bytes();

        self.backend_cache_invalidator.invalidate_key(tg_key).await;

        let table = &TableName {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
        };

        self.table_route_cache_invalidator
            .invalidate_table_route(table)
            .await;
    }
}
