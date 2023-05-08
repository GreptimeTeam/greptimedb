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

use std::sync::Arc;

use common_catalog::format_full_table_name;
use common_error::prelude::BoxedError;
use common_meta::instruction::InstructionReply;
use snafu::ResultExt;
use table::engine::EngineContext;
use table::requests::{CloseTableRequest, DropTableRequest};

use crate::error;
use crate::error::Result;
use crate::heartbeat::handler::{InstructionHandler, MessageMeta};
use crate::heartbeat::HeartbeatResponseHandlerContext;

impl InstructionHandler {
    pub(crate) fn handle_close_table(
        &self,
        ctx: &mut HeartbeatResponseHandlerContext,
        meta: MessageMeta,
        engine: String,
        request: CloseTableRequest,
    ) {
        let mailbox = ctx.mailbox.clone();
        let arc = Arc::new(self.clone());

        common_runtime::spawn_bg(async move {
            let result = arc.close_table_inner(engine, request).await;
            let mut mailbox_guard = mailbox.lock().await;

            mailbox_guard.replies.push((
                meta,
                result.map_or_else(
                    |error| InstructionReply::CloseTable {
                        result: false,
                        error: Some(error.to_string()),
                    },
                    |result| InstructionReply::CloseTable {
                        result,
                        error: None,
                    },
                ),
            ));
        });
    }

    async fn close_table_inner(
        self: Arc<Self>,
        engine: String,
        request: CloseTableRequest,
    ) -> Result<bool> {
        let CloseTableRequest {
            catalog_name,
            schema_name,
            table_name,
            ..
        } = &request;

        let table = self
            .catalog_manager
            .table(catalog_name, schema_name, table_name)
            .await
            .context(error::AccessCatalogSnafu)?;

        if table.is_none() {
            return Ok(true);
        }

        let engine =
            self.table_engine_manager
                .engine(&engine)
                .context(error::TableEngineNotFoundSnafu {
                    engine_name: &engine,
                })?;

        let ctx = EngineContext::default();
        let req = DropTableRequest {
            catalog_name: catalog_name.clone(),
            schema_name: schema_name.clone(),
            table_name: table_name.clone(),
        };
        engine
            .drop_table(&ctx, req)
            .await
            .map_err(BoxedError::new)
            .with_context(|_| error::DropTableSnafu {
                table_name: format_full_table_name(catalog_name, schema_name, table_name),
            })?;
        Ok(true)
    }
}
