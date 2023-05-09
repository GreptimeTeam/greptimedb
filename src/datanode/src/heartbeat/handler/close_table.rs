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

use catalog::CatalogManagerRef;
use common_catalog::format_full_table_name;
use common_error::prelude::BoxedError;
use common_meta::instruction::{Instruction, InstructionReply};
use common_telemetry::error;
use snafu::ResultExt;
use table::engine::manager::TableEngineManagerRef;
use table::engine::EngineContext;
use table::requests::{CloseTableRequest, DropTableRequest};

use crate::error;
use crate::error::Result;
use crate::heartbeat::handler::{HeartbeatResponseHandler, MessageMeta};
use crate::heartbeat::HeartbeatResponseHandlerContext;
#[derive(Clone)]
pub struct CloseTableHandler {
    catalog_manager: CatalogManagerRef,
    table_engine_manager: TableEngineManagerRef,
}

impl HeartbeatResponseHandler for CloseTableHandler {
    fn handle(&self, ctx: &mut HeartbeatResponseHandlerContext) -> Result<()> {
        let messages = ctx
            .incoming_messages
            .drain_filter(|(_, instruction)| matches!(instruction, Instruction::CloseRegion { .. }))
            .collect::<Vec<_>>();

        for (meta, instruction) in messages {
            if let Instruction::CloseRegion {
                catalog,
                schema,
                table,
                table_id,
                engine,
                ..
            } = instruction
            {
                self.handle_close_table(
                    ctx,
                    meta,
                    engine,
                    CloseTableRequest {
                        catalog_name: catalog,
                        schema_name: schema,
                        table_name: table,
                        table_id,
                    },
                )
            }
        }

        Ok(())
    }
}

impl CloseTableHandler {
    pub fn new(
        catalog_manager: CatalogManagerRef,
        table_engine_manager: TableEngineManagerRef,
    ) -> Self {
        Self {
            catalog_manager,
            table_engine_manager,
        }
    }

    pub(crate) fn handle_close_table(
        &self,
        ctx: &mut HeartbeatResponseHandlerContext,
        meta: MessageMeta,
        engine: String,
        request: CloseTableRequest,
    ) {
        let mailbox = ctx.mailbox.clone();
        let self_ref = Arc::new(self.clone());

        common_runtime::spawn_bg(async move {
            let result = self_ref.close_table_inner(engine, request).await;

            if let Err(e) = mailbox
                .send((
                    meta,
                    result.map_or_else(
                        |error| InstructionReply::CloseRegion {
                            result: false,
                            error: Some(error.to_string()),
                        },
                        |result| InstructionReply::CloseRegion {
                            result,
                            error: None,
                        },
                    ),
                ))
                .await
            {
                error!(e;"Failed to send reply to mailbox");
            }
        });
    }

    async fn close_table_inner(&self, engine: String, request: CloseTableRequest) -> Result<bool> {
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
