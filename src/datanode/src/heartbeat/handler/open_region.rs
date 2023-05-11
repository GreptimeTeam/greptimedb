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

use catalog::{CatalogManagerRef, RegisterTableRequest};
use common_catalog::format_full_table_name;
use common_meta::instruction::{Instruction, InstructionReply};
use common_telemetry::{error, warn};
use snafu::ResultExt;
use table::engine::manager::TableEngineManagerRef;
use table::engine::EngineContext;
use table::requests::OpenTableRequest;

use crate::error::{self, Result};
use crate::heartbeat::handler::HeartbeatResponseHandler;
use crate::heartbeat::HeartbeatResponseHandlerContext;

#[derive(Clone)]
pub struct OpenTableHandler {
    catalog_manager: CatalogManagerRef,
    table_engine_manager: TableEngineManagerRef,
}

impl HeartbeatResponseHandler for OpenTableHandler {
    fn is_acceptable(&self, ctx: &HeartbeatResponseHandlerContext) -> bool {
        matches!(
            ctx.incoming_message.as_ref(),
            Some((_, Instruction::OpenRegion { .. }))
        )
    }

    fn handle(&self, ctx: &mut HeartbeatResponseHandlerContext) -> Result<()> {
        match ctx.incoming_message.take() {
            Some((
                meta,
                Instruction::OpenRegion {
                    catalog,
                    schema,
                    table,
                    table_id,
                    engine,
                    region_number,
                },
            )) => {
                ctx.finish();

                let mailbox = ctx.mailbox.clone();
                let self_ref = Arc::new(self.clone());

                common_runtime::spawn_bg(async move {
                    let result = self_ref
                        .open_region_inner(
                            engine,
                            OpenTableRequest {
                                catalog_name: catalog,
                                schema_name: schema,
                                table_name: table,
                                table_id,
                                region_number: Some(region_number),
                            },
                        )
                        .await;

                    if let Err(e) = mailbox
                        .send((
                            meta,
                            result.map_or_else(
                                |error| InstructionReply::OpenRegion {
                                    result: false,
                                    error: Some(error.to_string()),
                                },
                                |result| InstructionReply::OpenRegion {
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
            _ => warn!("Missing match in OpenTableHandler"),
        }

        Ok(())
    }
}

impl OpenTableHandler {
    async fn open_region_inner(&self, engine: String, request: OpenTableRequest) -> Result<bool> {
        let OpenTableRequest {
            catalog_name,
            schema_name,
            table_name,
            ..
        } = &request;
        let engine =
            self.table_engine_manager
                .engine(&engine)
                .context(error::TableEngineNotFoundSnafu {
                    engine_name: &engine,
                })?;
        let ctx = EngineContext::default();

        if let Some(table) = engine
            .open_table(&ctx, request.clone())
            .await
            .with_context(|_| error::OpenTableSnafu {
                table_name: format_full_table_name(catalog_name, schema_name, table_name),
            })?
        {
            let request = RegisterTableRequest {
                catalog: request.catalog_name.clone(),
                schema: request.schema_name.clone(),
                table_name: request.table_name.clone(),
                table_id: request.table_id,
                table,
            };
            self.catalog_manager
                .register_table(request)
                .await
                .with_context(|_| error::RegisterTableSnafu {
                    table_name: format_full_table_name(catalog_name, schema_name, table_name),
                })?;
            Ok(true)
        } else {
            // Case 1:
            // TODO(weny): Fix/Cleanup the broken table manifest
            // The manifest writing operation should be atomic.
            // Therefore, we won't meet this case, in theory.

            // Case 2: The target region was not found in table meta
            Ok(false)
        }
    }
}
