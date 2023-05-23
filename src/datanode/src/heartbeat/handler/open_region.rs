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

use catalog::error::Error as CatalogError;
use catalog::{CatalogManagerRef, RegisterTableRequest};
use common_catalog::format_full_table_name;
use common_meta::instruction::{Instruction, InstructionReply, RegionIdent, SimpleReply};
use common_telemetry::{error, warn};
use snafu::ResultExt;
use store_api::storage::RegionNumber;
use table::engine::manager::TableEngineManagerRef;
use table::engine::EngineContext;
use table::requests::OpenTableRequest;

use crate::error::{self, Result};
use crate::heartbeat::handler::HeartbeatResponseHandler;
use crate::heartbeat::HeartbeatResponseHandlerContext;

#[derive(Clone)]
pub struct OpenRegionHandler {
    catalog_manager: CatalogManagerRef,
    table_engine_manager: TableEngineManagerRef,
}

impl HeartbeatResponseHandler for OpenRegionHandler {
    fn is_acceptable(&self, ctx: &HeartbeatResponseHandlerContext) -> bool {
        matches!(
            ctx.incoming_message,
            Some((_, Instruction::OpenRegion { .. }))
        )
    }

    fn handle(&self, ctx: &mut HeartbeatResponseHandlerContext) -> Result<()> {
        let Some((meta, Instruction::OpenRegion(region_ident))) = ctx.incoming_message.take() else {
            unreachable!("OpenRegionHandler: should be guarded by 'is_acceptable'");
        };

        ctx.finish();
        let mailbox = ctx.mailbox.clone();
        let self_ref = Arc::new(self.clone());

        common_runtime::spawn_bg(async move {
            let (engine, request) = OpenRegionHandler::prepare_request(region_ident);
            let result = self_ref.open_region_inner(engine, request).await;
            if let Err(e) = mailbox
                .send((meta, OpenRegionHandler::map_result(result)))
                .await
            {
                error!(e; "Failed to send reply to mailbox");
            }
        });
        Ok(())
    }
}

impl OpenRegionHandler {
    pub fn new(
        catalog_manager: CatalogManagerRef,
        table_engine_manager: TableEngineManagerRef,
    ) -> Self {
        Self {
            catalog_manager,
            table_engine_manager,
        }
    }

    fn map_result(result: Result<bool>) -> InstructionReply {
        result.map_or_else(
            |error| {
                InstructionReply::OpenRegion(SimpleReply {
                    result: false,
                    error: Some(error.to_string()),
                })
            },
            |result| {
                InstructionReply::OpenRegion(SimpleReply {
                    result,
                    error: None,
                })
            },
        )
    }

    fn prepare_request(ident: RegionIdent) -> (String, OpenTableRequest) {
        let RegionIdent {
            catalog,
            schema,
            table,
            table_id,
            region_number,
            engine,
            ..
        } = ident;

        (
            engine,
            OpenTableRequest {
                catalog_name: catalog,
                schema_name: schema,
                table_name: table,
                table_id,
                region_numbers: vec![region_number],
            },
        )
    }

    /// Returns true if table has been opened.
    async fn check_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        region_numbers: &[RegionNumber],
    ) -> Result<bool> {
        if let Some(table) = self
            .catalog_manager
            .table(catalog_name, schema_name, table_name)
            .await
            .context(error::AccessCatalogSnafu)?
        {
            for r in region_numbers {
                let region_exist =
                    table
                        .contains_region(*r)
                        .with_context(|_| error::CheckRegionSnafu {
                            table_name: format_full_table_name(
                                catalog_name,
                                schema_name,
                                table_name,
                            ),
                            region_number: *r,
                        })?;
                if !region_exist {
                    warn!(
                        "Failed to check table: {}, region: {} does not exist",
                        format_full_table_name(catalog_name, schema_name, table_name,),
                        r
                    );
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    async fn open_region_inner(&self, engine: String, request: OpenTableRequest) -> Result<bool> {
        let OpenTableRequest {
            catalog_name,
            schema_name,
            table_name,
            region_numbers,
            ..
        } = &request;
        let engine =
            self.table_engine_manager
                .engine(&engine)
                .context(error::TableEngineNotFoundSnafu {
                    engine_name: &engine,
                })?;
        let ctx = EngineContext::default();

        if self
            .check_table(catalog_name, schema_name, table_name, region_numbers)
            .await?
        {
            return Ok(true);
        }

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
            let result = self.catalog_manager.register_table(request).await;
            match result {
                Ok(_) | Err(CatalogError::TableExists { .. }) => Ok(true),
                e => e.with_context(|_| error::RegisterTableSnafu {
                    table_name: format_full_table_name(catalog_name, schema_name, table_name),
                }),
            }
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
