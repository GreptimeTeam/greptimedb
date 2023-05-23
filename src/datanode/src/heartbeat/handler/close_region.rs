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

use catalog::{CatalogManagerRef, DeregisterTableRequest};
use common_catalog::format_full_table_name;
use common_meta::instruction::{Instruction, InstructionReply, RegionIdent, SimpleReply};
use common_telemetry::{error, warn};
use log::info;
use snafu::ResultExt;
use store_api::storage::RegionNumber;
use table::engine::manager::TableEngineManagerRef;
use table::engine::{CloseTableResult, EngineContext, TableReference};
use table::requests::CloseTableRequest;

use crate::error::{self, Result};
use crate::heartbeat::handler::{HandleControl, HeartbeatResponseHandler};
use crate::heartbeat::HeartbeatResponseHandlerContext;

#[derive(Clone)]
pub struct CloseRegionHandler {
    catalog_manager: CatalogManagerRef,
    table_engine_manager: TableEngineManagerRef,
}

impl HeartbeatResponseHandler for CloseRegionHandler {
    fn is_acceptable(&self, ctx: &HeartbeatResponseHandlerContext) -> bool {
        matches!(
            ctx.incoming_message.as_ref(),
            Some((_, Instruction::CloseRegion { .. }))
        )
    }

    fn handle(&self, ctx: &mut HeartbeatResponseHandlerContext) -> Result<HandleControl> {
        let Some((meta, Instruction::CloseRegion(region_ident))) = ctx.incoming_message.take() else {
            unreachable!("CloseRegionHandler: should be guarded by 'is_acceptable'");
        };

        let mailbox = ctx.mailbox.clone();
        let self_ref = Arc::new(self.clone());

        let RegionIdent {
            engine,
            catalog,
            schema,
            table,
            region_number,
            ..
        } = region_ident;

        common_runtime::spawn_bg(async move {
            let result = self_ref
                .close_region_inner(
                    engine,
                    &TableReference::full(&catalog, &schema, &table),
                    vec![region_number],
                )
                .await;

            if let Err(e) = mailbox
                .send((meta, CloseRegionHandler::map_result(result)))
                .await
            {
                error!(e;"Failed to send reply to mailbox");
            }
        });

        Ok(HandleControl::Done)
    }
}

impl CloseRegionHandler {
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
                InstructionReply::CloseRegion(SimpleReply {
                    result: false,
                    error: Some(error.to_string()),
                })
            },
            |result| {
                InstructionReply::CloseRegion(SimpleReply {
                    result,
                    error: None,
                })
            },
        )
    }

    /// Returns true if a table or target regions have been closed.
    async fn regions_closed(
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
                if region_exist {
                    return Ok(false);
                }
            }
        }
        // Returns true if table not exist
        Ok(true)
    }

    async fn close_region_inner(
        &self,
        engine: String,
        table_ref: &TableReference<'_>,
        region_numbers: Vec<RegionNumber>,
    ) -> Result<bool> {
        let engine =
            self.table_engine_manager
                .engine(&engine)
                .context(error::TableEngineNotFoundSnafu {
                    engine_name: &engine,
                })?;
        let ctx = EngineContext::default();

        if self
            .regions_closed(
                table_ref.catalog,
                table_ref.schema,
                table_ref.table,
                &region_numbers,
            )
            .await?
        {
            return Ok(true);
        }

        if engine
            .get_table(&ctx, table_ref)
            .with_context(|_| error::GetTableSnafu {
                table_name: table_ref.to_string(),
            })?
            .is_some()
        {
            return match engine
                .close_table(
                    &ctx,
                    CloseTableRequest {
                        catalog_name: table_ref.catalog.to_string(),
                        schema_name: table_ref.schema.to_string(),
                        table_name: table_ref.table.to_string(),
                        region_numbers: region_numbers.clone(),
                    },
                )
                .await
                .with_context(|_| error::CloseTableSnafu {
                    table_name: table_ref.to_string(),
                    region_numbers: region_numbers.clone(),
                })? {
                CloseTableResult::NotFound | CloseTableResult::Released(_) => {
                    // Deregister table if The table released.
                    self.deregister_table(table_ref).await
                }
                CloseTableResult::PartialClosed(regions) => {
                    // Requires caller to update the region_numbers
                    info!(
                        "Close partial regions: {:?} in table: {}",
                        regions, table_ref
                    );
                    Ok(true)
                }
            };
        }

        warn!("Trying to close a non-existing table: {}", table_ref);
        // Table doesn't exist
        Ok(true)
    }

    async fn deregister_table(&self, table_ref: &TableReference<'_>) -> Result<bool> {
        self.catalog_manager
            .deregister_table(DeregisterTableRequest {
                catalog: table_ref.catalog.to_string(),
                schema: table_ref.schema.to_string(),
                table_name: table_ref.table.to_string(),
            })
            .await
            .with_context(|_| error::DeregisterTableSnafu {
                table_name: table_ref.to_string(),
            })
    }
}
