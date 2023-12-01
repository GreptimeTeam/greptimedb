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
use common_meta::error::{InvalidHeartbeatResponseSnafu, Result as MetaResult};
use common_meta::heartbeat::handler::{
    HandleControl, HeartbeatResponseHandler, HeartbeatResponseHandlerContext,
};
use common_meta::instruction::{
    DowngradeRegion, DowngradeRegionReply, Instruction, InstructionReply, OpenRegion, SimpleReply,
};
use common_meta::RegionIdent;
use common_telemetry::error;
use futures::future::BoxFuture;
use snafu::OptionExt;
use store_api::path_utils::region_dir;
use store_api::region_engine::SetReadonlyResponse;
use store_api::region_request::{RegionCloseRequest, RegionOpenRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::region_server::RegionServer;
/// Handler for [Instruction::OpenRegion] and [Instruction::CloseRegion].
#[derive(Clone)]
pub struct RegionHeartbeatResponseHandler {
    region_server: RegionServer,
}

/// Handler of the instruction.
pub type InstructionHandler =
    Box<dyn FnOnce(RegionServer) -> BoxFuture<'static, InstructionReply> + Send>;

impl RegionHeartbeatResponseHandler {
    pub fn new(region_server: RegionServer) -> Self {
        Self { region_server }
    }

    fn build_handler(instruction: Instruction) -> MetaResult<InstructionHandler> {
        match instruction {
            Instruction::OpenRegion(OpenRegion {
                region_ident,
                region_storage_path,
                options,
            }) => Ok(Box::new(|region_server| {
                Box::pin(async move {
                    let region_id = Self::region_ident_to_region_id(&region_ident);
                    let request = RegionRequest::Open(RegionOpenRequest {
                        engine: region_ident.engine,
                        region_dir: region_dir(&region_storage_path, region_id),
                        options,
                    });
                    let result = region_server.handle_request(region_id, request).await;

                    let success = result.is_ok();
                    let error = result.as_ref().map_err(|e| e.to_string()).err();

                    InstructionReply::OpenRegion(SimpleReply {
                        result: success,
                        error,
                    })
                })
            })),
            Instruction::CloseRegion(region_ident) => Ok(Box::new(|region_server| {
                Box::pin(async move {
                    let region_id = Self::region_ident_to_region_id(&region_ident);
                    let request = RegionRequest::Close(RegionCloseRequest {});
                    let result = region_server.handle_request(region_id, request).await;

                    let success = result.is_ok();
                    let error = result.as_ref().map_err(|e| e.to_string()).err();

                    InstructionReply::CloseRegion(SimpleReply {
                        result: success,
                        error,
                    })
                })
            })),
            Instruction::DowngradeRegion(DowngradeRegion { region_id }) => {
                Ok(Box::new(move |region_server| {
                    Box::pin(async move {
                        match region_server.set_readonly_gracefully(region_id).await {
                            Ok(SetReadonlyResponse::Success { last_entry_id }) => {
                                InstructionReply::DowngradeRegion(DowngradeRegionReply {
                                    last_entry_id,
                                    exists: true,
                                    error: None,
                                })
                            }
                            Ok(SetReadonlyResponse::NotFound) => {
                                InstructionReply::DowngradeRegion(DowngradeRegionReply {
                                    last_entry_id: None,
                                    exists: false,
                                    error: None,
                                })
                            }
                            Err(err) => InstructionReply::DowngradeRegion(DowngradeRegionReply {
                                last_entry_id: None,
                                exists: false,
                                error: Some(err.to_string()),
                            }),
                        }
                    })
                }))
            }
            Instruction::UpgradeRegion(_) => {
                todo!()
            }
            Instruction::InvalidateTableIdCache(_) | Instruction::InvalidateTableNameCache(_) => {
                InvalidHeartbeatResponseSnafu.fail()
            }
        }
    }

    fn region_ident_to_region_id(region_ident: &RegionIdent) -> RegionId {
        RegionId::new(region_ident.table_id, region_ident.region_number)
    }
}

#[async_trait]
impl HeartbeatResponseHandler for RegionHeartbeatResponseHandler {
    fn is_acceptable(&self, ctx: &HeartbeatResponseHandlerContext) -> bool {
        matches!(
            ctx.incoming_message.as_ref(),
            Some((_, Instruction::OpenRegion { .. }))
                | Some((_, Instruction::CloseRegion { .. }))
                | Some((_, Instruction::DowngradeRegion { .. }))
        )
    }

    async fn handle(&self, ctx: &mut HeartbeatResponseHandlerContext) -> MetaResult<HandleControl> {
        let (meta, instruction) = ctx
            .incoming_message
            .take()
            .context(InvalidHeartbeatResponseSnafu)?;

        let mailbox = ctx.mailbox.clone();
        let region_server = self.region_server.clone();
        let handler = Self::build_handler(instruction)?;
        let _handle = common_runtime::spawn_bg(async move {
            let reply = handler(region_server).await;

            if let Err(e) = mailbox.send((meta, reply)).await {
                error!(e; "Failed to send reply to mailbox");
            }
        });

        Ok(HandleControl::Done)
    }
}
