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
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_meta::error::{InvalidHeartbeatResponseSnafu, Result as MetaResult};
use common_meta::heartbeat::handler::{
    HandleControl, HeartbeatResponseHandler, HeartbeatResponseHandlerContext,
};
use common_meta::instruction::{Instruction, InstructionReply, OpenRegion, SimpleReply};
use common_meta::RegionIdent;
use common_query::Output;
use common_telemetry::error;
use snafu::OptionExt;
use store_api::path_utils::region_dir;
use store_api::region_request::{RegionCloseRequest, RegionOpenRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::error::Result;
use crate::region_server::RegionServer;

/// Handler for [Instruction::OpenRegion] and [Instruction::CloseRegion].
#[derive(Clone)]
pub struct RegionHeartbeatResponseHandler {
    region_server: RegionServer,
}

impl RegionHeartbeatResponseHandler {
    pub fn new(region_server: RegionServer) -> Self {
        Self { region_server }
    }

    fn instruction_to_request(instruction: Instruction) -> MetaResult<(RegionId, RegionRequest)> {
        match instruction {
            Instruction::OpenRegion(OpenRegion {
                region_ident,
                region_storage_path,
                options,
            }) => {
                let region_id = Self::region_ident_to_region_id(&region_ident);
                let open_region_req = RegionRequest::Open(RegionOpenRequest {
                    engine: region_ident.engine,
                    region_dir: region_dir(&region_storage_path, region_id),
                    options,
                });
                Ok((region_id, open_region_req))
            }
            Instruction::CloseRegion(region_ident) => {
                let region_id = Self::region_ident_to_region_id(&region_ident);
                let close_region_req = RegionRequest::Close(RegionCloseRequest {});
                Ok((region_id, close_region_req))
            }
            Instruction::InvalidateTableIdCache(_) | Instruction::InvalidateTableNameCache(_) => {
                InvalidHeartbeatResponseSnafu.fail()
            }
            Instruction::DowngradeRegion(_) => {
                // TODO(weny): add it later.
                todo!()
            }
        }
    }

    fn region_ident_to_region_id(region_ident: &RegionIdent) -> RegionId {
        RegionId::new(region_ident.table_id, region_ident.region_number)
    }

    fn reply_template_from_instruction(instruction: &Instruction) -> InstructionReply {
        match instruction {
            Instruction::OpenRegion(_) => InstructionReply::OpenRegion(SimpleReply {
                result: false,
                error: None,
            }),
            Instruction::CloseRegion(_) => InstructionReply::CloseRegion(SimpleReply {
                result: false,
                error: None,
            }),
            Instruction::InvalidateTableIdCache(_) | Instruction::InvalidateTableNameCache(_) => {
                InstructionReply::InvalidateTableCache(SimpleReply {
                    result: false,
                    error: None,
                })
            }
            Instruction::DowngradeRegion(_) => {
                // TODO(weny): add it later.
                todo!()
            }
        }
    }

    fn fill_reply(mut template: InstructionReply, result: Result<Output>) -> InstructionReply {
        let success = result.is_ok();
        let error = result.as_ref().map_err(|e| e.to_string()).err();
        match &mut template {
            InstructionReply::OpenRegion(reply) => {
                reply.result = success;
                reply.error = error;
            }
            InstructionReply::CloseRegion(reply) => match result {
                Err(e) => {
                    if e.status_code() == StatusCode::RegionNotFound {
                        reply.result = true;
                    }
                }
                _ => {
                    reply.result = success;
                    reply.error = error;
                }
            },
            InstructionReply::InvalidateTableCache(reply) => {
                reply.result = success;
                reply.error = error;
            }
            InstructionReply::DowngradeRegion(_) => {
                // TODO(weny): add it later.
                todo!()
            }
        }

        template
    }
}

#[async_trait]
impl HeartbeatResponseHandler for RegionHeartbeatResponseHandler {
    fn is_acceptable(&self, ctx: &HeartbeatResponseHandlerContext) -> bool {
        matches!(
            ctx.incoming_message.as_ref(),
            Some((_, Instruction::OpenRegion { .. })) | Some((_, Instruction::CloseRegion { .. }))
        )
    }

    async fn handle(&self, ctx: &mut HeartbeatResponseHandlerContext) -> MetaResult<HandleControl> {
        let (meta, instruction) = ctx
            .incoming_message
            .take()
            .context(InvalidHeartbeatResponseSnafu)?;

        let mailbox = ctx.mailbox.clone();
        let region_server = self.region_server.clone();
        let reply_template = Self::reply_template_from_instruction(&instruction);
        let (region_id, region_req) = Self::instruction_to_request(instruction)?;
        let _handle = common_runtime::spawn_bg(async move {
            let result = region_server.handle_request(region_id, region_req).await;

            if let Err(e) = mailbox
                .send((meta, Self::fill_reply(reply_template, result)))
                .await
            {
                error!(e; "Failed to send reply to mailbox");
            }
        });

        Ok(HandleControl::Done)
    }
}
