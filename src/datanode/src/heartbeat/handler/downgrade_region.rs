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

use common_error::ext::ErrorExt;
use common_meta::instruction::{DowngradeRegion, DowngradeRegionReply, InstructionReply};
use futures_util::future::BoxFuture;
use store_api::region_engine::SetReadonlyResponse;

use crate::heartbeat::handler::HandlerContext;

impl HandlerContext {
    pub(crate) fn handle_downgrade_region_instruction(
        self,
        DowngradeRegion { region_id }: DowngradeRegion,
    ) -> BoxFuture<'static, InstructionReply> {
        Box::pin(async move {
            match self.region_server.set_readonly_gracefully(region_id).await {
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
                    exists: true,
                    error: Some(err.output_msg()),
                }),
            }
        })
    }
}
