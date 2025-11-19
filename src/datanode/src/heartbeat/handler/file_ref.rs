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
use common_meta::instruction::{GetFileRefs, GetFileRefsReply, InstructionReply};
use store_api::storage::FileRefsManifest;

use crate::heartbeat::handler::{HandlerContext, InstructionHandler};

pub struct GetFileRefsHandler;

#[async_trait::async_trait]
impl InstructionHandler for GetFileRefsHandler {
    type Instruction = GetFileRefs;

    async fn handle(
        &self,
        ctx: &HandlerContext,
        get_file_refs: Self::Instruction,
    ) -> Option<InstructionReply> {
        let region_server = &ctx.region_server;

        // Get the MitoEngine
        let Some(mito_engine) = region_server.mito_engine() else {
            return Some(InstructionReply::GetFileRefs(GetFileRefsReply {
                file_refs_manifest: FileRefsManifest::default(),
                success: false,
                error: Some("MitoEngine not found".to_string()),
            }));
        };
        match mito_engine
            .get_snapshot_of_file_refs(get_file_refs.query_regions, get_file_refs.related_regions)
            .await
        {
            Ok(all_file_refs) => {
                // Return the file references
                Some(InstructionReply::GetFileRefs(GetFileRefsReply {
                    file_refs_manifest: all_file_refs,
                    success: true,
                    error: None,
                }))
            }
            Err(e) => Some(InstructionReply::GetFileRefs(GetFileRefsReply {
                file_refs_manifest: FileRefsManifest::default(),
                success: false,
                error: Some(format!("Failed to get file refs: {}", e.output_msg())),
            })),
        }
    }
}
