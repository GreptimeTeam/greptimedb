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

use common_meta::instruction::{InstructionReply, OpenRegion, SimpleReply};
use common_meta::wal_options_allocator::prepare_wal_options;
use futures_util::future::BoxFuture;
use store_api::path_utils::table_dir;
use store_api::region_request::{PathType, RegionOpenRequest, RegionRequest, ReplayCheckpoint};

use crate::heartbeat::handler::HandlerContext;

impl HandlerContext {
    pub(crate) fn handle_open_region_instruction(
        self,
        OpenRegion {
            region_ident,
            region_storage_path,
            mut region_options,
            region_wal_options,
            skip_wal_replay,
            replay_entry_id,
            metadata_replay_entry_id,
        }: OpenRegion,
    ) -> BoxFuture<'static, Option<InstructionReply>> {
        Box::pin(async move {
            let region_id = Self::region_ident_to_region_id(&region_ident);
            prepare_wal_options(&mut region_options, region_id, &region_wal_options);
            let checkpoint = match (replay_entry_id, metadata_replay_entry_id) {
                (Some(replay_entry_id), Some(metadata_replay_entry_id)) => Some(ReplayCheckpoint {
                    entry_id: replay_entry_id,
                    metadata_entry_id: Some(metadata_replay_entry_id),
                }),
                (Some(replay_entry_id), None) => Some(ReplayCheckpoint {
                    entry_id: replay_entry_id,
                    metadata_entry_id: None,
                }),
                _ => None,
            };
            let request = RegionRequest::Open(RegionOpenRequest {
                engine: region_ident.engine,
                table_dir: table_dir(&region_storage_path, region_id.table_id()),
                path_type: PathType::Bare,
                options: region_options,
                skip_wal_replay,
                checkpoint,
            });
            let result = self.region_server.handle_request(region_id, request).await;
            let success = result.is_ok();
            let error = result.as_ref().map_err(|e| format!("{e:?}")).err();
            Some(InstructionReply::OpenRegion(SimpleReply {
                result: success,
                error,
            }))
        })
    }
}
