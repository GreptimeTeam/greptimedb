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
use common_meta::instruction::{InstructionReply, OpenRegion, SimpleReply};
use common_meta::wal::prepare_wal_option;
use futures_util::future::BoxFuture;
use store_api::path_utils::region_dir;
use store_api::region_request::{RegionOpenRequest, RegionRequest};

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
        }: OpenRegion,
    ) -> BoxFuture<'static, InstructionReply> {
        Box::pin(async move {
            let region_id = Self::region_ident_to_region_id(&region_ident);
            prepare_wal_option(&mut region_options, region_id, &region_wal_options);
            let request = RegionRequest::Open(RegionOpenRequest {
                engine: region_ident.engine,
                region_dir: region_dir(&region_storage_path, region_id),
                options: region_options,
                skip_wal_replay,
            });
            let result = self.region_server.handle_request(region_id, request).await;
            let success = result.is_ok();
            let error = result.as_ref().map_err(|e| e.output_msg()).err();
            InstructionReply::OpenRegion(SimpleReply {
                result: success,
                error,
            })
        })
    }
}
