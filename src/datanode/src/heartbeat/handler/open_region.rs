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
use common_meta::wal_provider::prepare_wal_options;
use store_api::path_utils::table_dir;
use store_api::region_request::{PathType, RegionOpenRequest};
use store_api::storage::RegionId;

use crate::heartbeat::handler::{HandlerContext, InstructionHandler};

pub struct OpenRegionsHandler {
    pub open_region_parallelism: usize,
}

#[async_trait::async_trait]
impl InstructionHandler for OpenRegionsHandler {
    type Instruction = Vec<OpenRegion>;
    async fn handle(
        &self,
        ctx: &HandlerContext,
        open_regions: Self::Instruction,
    ) -> Option<InstructionReply> {
        let requests = open_regions
            .into_iter()
            .map(|open_region| {
                let OpenRegion {
                    region_ident,
                    region_storage_path,
                    mut region_options,
                    region_wal_options,
                    skip_wal_replay,
                } = open_region;
                let region_id = RegionId::new(region_ident.table_id, region_ident.region_number);
                prepare_wal_options(&mut region_options, region_id, &region_wal_options);
                let request = RegionOpenRequest {
                    engine: region_ident.engine,
                    table_dir: table_dir(&region_storage_path, region_id.table_id()),
                    path_type: PathType::Bare,
                    options: region_options,
                    skip_wal_replay,
                    checkpoint: None,
                };
                (region_id, request)
            })
            .collect::<Vec<_>>();

        let result = ctx
            .region_server
            .handle_batch_open_requests(self.open_region_parallelism, requests, false)
            .await;
        let success = result.is_ok();
        let error = result.as_ref().map_err(|e| format!("{e:?}")).err();

        Some(InstructionReply::OpenRegions(SimpleReply {
            result: success,
            error,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashMap;
    use std::sync::Arc;

    use common_meta::RegionIdent;
    use common_meta::heartbeat::handler::{HandleControl, HeartbeatResponseHandler};
    use common_meta::heartbeat::mailbox::MessageMeta;
    use common_meta::instruction::{Instruction, OpenRegion};
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use mito2::config::MitoConfig;
    use mito2::engine::MITO_ENGINE_NAME;
    use mito2::test_util::{CreateRequestBuilder, TestEnv};
    use store_api::path_utils::table_dir;
    use store_api::region_request::{RegionCloseRequest, RegionRequest};
    use store_api::storage::RegionId;

    use crate::heartbeat::handler::RegionHeartbeatResponseHandler;
    use crate::heartbeat::handler::tests::HeartbeatResponseTestEnv;
    use crate::tests::mock_region_server;

    fn open_regions_instruction(
        region_ids: impl IntoIterator<Item = RegionId>,
        storage_path: &str,
    ) -> Instruction {
        let region_idents = region_ids
            .into_iter()
            .map(|region_id| OpenRegion {
                region_ident: RegionIdent {
                    datanode_id: 0,
                    table_id: region_id.table_id(),
                    region_number: region_id.region_number(),
                    engine: MITO_ENGINE_NAME.to_string(),
                },
                region_storage_path: storage_path.to_string(),
                region_options: HashMap::new(),
                region_wal_options: HashMap::new(),
                skip_wal_replay: false,
            })
            .collect();

        Instruction::OpenRegions(region_idents)
    }

    #[tokio::test]
    async fn test_open_regions() {
        common_telemetry::init_default_ut_logging();

        let mut region_server = mock_region_server();
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let heartbeat_handler =
            RegionHeartbeatResponseHandler::new(region_server.clone(), kv_backend);
        let mut engine_env = TestEnv::with_prefix("open-regions").await;
        let engine = engine_env.create_engine(MitoConfig::default()).await;
        region_server.register_engine(Arc::new(engine.clone()));
        let region_id = RegionId::new(1024, 1);
        let region_id1 = RegionId::new(1024, 2);
        let storage_path = "test";
        let builder = CreateRequestBuilder::new();
        let mut create_req = builder.build();
        create_req.table_dir = table_dir(storage_path, region_id.table_id());
        region_server
            .handle_request(region_id, RegionRequest::Create(create_req))
            .await
            .unwrap();
        let mut create_req1 = builder.build();
        create_req1.table_dir = table_dir(storage_path, region_id1.table_id());
        region_server
            .handle_request(region_id1, RegionRequest::Create(create_req1))
            .await
            .unwrap();
        region_server
            .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
            .await
            .unwrap();
        region_server
            .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
            .await
            .unwrap();

        let meta = MessageMeta::new_test(1, "test", "dn-1", "me-0");
        let instruction = open_regions_instruction([region_id, region_id1], storage_path);
        let mut heartbeat_env = HeartbeatResponseTestEnv::new();
        let mut ctx = heartbeat_env.create_handler_ctx((meta, Default::default(), instruction));
        let control = heartbeat_handler.handle(&mut ctx).await.unwrap();
        assert_matches!(control, HandleControl::Continue);
        let (_, reply) = heartbeat_env.receiver.recv().await.unwrap();

        let reply = reply.expect_open_regions_reply();
        assert!(reply.result);
        assert!(reply.error.is_none());

        assert!(engine.is_region_exists(region_id));
        assert!(engine.is_region_exists(region_id1));
    }
}
