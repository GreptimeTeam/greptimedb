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

use common_meta::RegionIdent;
use common_meta::instruction::{InstructionReply, SimpleReply};
use common_telemetry::warn;
use futures::future::join_all;
use store_api::region_request::{RegionCloseRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::error;
use crate::heartbeat::handler::{HandlerContext, InstructionHandler};

#[derive(Debug, Clone, Copy, Default)]
pub struct CloseRegionsHandler;

#[async_trait::async_trait]
impl InstructionHandler for CloseRegionsHandler {
    type Instruction = Vec<RegionIdent>;

    async fn handle(
        &self,
        ctx: &HandlerContext,
        region_idents: Self::Instruction,
    ) -> Option<InstructionReply> {
        let region_ids = region_idents
            .into_iter()
            .map(|region_ident| RegionId::new(region_ident.table_id, region_ident.region_number))
            .collect::<Vec<_>>();

        let futs = region_ids.iter().map(|region_id| {
            ctx.region_server
                .handle_request(*region_id, RegionRequest::Close(RegionCloseRequest {}))
        });

        let results = join_all(futs).await;

        let mut errors = vec![];
        for (region_id, result) in region_ids.into_iter().zip(results.into_iter()) {
            match result {
                Ok(_) => (),
                Err(error::Error::RegionNotFound { .. }) => {
                    warn!(
                        "Received a close regions instruction from meta, but target region:{} is not found.",
                        region_id
                    );
                }
                Err(err) => errors.push(format!("region:{region_id}: {err:?}")),
            }
        }

        if errors.is_empty() {
            return Some(InstructionReply::CloseRegions(SimpleReply {
                result: true,
                error: None,
            }));
        }

        Some(InstructionReply::CloseRegions(SimpleReply {
            result: false,
            error: Some(errors.join("; ")),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use common_meta::RegionIdent;
    use common_meta::heartbeat::handler::{HandleControl, HeartbeatResponseHandler};
    use common_meta::heartbeat::mailbox::MessageMeta;
    use common_meta::instruction::Instruction;
    use mito2::config::MitoConfig;
    use mito2::engine::MITO_ENGINE_NAME;
    use mito2::test_util::{CreateRequestBuilder, TestEnv};
    use store_api::region_request::RegionRequest;
    use store_api::storage::RegionId;

    use crate::heartbeat::handler::RegionHeartbeatResponseHandler;
    use crate::heartbeat::handler::tests::HeartbeatResponseTestEnv;
    use crate::tests::mock_region_server;

    fn close_regions_instruction(region_ids: impl IntoIterator<Item = RegionId>) -> Instruction {
        let region_idents = region_ids
            .into_iter()
            .map(|region_id| RegionIdent {
                table_id: region_id.table_id(),
                region_number: region_id.region_number(),
                datanode_id: 2,
                engine: MITO_ENGINE_NAME.to_string(),
            })
            .collect();

        Instruction::CloseRegions(region_idents)
    }

    #[tokio::test]
    async fn test_close_regions() {
        common_telemetry::init_default_ut_logging();

        let mut region_server = mock_region_server();
        let kv_backend = Arc::new(common_meta::kv_backend::memory::MemoryKvBackend::new());
        let heartbeat_handler =
            RegionHeartbeatResponseHandler::new(region_server.clone(), kv_backend);
        let mut engine_env = TestEnv::with_prefix("close-regions").await;
        let engine = engine_env.create_engine(MitoConfig::default()).await;
        region_server.register_engine(Arc::new(engine.clone()));
        let region_id = RegionId::new(1024, 1);
        let region_id1 = RegionId::new(1024, 2);

        let builder = CreateRequestBuilder::new();
        let create_req = builder.build();
        region_server
            .handle_request(region_id, RegionRequest::Create(create_req))
            .await
            .unwrap();

        let create_req1 = builder.build();
        region_server
            .handle_request(region_id1, RegionRequest::Create(create_req1))
            .await
            .unwrap();
        let meta = MessageMeta::new_test(1, "test", "dn-1", "meta-0");
        let instruction =
            close_regions_instruction([region_id, region_id1, RegionId::new(1024, 3)]);
        let mut heartbeat_env = HeartbeatResponseTestEnv::new();
        let mut ctx = heartbeat_env.create_handler_ctx((meta, instruction));
        let control = heartbeat_handler.handle(&mut ctx).await.unwrap();
        assert_matches!(control, HandleControl::Continue);

        let (_, reply) = heartbeat_env.receiver.recv().await.unwrap();
        let reply = reply.expect_close_regions_reply();
        assert!(reply.result);
        assert!(reply.error.is_none());
        assert!(!engine.is_region_exists(region_id));
        assert!(!engine.is_region_exists(region_id1));
        assert!(!engine.is_region_exists(RegionId::new(1024, 3)));
    }
}
