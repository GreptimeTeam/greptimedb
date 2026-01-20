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

use common_meta::instruction::{
    EnterStagingRegion, EnterStagingRegionReply, EnterStagingRegionsReply, InstructionReply,
};
use common_telemetry::{error, warn};
use futures::future::join_all;
use store_api::region_request::{EnterStagingRequest, RegionRequest};

use crate::heartbeat::handler::{HandlerContext, InstructionHandler};

#[derive(Debug, Clone, Copy, Default)]
pub struct EnterStagingRegionsHandler;

#[async_trait::async_trait]
impl InstructionHandler for EnterStagingRegionsHandler {
    type Instruction = Vec<EnterStagingRegion>;

    async fn handle(
        &self,
        ctx: &HandlerContext,
        enter_staging: Self::Instruction,
    ) -> Option<InstructionReply> {
        let futures = enter_staging.into_iter().map(|enter_staging_region| {
            Self::handle_enter_staging_region(ctx, enter_staging_region)
        });
        let results = join_all(futures).await;
        Some(InstructionReply::EnterStagingRegions(
            EnterStagingRegionsReply::new(results),
        ))
    }
}

impl EnterStagingRegionsHandler {
    async fn handle_enter_staging_region(
        ctx: &HandlerContext,
        EnterStagingRegion {
            region_id,
            partition_expr,
        }: EnterStagingRegion,
    ) -> EnterStagingRegionReply {
        common_telemetry::info!(
            "Datanode received enter staging region: {}, partition_expr: {}",
            region_id,
            partition_expr
        );
        let Some(writable) = ctx.region_server.is_region_leader(region_id) else {
            warn!("Region: {} is not found", region_id);
            return EnterStagingRegionReply {
                region_id,
                ready: false,
                exists: false,
                error: None,
            };
        };
        if !writable {
            warn!("Region: {} is not writable", region_id);
            return EnterStagingRegionReply {
                region_id,
                ready: false,
                exists: true,
                error: Some("Region is not writable".into()),
            };
        }

        match ctx
            .region_server
            .handle_request(
                region_id,
                RegionRequest::EnterStaging(EnterStagingRequest { partition_expr }),
            )
            .await
        {
            Ok(_) => EnterStagingRegionReply {
                region_id,
                ready: true,
                exists: true,
                error: None,
            },
            Err(err) => {
                error!(err; "Failed to enter staging region, region_id: {}", region_id);
                EnterStagingRegionReply {
                    region_id,
                    ready: false,
                    exists: true,
                    error: Some(format!("{err:?}")),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_meta::instruction::EnterStagingRegion;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use mito2::config::MitoConfig;
    use mito2::engine::MITO_ENGINE_NAME;
    use mito2::test_util::{CreateRequestBuilder, TestEnv};
    use store_api::path_utils::table_dir;
    use store_api::region_engine::RegionRole;
    use store_api::region_request::RegionRequest;
    use store_api::storage::RegionId;

    use crate::heartbeat::handler::enter_staging::EnterStagingRegionsHandler;
    use crate::heartbeat::handler::{HandlerContext, InstructionHandler};
    use crate::region_server::RegionServer;
    use crate::tests::{MockRegionEngine, mock_region_server};

    const PARTITION_EXPR: &str = "partition_expr";

    #[tokio::test]
    async fn test_region_not_exist() {
        let mut mock_region_server = mock_region_server();
        let (mock_engine, _) = MockRegionEngine::new(MITO_ENGINE_NAME);
        mock_region_server.register_engine(mock_engine);
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let handler_context = HandlerContext::new_for_test(mock_region_server, kv_backend);
        let region_id = RegionId::new(1024, 1);
        let replies = EnterStagingRegionsHandler
            .handle(
                &handler_context,
                vec![EnterStagingRegion {
                    region_id,
                    partition_expr: "".to_string(),
                }],
            )
            .await
            .unwrap();
        let replies = replies.expect_enter_staging_regions_reply();
        let reply = &replies[0];
        assert!(!reply.exists);
        assert!(reply.error.is_none());
        assert!(!reply.ready);
    }

    #[tokio::test]
    async fn test_region_not_writable() {
        let mock_region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);
        let (mock_engine, _) =
            MockRegionEngine::with_custom_apply_fn(MITO_ENGINE_NAME, |region_engine| {
                region_engine.mock_role = Some(Some(RegionRole::Follower));
                region_engine.handle_request_mock_fn = Some(Box::new(|_, _| Ok(0)));
            });
        mock_region_server.register_test_region(region_id, mock_engine);
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let handler_context = HandlerContext::new_for_test(mock_region_server, kv_backend);
        let replies = EnterStagingRegionsHandler
            .handle(
                &handler_context,
                vec![EnterStagingRegion {
                    region_id,
                    partition_expr: "".to_string(),
                }],
            )
            .await
            .unwrap();
        let replies = replies.expect_enter_staging_regions_reply();
        let reply = &replies[0];
        assert!(reply.exists);
        assert!(reply.error.is_some());
        assert!(!reply.ready);
    }

    async fn prepare_region(region_server: &RegionServer) {
        let builder = CreateRequestBuilder::new();
        let mut create_req = builder.build();
        create_req.table_dir = table_dir("test", 1024);
        let region_id = RegionId::new(1024, 1);
        region_server
            .handle_request(region_id, RegionRequest::Create(create_req))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_enter_staging() {
        let mut region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);
        let mut engine_env = TestEnv::new().await;
        let engine = engine_env.create_engine(MitoConfig::default()).await;
        region_server.register_engine(Arc::new(engine.clone()));
        prepare_region(&region_server).await;

        let kv_backend = Arc::new(MemoryKvBackend::new());
        let handler_context = HandlerContext::new_for_test(region_server, kv_backend);
        let replies = EnterStagingRegionsHandler
            .handle(
                &handler_context,
                vec![EnterStagingRegion {
                    region_id,
                    partition_expr: PARTITION_EXPR.to_string(),
                }],
            )
            .await
            .unwrap();
        let replies = replies.expect_enter_staging_regions_reply();
        let reply = &replies[0];
        assert!(reply.exists);
        assert!(reply.error.is_none());
        assert!(reply.ready);

        // Should be ok to enter staging mode again with the same partition expr
        let replies = EnterStagingRegionsHandler
            .handle(
                &handler_context,
                vec![EnterStagingRegion {
                    region_id,
                    partition_expr: PARTITION_EXPR.to_string(),
                }],
            )
            .await
            .unwrap();
        let replies = replies.expect_enter_staging_regions_reply();
        let reply = &replies[0];
        assert!(reply.exists);
        assert!(reply.error.is_none());
        assert!(reply.ready);

        // Should throw error if try to enter staging mode again with a different partition expr
        let replies = EnterStagingRegionsHandler
            .handle(
                &handler_context,
                vec![EnterStagingRegion {
                    region_id,
                    partition_expr: "".to_string(),
                }],
            )
            .await
            .unwrap();
        let replies = replies.expect_enter_staging_regions_reply();
        let reply = &replies[0];
        assert!(reply.exists);
        assert!(reply.error.is_some());
        assert!(!reply.ready);
    }
}
