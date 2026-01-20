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

use common_meta::instruction::{InstructionReply, SyncRegion, SyncRegionReply, SyncRegionsReply};
use common_telemetry::{error, info, warn};
use futures::future::join_all;

use crate::heartbeat::handler::{HandlerContext, InstructionHandler};

/// Handler for [SyncRegion] instruction.
/// It syncs the region from a manifest or another region.
#[derive(Debug, Clone, Copy, Default)]
pub struct SyncRegionHandler;

#[async_trait::async_trait]
impl InstructionHandler for SyncRegionHandler {
    type Instruction = Vec<SyncRegion>;

    /// Handles a batch of [SyncRegion] instructions.
    async fn handle(
        &self,
        ctx: &HandlerContext,
        regions: Self::Instruction,
    ) -> Option<InstructionReply> {
        let futures = regions
            .into_iter()
            .map(|sync_region| Self::handle_sync_region(ctx, sync_region));
        let results = join_all(futures).await;

        Some(InstructionReply::SyncRegions(SyncRegionsReply::new(
            results,
        )))
    }
}

impl SyncRegionHandler {
    /// Handles a single [SyncRegion] instruction.
    async fn handle_sync_region(
        ctx: &HandlerContext,
        SyncRegion { region_id, request }: SyncRegion,
    ) -> SyncRegionReply {
        let Some(writable) = ctx.region_server.is_region_leader(region_id) else {
            warn!("Region: {} is not found", region_id);
            return SyncRegionReply {
                region_id,
                ready: false,
                exists: false,
                error: None,
            };
        };

        if !writable {
            warn!("Region: {} is not writable", region_id);
            return SyncRegionReply {
                region_id,
                ready: false,
                exists: true,
                error: Some("Region is not writable".into()),
            };
        }

        match ctx.region_server.sync_region(region_id, request).await {
            Ok(_) => {
                info!("Successfully synced region: {}", region_id);
                SyncRegionReply {
                    region_id,
                    ready: true,
                    exists: true,
                    error: None,
                }
            }
            Err(e) => {
                error!(e; "Failed to sync region: {}", region_id);
                SyncRegionReply {
                    region_id,
                    ready: false,
                    exists: true,
                    error: Some(format!("{:?}", e)),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_meta::kv_backend::memory::MemoryKvBackend;
    use store_api::metric_engine_consts::METRIC_ENGINE_NAME;
    use store_api::region_engine::{RegionRole, SyncRegionFromRequest};
    use store_api::storage::RegionId;

    use crate::heartbeat::handler::sync_region::SyncRegionHandler;
    use crate::heartbeat::handler::{HandlerContext, InstructionHandler};
    use crate::tests::{MockRegionEngine, mock_region_server};

    #[tokio::test]
    async fn test_handle_sync_region_not_found() {
        let mut mock_region_server = mock_region_server();
        let (mock_engine, _) = MockRegionEngine::new(METRIC_ENGINE_NAME);
        mock_region_server.register_engine(mock_engine);

        let kv_backend = Arc::new(MemoryKvBackend::new());
        let handler_context = HandlerContext::new_for_test(mock_region_server, kv_backend);
        let handler = SyncRegionHandler;

        let region_id = RegionId::new(1024, 1);
        let sync_region = common_meta::instruction::SyncRegion {
            region_id,
            request: SyncRegionFromRequest::from_manifest(Default::default()),
        };

        let reply = handler
            .handle(&handler_context, vec![sync_region])
            .await
            .unwrap()
            .expect_sync_regions_reply();

        assert_eq!(reply.len(), 1);
        assert_eq!(reply[0].region_id, region_id);
        assert!(!reply[0].exists);
        assert!(!reply[0].ready);
    }

    #[tokio::test]
    async fn test_handle_sync_region_not_writable() {
        let mock_region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);
        let (mock_engine, _) = MockRegionEngine::with_custom_apply_fn(METRIC_ENGINE_NAME, |r| {
            r.mock_role = Some(Some(RegionRole::Follower));
        });
        mock_region_server.register_test_region(region_id, mock_engine);

        let kv_backend = Arc::new(MemoryKvBackend::new());
        let handler_context = HandlerContext::new_for_test(mock_region_server, kv_backend);
        let handler = SyncRegionHandler;

        let sync_region = common_meta::instruction::SyncRegion {
            region_id,
            request: SyncRegionFromRequest::from_manifest(Default::default()),
        };

        let reply = handler
            .handle(&handler_context, vec![sync_region])
            .await
            .unwrap()
            .expect_sync_regions_reply();

        assert_eq!(reply.len(), 1);
        assert_eq!(reply[0].region_id, region_id);
        assert!(reply[0].exists);
        assert!(!reply[0].ready);
        assert!(reply[0].error.is_some());
    }

    #[tokio::test]
    async fn test_handle_sync_region_success() {
        let mock_region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);
        let (mock_engine, _) = MockRegionEngine::with_custom_apply_fn(METRIC_ENGINE_NAME, |r| {
            r.mock_role = Some(Some(RegionRole::Leader));
        });
        mock_region_server.register_test_region(region_id, mock_engine);

        let kv_backend = Arc::new(MemoryKvBackend::new());
        let handler_context = HandlerContext::new_for_test(mock_region_server, kv_backend);
        let handler = SyncRegionHandler;

        let sync_region = common_meta::instruction::SyncRegion {
            region_id,
            request: SyncRegionFromRequest::from_manifest(Default::default()),
        };

        let reply = handler
            .handle(&handler_context, vec![sync_region])
            .await
            .unwrap()
            .expect_sync_regions_reply();

        assert_eq!(reply.len(), 1);
        assert_eq!(reply[0].region_id, region_id);
        assert!(reply[0].exists);
        assert!(reply[0].ready);
        assert!(reply[0].error.is_none());
    }
}
