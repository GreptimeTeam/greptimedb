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

use std::time::Instant;

use common_meta::instruction::{
    FlushErrorStrategy, FlushRegionReply, FlushRegions, FlushStrategy, InstructionReply,
};
use common_telemetry::{debug, warn};
use store_api::region_request::{RegionFlushRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::error::{self, RegionNotFoundSnafu, RegionNotReadySnafu, Result, UnexpectedSnafu};
use crate::heartbeat::handler::{HandlerContext, InstructionHandler};

pub struct FlushRegionsHandler;

#[async_trait::async_trait]
impl InstructionHandler for FlushRegionsHandler {
    type Instruction = FlushRegions;

    async fn handle(
        &self,
        ctx: &HandlerContext,
        flush_regions: FlushRegions,
    ) -> Option<InstructionReply> {
        let start_time = Instant::now();
        let strategy = flush_regions.strategy;
        let region_ids = flush_regions.region_ids;
        let error_strategy = flush_regions.error_strategy;

        let reply = if matches!(strategy, FlushStrategy::Async) {
            // Asynchronous hint mode: fire-and-forget, no reply expected
            ctx.handle_flush_hint(region_ids).await;
            None
        } else {
            // Synchronous mode: return reply with results
            let reply = ctx.handle_flush_sync(region_ids, error_strategy).await;
            Some(InstructionReply::FlushRegions(reply))
        };

        let elapsed = start_time.elapsed();
        debug!(
            "FlushRegions strategy: {:?}, elapsed: {:?}, reply: {:?}",
            strategy, elapsed, reply
        );

        reply
    }
}

impl HandlerContext {
    /// Performs the actual region flush operation.
    async fn perform_region_flush(&self, region_id: RegionId) -> Result<()> {
        let request = RegionRequest::Flush(RegionFlushRequest {
            row_group_size: None,
        });
        self.region_server
            .handle_request(region_id, request)
            .await?;
        Ok(())
    }

    /// Handles asynchronous flush hints (fire-and-forget).
    async fn handle_flush_hint(&self, region_ids: Vec<RegionId>) {
        let start_time = Instant::now();
        for region_id in &region_ids {
            let result = self.perform_region_flush(*region_id).await;
            match result {
                Ok(_) => {}
                Err(error::Error::RegionNotFound { .. }) => {
                    warn!(
                        "Received a flush region hint from meta, but target region: {} is not found.",
                        region_id
                    );
                }
                Err(err) => {
                    warn!("Failed to flush region: {}, error: {}", region_id, err);
                }
            }
        }
        let elapsed = start_time.elapsed();
        debug!(
            "Flush regions hint: {:?}, elapsed: {:?}",
            region_ids, elapsed
        );
    }

    /// Handles synchronous flush operations with proper error handling and replies.
    async fn handle_flush_sync(
        &self,
        region_ids: Vec<RegionId>,
        error_strategy: FlushErrorStrategy,
    ) -> FlushRegionReply {
        let mut results = Vec::with_capacity(region_ids.len());

        for region_id in region_ids {
            let result = self.flush_single_region_sync(region_id).await;

            match &result {
                Ok(_) => results.push((region_id, Ok(()))),
                Err(err) => {
                    // Convert error::Error to String for FlushRegionReply compatibility
                    let error_string = err.to_string();
                    results.push((region_id, Err(error_string)));

                    // For fail-fast strategy, abort on first error
                    if matches!(error_strategy, FlushErrorStrategy::FailFast) {
                        break;
                    }
                }
            }
        }

        FlushRegionReply::from_results(results)
    }

    /// Flushes a single region synchronously with proper error handling.
    async fn flush_single_region_sync(&self, region_id: RegionId) -> Result<()> {
        // Check if region is leader and writable
        let Some(writable) = self.region_server.is_region_leader(region_id) else {
            return Err(RegionNotFoundSnafu { region_id }.build());
        };

        if !writable {
            return Err(RegionNotReadySnafu { region_id }.build());
        }

        // Register and execute the flush task
        let region_server_moved = self.region_server.clone();
        let register_result = self
            .flush_tasks
            .try_register(
                region_id,
                Box::pin(async move {
                    region_server_moved
                        .handle_request(
                            region_id,
                            RegionRequest::Flush(RegionFlushRequest {
                                row_group_size: None,
                            }),
                        )
                        .await?;
                    Ok(())
                }),
            )
            .await;

        if register_result.is_busy() {
            warn!("Another flush task is running for the region: {region_id}");
        }

        let mut watcher = register_result.into_watcher();
        match self.flush_tasks.wait_until_finish(&mut watcher).await {
            Ok(()) => Ok(()),
            Err(err) => Err(UnexpectedSnafu {
                violated: format!("Flush task failed: {err:?}"),
            }
            .build()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use common_meta::instruction::{FlushErrorStrategy, FlushRegions};
    use mito2::engine::MITO_ENGINE_NAME;
    use store_api::storage::RegionId;

    use super::*;
    use crate::tests::{MockRegionEngine, mock_region_server};

    #[tokio::test]
    async fn test_handle_flush_region_hint() {
        let flushed_region_ids: Arc<RwLock<Vec<RegionId>>> = Arc::new(RwLock::new(Vec::new()));

        let mock_region_server = mock_region_server();
        let region_ids = (0..16).map(|i| RegionId::new(1024, i)).collect::<Vec<_>>();
        for region_id in &region_ids {
            let flushed_region_ids_ref = flushed_region_ids.clone();
            let (mock_engine, _) =
                MockRegionEngine::with_custom_apply_fn(MITO_ENGINE_NAME, move |region_engine| {
                    region_engine.handle_request_mock_fn =
                        Some(Box::new(move |region_id, _request| {
                            flushed_region_ids_ref.write().unwrap().push(region_id);
                            Ok(0)
                        }))
                });
            mock_region_server.register_test_region(*region_id, mock_engine);
        }
        let kv_backend = Arc::new(common_meta::kv_backend::memory::MemoryKvBackend::new());
        let handler_context = HandlerContext::new_for_test(mock_region_server, kv_backend);

        // Async hint mode
        let flush_instruction = FlushRegions::async_batch(region_ids.clone());
        let reply = FlushRegionsHandler
            .handle(&handler_context, flush_instruction)
            .await;
        assert!(reply.is_none()); // Hint mode returns no reply
        assert_eq!(*flushed_region_ids.read().unwrap(), region_ids);

        // Non-existent regions
        flushed_region_ids.write().unwrap().clear();
        let not_found_region_ids = (0..2).map(|i| RegionId::new(2048, i)).collect::<Vec<_>>();
        let flush_instruction = FlushRegions::async_batch(not_found_region_ids);
        let reply = FlushRegionsHandler
            .handle(&handler_context, flush_instruction)
            .await;
        assert!(reply.is_none());
        assert!(flushed_region_ids.read().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_handle_flush_region_sync_single() {
        let flushed_region_ids: Arc<RwLock<Vec<RegionId>>> = Arc::new(RwLock::new(Vec::new()));

        let mock_region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);

        let flushed_region_ids_ref = flushed_region_ids.clone();
        let (mock_engine, _) =
            MockRegionEngine::with_custom_apply_fn(MITO_ENGINE_NAME, move |region_engine| {
                region_engine.handle_request_mock_fn = Some(Box::new(move |region_id, _request| {
                    flushed_region_ids_ref.write().unwrap().push(region_id);
                    Ok(0)
                }))
            });
        mock_region_server.register_test_region(region_id, mock_engine);
        let kv_backend = Arc::new(common_meta::kv_backend::memory::MemoryKvBackend::new());
        let handler_context = HandlerContext::new_for_test(mock_region_server, kv_backend);

        let flush_instruction = FlushRegions::sync_single(region_id);
        let reply = FlushRegionsHandler
            .handle(&handler_context, flush_instruction)
            .await;
        let flush_reply = reply.unwrap().expect_flush_regions_reply();
        assert!(flush_reply.overall_success);
        assert_eq!(flush_reply.results.len(), 1);
        assert_eq!(flush_reply.results[0].0, region_id);
        assert!(flush_reply.results[0].1.is_ok());
        assert_eq!(*flushed_region_ids.read().unwrap(), vec![region_id]);
    }

    #[tokio::test]
    async fn test_handle_flush_region_sync_batch_fail_fast() {
        let flushed_region_ids: Arc<RwLock<Vec<RegionId>>> = Arc::new(RwLock::new(Vec::new()));

        let mock_region_server = mock_region_server();
        let region_ids = vec![
            RegionId::new(1024, 1),
            RegionId::new(1024, 2),
            RegionId::new(1024, 3),
        ];

        // Register only the first region, others will fail
        let flushed_region_ids_ref = flushed_region_ids.clone();
        let (mock_engine, _) =
            MockRegionEngine::with_custom_apply_fn(MITO_ENGINE_NAME, move |region_engine| {
                region_engine.handle_request_mock_fn = Some(Box::new(move |region_id, _request| {
                    flushed_region_ids_ref.write().unwrap().push(region_id);
                    Ok(0)
                }))
            });
        mock_region_server.register_test_region(region_ids[0], mock_engine);
        let kv_backend = Arc::new(common_meta::kv_backend::memory::MemoryKvBackend::new());
        let handler_context = HandlerContext::new_for_test(mock_region_server, kv_backend);

        // Sync batch with fail-fast strategy
        let flush_instruction =
            FlushRegions::sync_batch(region_ids.clone(), FlushErrorStrategy::FailFast);
        let reply = FlushRegionsHandler
            .handle(&handler_context, flush_instruction)
            .await;
        let flush_reply = reply.unwrap().expect_flush_regions_reply();
        assert!(!flush_reply.overall_success); // Should fail due to non-existent regions
        // With fail-fast, only process regions until first failure
        assert!(flush_reply.results.len() <= region_ids.len());
    }

    #[tokio::test]
    async fn test_handle_flush_region_sync_batch_try_all() {
        let flushed_region_ids: Arc<RwLock<Vec<RegionId>>> = Arc::new(RwLock::new(Vec::new()));

        let mock_region_server = mock_region_server();
        let region_ids = vec![RegionId::new(1024, 1), RegionId::new(1024, 2)];

        // Register only the first region
        let flushed_region_ids_ref = flushed_region_ids.clone();
        let (mock_engine, _) =
            MockRegionEngine::with_custom_apply_fn(MITO_ENGINE_NAME, move |region_engine| {
                region_engine.handle_request_mock_fn = Some(Box::new(move |region_id, _request| {
                    flushed_region_ids_ref.write().unwrap().push(region_id);
                    Ok(0)
                }))
            });
        mock_region_server.register_test_region(region_ids[0], mock_engine);
        let kv_backend = Arc::new(common_meta::kv_backend::memory::MemoryKvBackend::new());
        let handler_context = HandlerContext::new_for_test(mock_region_server, kv_backend);

        // Sync batch with try-all strategy
        let flush_instruction =
            FlushRegions::sync_batch(region_ids.clone(), FlushErrorStrategy::TryAll);
        let reply = FlushRegionsHandler
            .handle(&handler_context, flush_instruction)
            .await;
        let flush_reply = reply.unwrap().expect_flush_regions_reply();
        assert!(!flush_reply.overall_success); // Should fail due to one non-existent region
        // With try-all, should process all regions
        assert_eq!(flush_reply.results.len(), region_ids.len());
        // First should succeed, second should fail
        assert!(flush_reply.results[0].1.is_ok());
        assert!(flush_reply.results[1].1.is_err());
    }

    #[test]
    fn test_flush_regions_display() {
        let region_id = RegionId::new(1024, 1);
        let flush_regions = FlushRegions::sync_single(region_id);
        let display = format!("{}", flush_regions);
        assert_eq!(
            display,
            "FlushRegions(region_ids=[4398046511105(1024, 1)], strategy=Sync, error_strategy=FailFast)"
        );
    }
}
