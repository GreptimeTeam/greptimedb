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

use common_meta::instruction::{FlushRegion, InstructionReply, SimpleReply};
use common_telemetry::warn;
use futures_util::future::BoxFuture;
use store_api::region_request::{RegionFlushRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::error;
use crate::heartbeat::handler::HandlerContext;

impl HandlerContext {
    pub(crate) fn handle_flush_region_instruction(
        self,
        flush_region: FlushRegion,
    ) -> BoxFuture<'static, Option<InstructionReply>> {
        Box::pin(async move {
            if flush_region.is_hint {
                // Hint mode: async, best-effort, no reply
                self.handle_flush_hint(flush_region.region_ids).await;
                None
            } else {
                // Enforced mode: sync, mandatory, with reply
                self.handle_flush_enforced(flush_region.region_ids, flush_region.fail_fast)
                    .await
            }
        })
    }

    async fn handle_flush_hint(&self, region_ids: Vec<RegionId>) {
        for region_id in region_ids {
            let request = RegionRequest::Flush(RegionFlushRequest {
                row_group_size: None,
            });
            let result = self.region_server.handle_request(region_id, request).await;

            match result {
                Ok(_) => {}
                Err(error::Error::RegionNotFound { .. }) => {
                    warn!("Received a flush region hint from meta, but target region: {region_id} is not found.");
                }
                Err(err) => {
                    warn!(
                        "Failed to flush region: {region_id}, error: {err}",
                        region_id = region_id,
                        err = err,
                    );
                }
            }
        }
    }

    async fn handle_flush_enforced(
        &self,
        region_ids: Vec<RegionId>,
        fail_fast: bool,
    ) -> Option<InstructionReply> {
        // For enforced mode, we handle multiple regions
        // If fail_fast is true, return on first error (original behavior)
        // If fail_fast is false, try all regions and collect errors

        if fail_fast {
            // Original fail-fast behavior
            for region_id in region_ids {
                if let Some(error_reply) = self.flush_single_region(region_id).await {
                    return Some(error_reply);
                }
            }
            // All regions flushed successfully
            Some(InstructionReply::FlushRegion(SimpleReply {
                result: true,
                error: None,
            }))
        } else {
            // Try-all behavior: attempt to flush all regions and collect errors
            let mut errors = Vec::new();
            let mut success_count = 0;

            for region_id in region_ids.iter() {
                match self.flush_single_region(*region_id).await {
                    Some(InstructionReply::FlushRegion(SimpleReply {
                        result: false,
                        error: Some(err),
                    })) => {
                        errors.push(format!("Region {}: {}", region_id, err));
                    }
                    Some(_) => {
                        // This case shouldn't happen as flush_single_region returns None on success
                        errors.push(format!("Region {}: unexpected reply format", region_id));
                    }
                    None => {
                        // Success case - flush_single_region returns None on success
                        success_count += 1;
                    }
                }
            }

            if errors.is_empty() {
                // All regions flushed successfully
                Some(InstructionReply::FlushRegion(SimpleReply {
                    result: true,
                    error: None,
                }))
            } else {
                // Some regions failed, return aggregated error
                let error_msg = if success_count > 0 {
                    format!(
                        "Partial success: {} regions flushed successfully, {} failed. Errors: [{}]",
                        success_count,
                        errors.len(),
                        errors.join("; ")
                    )
                } else {
                    format!("All regions failed. Errors: [{}]", errors.join("; "))
                };
                Some(InstructionReply::FlushRegion(SimpleReply {
                    result: success_count > 0, // true if at least one region succeeded
                    error: Some(error_msg),
                }))
            }
        }
    }

    async fn flush_single_region(&self, region_id: RegionId) -> Option<InstructionReply> {
        let Some(writable) = self.region_server.is_region_leader(region_id) else {
            return Some(InstructionReply::FlushRegion(SimpleReply {
                result: false,
                error: Some(format!("Region {region_id} is not leader")),
            }));
        };

        if !writable {
            return Some(InstructionReply::FlushRegion(SimpleReply {
                result: false,
                error: Some(format!("Region {region_id} is not writable")),
            }));
        }

        let region_server_moved = self.region_server.clone();
        let register_result = self
            .flush_tasks
            .try_register(
                region_id,
                Box::pin(async move {
                    let request = RegionRequest::Flush(RegionFlushRequest {
                        row_group_size: None,
                    });
                    region_server_moved
                        .handle_request(region_id, request)
                        .await?;
                    Ok(())
                }),
            )
            .await;

        if register_result.is_busy() {
            warn!("Another flush task is running for the region: {region_id}");
            return Some(InstructionReply::FlushRegion(SimpleReply {
                result: false,
                error: Some(format!(
                    "Region {region_id} is busy with another flush task"
                )),
            }));
        }

        let mut watcher = register_result.into_watcher();
        let result = self.flush_tasks.wait_until_finish(&mut watcher).await;

        if let Err(err) = result {
            return Some(InstructionReply::FlushRegion(SimpleReply {
                result: false,
                error: Some(format!("Failed to flush region {region_id}: {err:?}")),
            }));
        }

        // Success case - return None to indicate success for this single region
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use mito2::engine::MITO_ENGINE_NAME;
    use store_api::storage::RegionId;

    use super::*;
    use crate::tests::{mock_region_server, MockRegionEngine};

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
        let handler_context = HandlerContext::new_for_test(mock_region_server);

        // Test hint mode (async, no reply)
        let reply = handler_context
            .clone()
            .handle_flush_region_instruction(FlushRegion {
                region_ids: region_ids.clone(),
                is_hint: true,
                fail_fast: false,
            })
            .await;
        assert!(reply.is_none());
        assert_eq!(*flushed_region_ids.read().unwrap(), region_ids);

        // Test with not found regions
        flushed_region_ids.write().unwrap().clear();
        let not_found_region_ids = (0..2).map(|i| RegionId::new(2048, i)).collect::<Vec<_>>();
        let reply = handler_context
            .handle_flush_region_instruction(FlushRegion {
                region_ids: not_found_region_ids.clone(),
                is_hint: true,
                fail_fast: false,
            })
            .await;
        assert!(reply.is_none());
        assert!(flushed_region_ids.read().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_handle_flush_region_enforced() {
        let flushed_region_ids: Arc<RwLock<Vec<RegionId>>> = Arc::new(RwLock::new(Vec::new()));

        let mock_region_server = mock_region_server();
        let region_ids = (0..3).map(|i| RegionId::new(1024, i)).collect::<Vec<_>>();
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
        let handler_context = HandlerContext::new_for_test(mock_region_server);

        // Test enforced mode (sync, with reply)
        let reply = handler_context
            .handle_flush_region_instruction(FlushRegion {
                region_ids: region_ids.clone(),
                is_hint: false,
                fail_fast: true,
            })
            .await;

        assert!(reply.is_some());
        if let Some(InstructionReply::FlushRegion(reply)) = reply {
            assert!(reply.result);
            assert!(reply.error.is_none());
        }
        assert_eq!(*flushed_region_ids.read().unwrap(), region_ids);
    }

    #[tokio::test]
    async fn test_handle_flush_region_fail_fast() {
        let flushed_region_ids: Arc<RwLock<Vec<RegionId>>> = Arc::new(RwLock::new(Vec::new()));

        let mock_region_server = mock_region_server();
        let region_ids = (0..3).map(|i| RegionId::new(1024, i)).collect::<Vec<_>>();

        // Set up first region to succeed, second to fail, third to succeed
        for (idx, region_id) in region_ids.iter().enumerate() {
            let flushed_region_ids_ref = flushed_region_ids.clone();
            let (mock_engine, _) =
                MockRegionEngine::with_custom_apply_fn(MITO_ENGINE_NAME, move |region_engine| {
                    region_engine.handle_request_mock_fn =
                        Some(Box::new(move |region_id, _request| {
                            if idx == 1 {
                                // Second region fails
                                Err(crate::error::RegionNotFoundSnafu { region_id }.build())
                            } else {
                                // First and third regions succeed
                                flushed_region_ids_ref.write().unwrap().push(region_id);
                                Ok(0)
                            }
                        }))
                });
            mock_region_server.register_test_region(*region_id, mock_engine);
        }
        let handler_context = HandlerContext::new_for_test(mock_region_server);

        // Test fail_fast = true (should stop on first error)
        flushed_region_ids.write().unwrap().clear();
        let reply = handler_context
            .clone()
            .handle_flush_region_instruction(FlushRegion {
                region_ids: region_ids.clone(),
                is_hint: false,
                fail_fast: true,
            })
            .await;

        assert!(reply.is_some());
        if let Some(InstructionReply::FlushRegion(reply)) = reply {
            assert!(!reply.result);
            assert!(reply.error.is_some());
        }
        // Only first region should have been flushed
        assert_eq!(flushed_region_ids.read().unwrap().len(), 1);

        // Test fail_fast = false (should try all regions)
        flushed_region_ids.write().unwrap().clear();
        let reply = handler_context
            .handle_flush_region_instruction(FlushRegion {
                region_ids: region_ids.clone(),
                is_hint: false,
                fail_fast: false,
            })
            .await;

        assert!(reply.is_some());
        if let Some(InstructionReply::FlushRegion(reply)) = reply {
            assert!(reply.result); // Should be true because some regions succeeded
            assert!(reply.error.is_some()); // Should have error message about partial success
            assert!(reply.error.as_ref().unwrap().contains("Partial success"));
        }
        // First and third regions should have been flushed
        assert_eq!(flushed_region_ids.read().unwrap().len(), 2);
    }
}
