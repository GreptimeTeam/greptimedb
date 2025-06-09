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

use common_meta::instruction::{FlushRegions, InstructionReply, SimpleReply};
use common_telemetry::warn;
use futures_util::future::BoxFuture;
use store_api::region_request::{RegionFlushRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::error;
use crate::heartbeat::handler::HandlerContext;

impl HandlerContext {
    pub(crate) fn handle_flush_regions_instruction(
        self,
        flush_regions: FlushRegions,
    ) -> BoxFuture<'static, Option<InstructionReply>> {
        Box::pin(async move {
            for region_id in flush_regions.region_ids {
                let request = RegionRequest::Flush(RegionFlushRequest {
                    row_group_size: None,
                });
                let result = self.region_server.handle_request(region_id, request).await;

                match result {
                    Ok(_) => {}
                    Err(error::Error::RegionNotFound { .. }) => {
                        warn!("Received a flush region instruction from meta, but target region: {region_id} is not found.");
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
            None
        })
    }

    pub(crate) fn handle_flush_region_instruction(
        self,
        region_id: RegionId,
    ) -> BoxFuture<'static, Option<InstructionReply>> {
        Box::pin(async move {
            let Some(writable) = self.region_server.is_region_leader(region_id) else {
                return Some(InstructionReply::FlushRegion(SimpleReply {
                    result: false,
                    error: Some("Region is not leader".to_string()),
                }));
            };

            if !writable {
                return Some(InstructionReply::FlushRegion(SimpleReply {
                    result: false,
                    error: Some("Region is not writable".to_string()),
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
            }
            let mut watcher = register_result.into_watcher();
            let result = self.flush_tasks.wait_until_finish(&mut watcher).await;
            match result {
                Ok(()) => Some(InstructionReply::FlushRegion(SimpleReply {
                    result: true,
                    error: None,
                })),
                Err(err) => Some(InstructionReply::FlushRegion(SimpleReply {
                    result: false,
                    error: Some(format!("{err:?}")),
                })),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use common_meta::instruction::FlushRegions;
    use mito2::engine::MITO_ENGINE_NAME;
    use store_api::storage::RegionId;

    use super::*;
    use crate::tests::{mock_region_server, MockRegionEngine};

    #[tokio::test]
    async fn test_handle_flush_region_instruction() {
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

        let reply = handler_context
            .clone()
            .handle_flush_regions_instruction(FlushRegions {
                region_ids: region_ids.clone(),
            })
            .await;
        assert!(reply.is_none());
        assert_eq!(*flushed_region_ids.read().unwrap(), region_ids);

        flushed_region_ids.write().unwrap().clear();
        let not_found_region_ids = (0..2).map(|i| RegionId::new(2048, i)).collect::<Vec<_>>();
        let reply = handler_context
            .handle_flush_regions_instruction(FlushRegions {
                region_ids: not_found_region_ids.clone(),
            })
            .await;
        assert!(reply.is_none());
        assert!(flushed_region_ids.read().unwrap().is_empty());
    }
}
