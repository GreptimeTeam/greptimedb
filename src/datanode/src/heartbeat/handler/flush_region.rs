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

use std::collections::HashMap;

use common_meta::instruction::{
    FlushErrorStrategy, FlushRegionConfig, FlushRegionReply, FlushResult, FlushTarget,
    InstructionReply,
};
use common_telemetry::warn;
use futures_util::future::BoxFuture;
use store_api::region_request::{RegionFlushRequest, RegionRequest};

use crate::error;
use crate::heartbeat::handler::HandlerContext;

impl HandlerContext {
    pub(crate) fn handle_flush_region_instruction(
        self,
        config: FlushRegionConfig,
    ) -> BoxFuture<'static, Option<InstructionReply>> {
        Box::pin(async move {
            let region_ids = match config.target {
                FlushTarget::All => self
                    .region_server
                    .reportable_regions()
                    .into_iter()
                    .map(|stat| stat.region_id)
                    .collect(),
                FlushTarget::Regions(ids) => ids,
                FlushTarget::Table(table_id) => self
                    .region_server
                    .reportable_regions()
                    .into_iter()
                    .filter(|stat| stat.region_id.table_id() == table_id)
                    .map(|stat| stat.region_id)
                    .collect(),
            };
            let mut results = HashMap::new();
            let mut overall_success = true;
            if config.is_hint {
                for region_id in region_ids {
                    let request = RegionRequest::Flush(RegionFlushRequest {
                        row_group_size: None,
                    });
                    let result = self.region_server.handle_request(region_id, request).await;
                    match result {
                        Ok(_) => {
                            results.insert(
                                region_id,
                                FlushResult {
                                    success: true,
                                    error: None,
                                    skipped: false,
                                },
                            );
                        }
                        Err(err) => {
                            if let error::Error::RegionNotFound { .. } = err {
                                warn!("Received a flush region instruction from meta, but target region:{region_id} is not found.");
                            }
                            results.insert(
                                region_id,
                                FlushResult {
                                    success: false,
                                    error: Some(err.to_string()),
                                    skipped: false,
                                },
                            );
                            if config.error_strategy == FlushErrorStrategy::FailFast {
                                overall_success = false;
                                break;
                            }
                            if config.error_strategy == FlushErrorStrategy::TryAllStrict {
                                overall_success = false;
                            }
                        }
                    }
                }
            } else {
                let mut watcher_tasks = vec![];
                for region_id in region_ids {
                    let region_server = self.region_server.clone();
                    let request = RegionRequest::Flush(RegionFlushRequest {
                        row_group_size: None,
                    });
                    let fut = async move {
                        region_server
                            .handle_request(region_id, request)
                            .await
                            .map(|_| ())
                    };
                    let register_result = self
                        .flush_tasks
                        .try_register(region_id, Box::pin(fut))
                        .await;
                    if register_result.is_busy() {
                        warn!("Another flush task is running for the region: {region_id}");
                    }
                    let watcher = register_result.into_watcher();
                    watcher_tasks.push((region_id, watcher));
                }

                for (region_id, mut watcher) in watcher_tasks {
                    let result = self.flush_tasks.wait_until_finish(&mut watcher).await;
                    match result {
                        Ok(_) => {
                            results.insert(
                                region_id,
                                FlushResult {
                                    success: true,
                                    error: None,
                                    skipped: false,
                                },
                            );
                        }
                        Err(err) => {
                            if let error::Error::RegionNotFound { .. } = err {
                                warn!("Received a flush region instruction from meta, but target region:{region_id} is not found.");
                            }
                            results.insert(
                                region_id,
                                FlushResult {
                                    success: false,
                                    error: Some(err.to_string()),
                                    skipped: false,
                                },
                            );
                            if config.error_strategy == FlushErrorStrategy::FailFast {
                                overall_success = false;
                                break;
                            }
                            if config.error_strategy == FlushErrorStrategy::TryAllStrict {
                                overall_success = false;
                            }
                        }
                    }
                }
            }
            Some(InstructionReply::FlushRegion(FlushRegionReply {
                success: overall_success,
                results,
                error_strategy: config.error_strategy,
            }))
        })
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
            .handle_flush_region_instruction(FlushRegionConfig {
                target: FlushTarget::Regions(region_ids.clone()),
                error_strategy: FlushErrorStrategy::FailFast,
                is_hint: false,
                timeout: None,
            })
            .await;
        assert!(reply.is_some());
        let mut actual = flushed_region_ids.read().unwrap().clone();
        let mut expected = region_ids.clone();
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);

        flushed_region_ids.write().unwrap().clear();
        let not_found_region_ids = (0..2).map(|i| RegionId::new(2048, i)).collect::<Vec<_>>();
        let reply = handler_context
            .handle_flush_region_instruction(FlushRegionConfig {
                target: FlushTarget::Regions(not_found_region_ids.clone()),
                error_strategy: FlushErrorStrategy::FailFast,
                is_hint: false,
                timeout: None,
            })
            .await;
        assert!(reply.is_some());
        assert!(flushed_region_ids.read().unwrap().is_empty());
    }
}
