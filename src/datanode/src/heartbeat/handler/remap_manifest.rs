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

use common_meta::instruction::{InstructionReply, RemapManifest, RemapManifestReply};
use common_telemetry::{error, info, warn};
use store_api::region_engine::RemapManifestsRequest;

use crate::heartbeat::handler::{HandlerContext, InstructionHandler};

pub struct RemapManifestHandler;

#[async_trait::async_trait]
impl InstructionHandler for RemapManifestHandler {
    type Instruction = RemapManifest;
    async fn handle(
        &self,
        ctx: &HandlerContext,
        request: Self::Instruction,
    ) -> Option<InstructionReply> {
        let RemapManifest {
            region_id,
            input_regions,
            region_mapping,
            new_partition_exprs,
        } = request;
        info!(
            "Datanode received remap manifest request, region_id: {}, input_regions: {}, target_regions: {}",
            region_id,
            input_regions.len(),
            new_partition_exprs.len()
        );
        let Some(leader) = ctx.region_server.is_region_leader(region_id) else {
            warn!("Region: {} is not found", region_id);
            return Some(InstructionReply::RemapManifest(RemapManifestReply {
                exists: false,
                manifest_paths: Default::default(),
                error: None,
            }));
        };

        if !leader {
            warn!("Region: {} is not leader", region_id);
            return Some(InstructionReply::RemapManifest(RemapManifestReply {
                exists: true,
                manifest_paths: Default::default(),
                error: Some("Region is not leader".into()),
            }));
        }

        let reply = match ctx
            .region_server
            .remap_manifests(RemapManifestsRequest {
                region_id,
                input_regions,
                region_mapping,
                new_partition_exprs,
            })
            .await
        {
            Ok(result) => InstructionReply::RemapManifest(RemapManifestReply {
                exists: true,
                manifest_paths: result.manifest_paths,
                error: None,
            }),
            Err(e) => {
                error!(
                    e;
                    "Remap manifests failed on datanode, region_id: {}",
                    region_id
                );
                InstructionReply::RemapManifest(RemapManifestReply {
                    exists: true,
                    manifest_paths: Default::default(),
                    error: Some(format!("{e:?}")),
                })
            }
        };

        Some(reply)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use common_meta::instruction::RemapManifest;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use datatypes::value::Value;
    use mito2::config::MitoConfig;
    use mito2::engine::MITO_ENGINE_NAME;
    use mito2::test_util::{CreateRequestBuilder, TestEnv};
    use partition::expr::{PartitionExpr, col};
    use store_api::path_utils::table_dir;
    use store_api::region_engine::RegionRole;
    use store_api::region_request::{
        EnterStagingRequest, RegionRequest, StagingPartitionDirective,
    };
    use store_api::storage::RegionId;

    use crate::heartbeat::handler::remap_manifest::RemapManifestHandler;
    use crate::heartbeat::handler::{HandlerContext, InstructionHandler};
    use crate::region_server::RegionServer;
    use crate::tests::{MockRegionEngine, mock_region_server};

    #[tokio::test]
    async fn test_region_not_exist() {
        let mut mock_region_server = mock_region_server();
        let (mock_engine, _) = MockRegionEngine::new(MITO_ENGINE_NAME);
        mock_region_server.register_engine(mock_engine);
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let handler_context = HandlerContext::new_for_test(mock_region_server, kv_backend);
        let region_id = RegionId::new(1024, 1);
        let reply = RemapManifestHandler
            .handle(
                &handler_context,
                RemapManifest {
                    region_id,
                    input_regions: vec![],
                    region_mapping: HashMap::new(),
                    new_partition_exprs: HashMap::new(),
                },
            )
            .await
            .unwrap();
        let reply = &reply.expect_remap_manifest_reply();
        assert!(!reply.exists);
        assert!(reply.error.is_none());
        assert!(reply.manifest_paths.is_empty());
    }

    #[tokio::test]
    async fn test_region_not_leader() {
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
        let reply = RemapManifestHandler
            .handle(
                &handler_context,
                RemapManifest {
                    region_id,
                    input_regions: vec![],
                    region_mapping: HashMap::new(),
                    new_partition_exprs: HashMap::new(),
                },
            )
            .await
            .unwrap();
        let reply = reply.expect_remap_manifest_reply();
        assert!(reply.exists);
        assert!(reply.error.is_some());
    }

    fn range_expr(col_name: &str, start: i64, end: i64) -> PartitionExpr {
        col(col_name)
            .gt_eq(Value::Int64(start))
            .and(col(col_name).lt(Value::Int64(end)))
    }

    async fn prepare_region(region_server: &RegionServer) {
        let region_specs = [
            (RegionId::new(1024, 1), range_expr("x", 0, 50)),
            (RegionId::new(1024, 2), range_expr("x", 50, 100)),
        ];

        for (region_id, partition_expr) in region_specs {
            let builder = CreateRequestBuilder::new();
            let mut create_req = builder.build();
            create_req.table_dir = table_dir("test", 1024);
            region_server
                .handle_request(region_id, RegionRequest::Create(create_req))
                .await
                .unwrap();
            region_server
                .handle_request(
                    region_id,
                    RegionRequest::EnterStaging(EnterStagingRequest {
                        partition_directive: StagingPartitionDirective::UpdatePartitionExpr(
                            partition_expr.as_json_str().unwrap(),
                        ),
                    }),
                )
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn test_remap_manifest() {
        common_telemetry::init_default_ut_logging();
        let mut region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);
        let mut engine_env = TestEnv::new().await;
        let engine = engine_env.create_engine(MitoConfig::default()).await;
        region_server.register_engine(Arc::new(engine.clone()));
        prepare_region(&region_server).await;

        let kv_backend = Arc::new(MemoryKvBackend::new());
        let handler_context = HandlerContext::new_for_test(region_server, kv_backend);
        let region_id2 = RegionId::new(1024, 2);
        let reply = RemapManifestHandler
            .handle(
                &handler_context,
                RemapManifest {
                    region_id,
                    input_regions: vec![region_id, region_id2],
                    region_mapping: HashMap::from([
                        (region_id, vec![region_id]),
                        (region_id2, vec![region_id]),
                    ]),
                    new_partition_exprs: HashMap::from([(
                        region_id,
                        range_expr("x", 0, 100).as_json_str().unwrap(),
                    )]),
                },
            )
            .await
            .unwrap();
        let reply = reply.expect_remap_manifest_reply();
        assert!(reply.exists);
        assert!(reply.error.is_none(), "{}", reply.error.unwrap());
        assert_eq!(reply.manifest_paths.len(), 1);

        // Remap failed
        let reply = RemapManifestHandler
            .handle(
                &handler_context,
                RemapManifest {
                    region_id,
                    input_regions: vec![region_id],
                    region_mapping: HashMap::from([
                        (region_id, vec![region_id]),
                        (region_id2, vec![region_id]),
                    ]),
                    new_partition_exprs: HashMap::from([(
                        region_id,
                        range_expr("x", 0, 100).as_json_str().unwrap(),
                    )]),
                },
            )
            .await
            .unwrap();
        let reply = reply.expect_remap_manifest_reply();
        assert!(reply.exists);
        assert!(reply.error.is_some());
        assert!(reply.manifest_paths.is_empty());
    }
}
