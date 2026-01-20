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
    ApplyStagingManifest, ApplyStagingManifestReply, ApplyStagingManifestsReply, InstructionReply,
};
use common_telemetry::{error, warn};
use futures::future::join_all;
use store_api::region_request::{ApplyStagingManifestRequest, RegionRequest};

use crate::heartbeat::handler::{HandlerContext, InstructionHandler};

pub struct ApplyStagingManifestsHandler;

#[async_trait::async_trait]
impl InstructionHandler for ApplyStagingManifestsHandler {
    type Instruction = Vec<ApplyStagingManifest>;
    async fn handle(
        &self,
        ctx: &HandlerContext,
        requests: Self::Instruction,
    ) -> Option<InstructionReply> {
        let results = join_all(
            requests
                .into_iter()
                .map(|request| Self::handle_apply_staging_manifest(ctx, request)),
        )
        .await;
        Some(InstructionReply::ApplyStagingManifests(
            ApplyStagingManifestsReply::new(results),
        ))
    }
}

impl ApplyStagingManifestsHandler {
    async fn handle_apply_staging_manifest(
        ctx: &HandlerContext,
        request: ApplyStagingManifest,
    ) -> ApplyStagingManifestReply {
        let ApplyStagingManifest {
            region_id,
            ref partition_expr,
            central_region_id,
            ref manifest_path,
        } = request;
        common_telemetry::info!(
            "Datanode received apply staging manifest request, region_id: {}, central_region_id: {}, partition_expr: {}, manifest_path: {}",
            region_id,
            central_region_id,
            partition_expr,
            manifest_path
        );
        let Some(leader) = ctx.region_server.is_region_leader(region_id) else {
            warn!("Region: {} is not found", region_id);
            return ApplyStagingManifestReply {
                region_id,
                exists: false,
                ready: false,
                error: None,
            };
        };
        if !leader {
            warn!("Region: {} is not leader", region_id);
            return ApplyStagingManifestReply {
                region_id,
                exists: true,
                ready: false,
                error: Some("Region is not leader".into()),
            };
        }

        match ctx
            .region_server
            .handle_request(
                region_id,
                RegionRequest::ApplyStagingManifest(ApplyStagingManifestRequest {
                    partition_expr: partition_expr.clone(),
                    central_region_id,
                    manifest_path: manifest_path.clone(),
                }),
            )
            .await
        {
            Ok(_) => ApplyStagingManifestReply {
                region_id,
                exists: true,
                ready: true,
                error: None,
            },
            Err(err) => {
                error!(err; "Failed to apply staging manifest, region_id: {}", region_id);
                ApplyStagingManifestReply {
                    region_id,
                    exists: true,
                    ready: false,
                    error: Some(format!("{err:?}")),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use common_meta::instruction::RemapManifest;
    use datatypes::value::Value;
    use mito2::config::MitoConfig;
    use mito2::engine::MITO_ENGINE_NAME;
    use mito2::test_util::{CreateRequestBuilder, TestEnv};
    use partition::expr::{PartitionExpr, col};
    use store_api::path_utils::table_dir;
    use store_api::region_engine::RegionRole;
    use store_api::region_request::EnterStagingRequest;
    use store_api::storage::RegionId;

    use super::*;
    use crate::heartbeat::handler::remap_manifest::RemapManifestHandler;
    use crate::region_server::RegionServer;
    use crate::tests::{MockRegionEngine, mock_region_server};

    #[tokio::test]
    async fn test_region_not_exist() {
        let mut mock_region_server = mock_region_server();
        let (mock_engine, _) = MockRegionEngine::new(MITO_ENGINE_NAME);
        mock_region_server.register_engine(mock_engine);
        let handler_context = HandlerContext::new_for_test(mock_region_server);
        let region_id = RegionId::new(1024, 1);
        let reply = ApplyStagingManifestsHandler
            .handle(
                &handler_context,
                vec![ApplyStagingManifest {
                    region_id,
                    partition_expr: "".to_string(),
                    central_region_id: RegionId::new(1024, 9999), // use a dummy value
                    manifest_path: "".to_string(),
                }],
            )
            .await
            .unwrap();
        let replies = reply.expect_apply_staging_manifests_reply();
        let reply = &replies[0];
        assert!(!reply.exists);
        assert!(!reply.ready);
        assert!(reply.error.is_none());
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
        let handler_context = HandlerContext::new_for_test(mock_region_server);
        let region_id = RegionId::new(1024, 1);
        let reply = ApplyStagingManifestsHandler
            .handle(
                &handler_context,
                vec![ApplyStagingManifest {
                    region_id,
                    partition_expr: "".to_string(),
                    central_region_id: RegionId::new(1024, 2),
                    manifest_path: "".to_string(),
                }],
            )
            .await
            .unwrap();
        let replies = reply.expect_apply_staging_manifests_reply();
        let reply = &replies[0];
        assert!(reply.exists);
        assert!(!reply.ready);
        assert!(reply.error.is_some());
    }

    fn range_expr(col_name: &str, start: i64, end: i64) -> PartitionExpr {
        col(col_name)
            .gt_eq(Value::Int64(start))
            .and(col(col_name).lt(Value::Int64(end)))
    }

    async fn prepare_region(region_server: &RegionServer) {
        let region_specs = [
            (RegionId::new(1024, 1), range_expr("x", 0, 49)),
            (RegionId::new(1024, 2), range_expr("x", 49, 100)),
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
                        partition_expr: partition_expr.as_json_str().unwrap(),
                    }),
                )
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn test_apply_staging_manifest() {
        common_telemetry::init_default_ut_logging();
        let mut region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);
        let mut engine_env = TestEnv::new().await;
        let engine = engine_env.create_engine(MitoConfig::default()).await;
        region_server.register_engine(Arc::new(engine.clone()));
        prepare_region(&region_server).await;

        let handler_context = HandlerContext::new_for_test(region_server);
        let region_id2 = RegionId::new(1024, 2);
        let reply = RemapManifestHandler
            .handle(
                &handler_context,
                RemapManifest {
                    region_id,
                    input_regions: vec![region_id, region_id2],
                    region_mapping: HashMap::from([
                        // [0,49) <- [0, 50)
                        (region_id, vec![region_id]),
                        // [49, 100) <- [0, 50), [50,100)
                        (region_id2, vec![region_id, region_id2]),
                    ]),
                    new_partition_exprs: HashMap::from([
                        (region_id, range_expr("x", 0, 49).as_json_str().unwrap()),
                        (region_id2, range_expr("x", 49, 100).as_json_str().unwrap()),
                    ]),
                },
            )
            .await
            .unwrap();
        let reply = reply.expect_remap_manifest_reply();
        assert!(reply.exists);
        assert!(reply.error.is_none(), "{}", reply.error.unwrap());
        assert_eq!(reply.manifest_paths.len(), 2);
        let manifest_path_1 = reply.manifest_paths[&region_id].clone();
        let manifest_path_2 = reply.manifest_paths[&region_id2].clone();

        let reply = ApplyStagingManifestsHandler
            .handle(
                &handler_context,
                vec![ApplyStagingManifest {
                    region_id,
                    partition_expr: range_expr("x", 0, 49).as_json_str().unwrap(),
                    central_region_id: region_id,
                    manifest_path: manifest_path_1,
                }],
            )
            .await
            .unwrap();
        let replies = reply.expect_apply_staging_manifests_reply();
        let reply = &replies[0];
        assert!(reply.exists);
        assert!(reply.ready);
        assert!(reply.error.is_none());

        // partition expr mismatch
        let reply = ApplyStagingManifestsHandler
            .handle(
                &handler_context,
                vec![ApplyStagingManifest {
                    region_id: region_id2,
                    partition_expr: range_expr("x", 50, 100).as_json_str().unwrap(),
                    central_region_id: region_id,
                    manifest_path: manifest_path_2,
                }],
            )
            .await
            .unwrap();
        let replies = reply.expect_apply_staging_manifests_reply();
        let reply = &replies[0];
        assert!(reply.exists);
        assert!(!reply.ready);
        assert!(reply.error.is_some());
    }
}
