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

use api::v1::region::region_request::Body as PbRegionRequest;
use api::v1::region::{ListMetadataRequest, RegionRequest, RegionRequestHeader};
use common_telemetry::tracing_context::TracingContext;
use futures::future::join_all;
use snafu::ResultExt;
use store_api::metadata::RegionMetadata;
use store_api::storage::{RegionId, TableId};

use crate::ddl::utils::add_peer_context_if_needed;
use crate::error::{DecodeJsonSnafu, Result};
use crate::region_rpc::RegionRpcRef;
use crate::rpc::router::{RegionRoute, find_leaders, region_distribution};

/// Collects the region metadata from the datanodes.
pub struct RegionMetadataLister {
    region_rpc: RegionRpcRef,
}

impl RegionMetadataLister {
    /// Creates a new [`RegionMetadataLister`] with the given [`RegionRpcRef`].
    pub fn new(region_rpc: RegionRpcRef) -> Self {
        Self { region_rpc }
    }

    /// Collects the region metadata from the datanodes.
    pub async fn list(
        &self,
        table_id: TableId,
        region_routes: &[RegionRoute],
    ) -> Result<Vec<Option<RegionMetadata>>> {
        let region_distribution = region_distribution(region_routes);
        let leaders = find_leaders(region_routes)
            .into_iter()
            .map(|p| (p.id, p))
            .collect::<HashMap<_, _>>();

        let total_num_region = region_distribution
            .values()
            .map(|r| r.leader_regions.len())
            .sum::<usize>();

        let mut list_metadata_tasks = Vec::with_capacity(leaders.len());

        // Build requests.
        for (datanode_id, region_role_set) in region_distribution {
            if region_role_set.leader_regions.is_empty() {
                continue;
            }
            // Safety: must exists.
            let peer = leaders.get(&datanode_id).unwrap();
            let region_ids = region_role_set
                .leader_regions
                .iter()
                .map(|r| RegionId::new(table_id, *r).as_u64())
                .collect();
            let request = Self::build_list_metadata_request(region_ids);

            let peer = peer.clone();
            let region_rpc = self.region_rpc.clone();
            list_metadata_tasks.push(async move {
                region_rpc
                    .handle_region(&peer, request)
                    .await
                    .map_err(add_peer_context_if_needed(peer))
            });
        }

        let results = join_all(list_metadata_tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|r| r.metadata);

        let mut output = Vec::with_capacity(total_num_region);
        for result in results {
            let region_metadatas: Vec<Option<RegionMetadata>> =
                serde_json::from_slice(&result).context(DecodeJsonSnafu)?;
            output.extend(region_metadatas);
        }

        Ok(output)
    }

    fn build_list_metadata_request(region_ids: Vec<u64>) -> RegionRequest {
        RegionRequest {
            header: Some(RegionRequestHeader {
                tracing_context: TracingContext::from_current_span().to_w3c(),
                ..Default::default()
            }),
            body: Some(PbRegionRequest::ListMetadata(ListMetadataRequest {
                region_ids,
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use api::region::RegionResponse;
    use api::v1::meta::Peer;
    use api::v1::region::RegionRequest;
    use api::v1::region::region_request::Body;
    use store_api::metadata::RegionMetadata;
    use store_api::storage::RegionId;
    use tokio::sync::mpsc;

    use crate::ddl::test_util::datanode_handler::{DatanodeWatcher, ListMetadataDatanodeHandler};
    use crate::ddl::test_util::region_metadata::build_region_metadata;
    use crate::ddl::test_util::test_column_metadatas;
    use crate::ddl::utils::region_metadata_lister::RegionMetadataLister;
    use crate::error::Result;
    use crate::rpc::router::{Region, RegionRoute};
    use crate::test_util::MockDatanodeManager;

    fn assert_list_metadata_request(req: RegionRequest, expected_region_ids: &[RegionId]) {
        let Some(Body::ListMetadata(req)) = req.body else {
            unreachable!()
        };

        assert_eq!(req.region_ids.len(), expected_region_ids.len());
        for region_id in expected_region_ids {
            assert!(req.region_ids.contains(&region_id.as_u64()));
        }
    }

    fn empty_list_metadata_handler(_peer: Peer, request: RegionRequest) -> Result<RegionResponse> {
        let Some(Body::ListMetadata(req)) = request.body else {
            unreachable!()
        };

        let mut output: Vec<Option<RegionMetadata>> = Vec::with_capacity(req.region_ids.len());
        for _region_id in req.region_ids {
            output.push(None);
        }

        Ok(RegionResponse::from_metadata(
            serde_json::to_vec(&output).unwrap(),
        ))
    }

    #[tokio::test]
    async fn test_list_request() {
        let (tx, mut rx) = mpsc::channel(8);
        let handler = DatanodeWatcher::new(tx).with_handler(empty_list_metadata_handler);
        let node_manager = Arc::new(MockDatanodeManager::new(handler));
        let lister = RegionMetadataLister::new(node_manager);
        let region_routes = vec![
            RegionRoute {
                region: Region::new_test(RegionId::new(1024, 1)),
                leader_peer: Some(Peer::empty(1)),
                follower_peers: vec![Peer::empty(5)],
                leader_state: None,
                leader_down_since: None,
            },
            RegionRoute {
                region: Region::new_test(RegionId::new(1024, 2)),
                leader_peer: Some(Peer::empty(3)),
                follower_peers: vec![Peer::empty(4)],
                leader_state: None,
                leader_down_since: None,
            },
            RegionRoute {
                region: Region::new_test(RegionId::new(1024, 3)),
                leader_peer: Some(Peer::empty(3)),
                follower_peers: vec![Peer::empty(4)],
                leader_state: None,
                leader_down_since: None,
            },
        ];
        let region_metadatas = lister.list(1024, &region_routes).await.unwrap();
        assert_eq!(region_metadatas.len(), 3);

        let mut requests = vec![];
        for _ in 0..2 {
            let (peer, request) = rx.try_recv().unwrap();
            requests.push((peer, request));
        }
        rx.try_recv().unwrap_err();

        let (peer, request) = requests.remove(0);
        assert_eq!(peer.id, 1);
        assert_list_metadata_request(request, &[RegionId::new(1024, 1)]);
        let (peer, request) = requests.remove(0);
        assert_eq!(peer.id, 3);
        assert_list_metadata_request(request, &[RegionId::new(1024, 2), RegionId::new(1024, 3)]);
    }

    #[tokio::test]
    async fn test_list_region_metadata() {
        let region_metadata =
            build_region_metadata(RegionId::new(1024, 1), &test_column_metadatas(&["tag_0"]));
        let region_metadatas = HashMap::from([
            (RegionId::new(1024, 0), None),
            (RegionId::new(1024, 1), Some(region_metadata.clone())),
        ]);
        let handler = ListMetadataDatanodeHandler::new(region_metadatas);
        let node_manager = Arc::new(MockDatanodeManager::new(handler));
        let lister = RegionMetadataLister::new(node_manager);
        let region_routes = vec![
            RegionRoute {
                region: Region::new_test(RegionId::new(1024, 0)),
                leader_peer: Some(Peer::empty(1)),
                follower_peers: vec![],
                leader_state: None,
                leader_down_since: None,
            },
            RegionRoute {
                region: Region::new_test(RegionId::new(1024, 1)),
                leader_peer: Some(Peer::empty(3)),
                follower_peers: vec![],
                leader_state: None,
                leader_down_since: None,
            },
        ];
        let region_metadatas = lister.list(1024, &region_routes).await.unwrap();
        assert_eq!(region_metadatas.len(), 2);
        assert_eq!(region_metadatas[0], None);
        assert_eq!(region_metadatas[1], Some(region_metadata));
    }
}
