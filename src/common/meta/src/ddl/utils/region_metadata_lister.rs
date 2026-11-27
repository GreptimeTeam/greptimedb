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

use std::borrow::Cow;
use std::collections::HashMap;

use api::v1::region::region_request::Body as PbRegionRequest;
use api::v1::region::{ListMetadataRequest, RegionRequest, RegionRequestHeader};
use common_telemetry::tracing_context::TracingContext;
use futures::future::join_all;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadata;
use store_api::storage::consts::ReservedColumnId;
use store_api::storage::{ColumnId, RegionId, TableId};
use table::metadata::TableInfo;

use crate::ddl::utils::add_peer_context_if_needed;
use crate::error::{self, DecodeJsonSnafu, Result};
use crate::node_manager::NodeManagerRef;
use crate::rpc::router::{RegionRoute, find_leaders, region_distribution};

/// Collects the region metadata from the datanodes.
pub struct RegionMetadataLister {
    node_manager: NodeManagerRef,
}

/// The ordered tag IDs and names of a sparse Mito region.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparseRegionPrimaryKey {
    columns: Vec<(ColumnId, String)>,
}

impl SparseRegionPrimaryKey {
    /// Preserves the current catalog schema, replacing only an obsolete ordering of the same tags.
    pub fn align_table<'a>(&self, table: &'a TableInfo) -> Result<Cow<'a, TableInfo>> {
        let meta = &table.meta;
        let catalog_tag = |index: usize| {
            let name = meta.schema.column_schemas().get(index)?.name.as_str();
            Some((*meta.column_ids.get(index)?, name))
        };
        let region_tags = self.columns.iter().map(|(id, name)| (*id, name.as_str()));
        if meta
            .primary_key_indices
            .iter()
            .map(|&i| catalog_tag(i))
            .eq(region_tags.clone().map(Some))
        {
            return Ok(Cow::Borrowed(table));
        }

        let positions: HashMap<_, _> = meta
            .primary_key_indices
            .iter()
            .filter_map(|&i| Some((catalog_tag(i)?, i)))
            .collect();
        let mismatch = || error::InconsistentSparsePrimaryKeySnafu {
            table_id: table.table_id(),
        };
        ensure!(
            self.columns.len() == meta.primary_key_indices.len(),
            mismatch()
        );
        let indices = region_tags
            .map(|tag| positions.get(&tag).copied())
            .collect::<Option<Vec<_>>>()
            .with_context(mismatch)?;
        let mut aligned = table.clone();
        aligned.meta.primary_key_indices = indices;
        Ok(Cow::Owned(aligned))
    }
}

impl RegionMetadataLister {
    /// Creates a new [`RegionMetadataLister`] with the given [`NodeManagerRef`].
    pub fn new(node_manager: NodeManagerRef) -> Self {
        Self { node_manager }
    }

    /// Reads each destination's physical key. Writes may safely omit a newly added NULL suffix.
    pub async fn sparse_primary_keys(
        &self,
        table_id: TableId,
        routes: &[RegionRoute],
        write_schema: Option<&TableInfo>,
    ) -> Result<HashMap<RegionId, SparseRegionPrimaryKey>> {
        let regions = self.list(table_id, routes).await?;
        ensure!(
            regions.len() == routes.len() && regions.iter().all(Option::is_some),
            error::NoLeaderSnafu { table_id }
        );
        let keys = regions
            .into_iter()
            .flatten()
            .map(|region| {
                let mismatch = || error::InconsistentSparsePrimaryKeySnafu { table_id };
                ensure!(
                    region.primary_key_encoding == PrimaryKeyEncoding::Sparse
                        && region
                            .primary_key
                            .starts_with(&[ReservedColumnId::table_id(), ReservedColumnId::tsid()]),
                    mismatch()
                );
                let mut key = region.primary_key.as_slice();
                if let Some(table) = write_schema {
                    let width = table.meta.primary_key_indices.len();
                    key = region
                        .sparse_key_prefix_for_write(width)
                        .context(error::SparseWriteSchemaSnafu)?;
                    for id in &region.primary_key[width..] {
                        let column = &region
                            .column_by_id(*id)
                            .with_context(mismatch)?
                            .column_schema;
                        ensure!(
                            !table.meta.column_ids.contains(id)
                                && !table.meta.schema.contains_column(&column.name),
                            mismatch()
                        );
                    }
                }
                let columns = key
                    .iter()
                    .map(|&id| {
                        let column = region.column_by_id(id).with_context(mismatch)?;
                        Ok((id, column.column_schema.name.clone()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok((region.region_id, SparseRegionPrimaryKey { columns }))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        ensure!(
            routes
                .iter()
                .all(|route| keys.contains_key(&route.region.id)),
            error::NoLeaderSnafu { table_id }
        );
        Ok(keys)
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
            let requester = self.node_manager.datanode(peer).await;
            let region_ids = region_role_set
                .leader_regions
                .iter()
                .map(|r| RegionId::new(table_id, *r).as_u64())
                .collect();
            let request = Self::build_list_metadata_request(region_ids);

            let peer = peer.clone();
            list_metadata_tasks.push(async move {
                requester
                    .handle(request)
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
                write_route_policy: None,
            },
            RegionRoute {
                region: Region::new_test(RegionId::new(1024, 2)),
                leader_peer: Some(Peer::empty(3)),
                follower_peers: vec![Peer::empty(4)],
                leader_state: None,
                leader_down_since: None,
                write_route_policy: None,
            },
            RegionRoute {
                region: Region::new_test(RegionId::new(1024, 3)),
                leader_peer: Some(Peer::empty(3)),
                follower_peers: vec![Peer::empty(4)],
                leader_state: None,
                leader_down_since: None,
                write_route_policy: None,
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
                write_route_policy: None,
            },
            RegionRoute {
                region: Region::new_test(RegionId::new(1024, 1)),
                leader_peer: Some(Peer::empty(3)),
                follower_peers: vec![],
                leader_state: None,
                leader_down_since: None,
                write_route_policy: None,
            },
        ];
        let region_metadatas = lister.list(1024, &region_routes).await.unwrap();
        assert_eq!(region_metadatas.len(), 2);
        assert_eq!(region_metadatas[0], None);
        assert_eq!(region_metadatas[1], Some(region_metadata));
        assert!(matches!(
            lister.sparse_primary_keys(1024, &region_routes, None).await,
            Err(crate::error::Error::NoLeader { .. })
        ));
    }

    #[tokio::test]
    async fn test_sparse_primary_keys_preserve_each_regions_order() {
        let mut columns = test_column_metadatas(&["host", "site"]);
        columns[2].column_schema.data_type =
            datatypes::prelude::ConcreteDataType::uint32_datatype();
        columns[3].column_schema.data_type =
            datatypes::prelude::ConcreteDataType::uint64_datatype();
        let routes = [0, 1].map(|number| RegionRoute {
            region: Region::new_test(RegionId::new(1024, number)),
            leader_peer: Some(Peer::empty(1)),
            ..Default::default()
        });
        let mut metadata = HashMap::new();
        for (index, route) in routes.iter().enumerate() {
            let mut region = build_region_metadata(route.region.id, &columns);
            region.primary_key_encoding = store_api::codec::PrimaryKeyEncoding::Sparse;
            if index == 1 {
                region.primary_key.swap(2, 3);
            }
            metadata.insert(region.region_id, Some(region));
        }
        let handler = ListMetadataDatanodeHandler::new(metadata);
        let lister = RegionMetadataLister::new(Arc::new(MockDatanodeManager::new(handler)));
        let keys = lister
            .sparse_primary_keys(1024, &routes, None)
            .await
            .unwrap();
        assert_ne!(keys[&routes[0].region.id], keys[&routes[1].region.id]);
    }
}
