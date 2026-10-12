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

#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use api::region::RegionResponse;
use api::v1::region::{DeleteRequests, InsertRequests, RegionRequest, region_request::Body};
use common_error::ext::BoxedError;
use common_meta::error::{self, Result};
use common_meta::node_manager::NodeManagerRef;
use common_meta::peer::Peer;
use common_meta::rpc::router;
use futures::future;
use partition::manager::PartitionRuleManagerRef;
use prometheus::Histogram;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;

/// Dispatches already partitioned insert, delete, and bulk requests to their peers.
pub struct WriteDispatcher {
    node_manager: NodeManagerRef,
    partition_manager: PartitionRuleManagerRef,
}

/// Shared reference to a [`WriteDispatcher`].
pub type WriteDispatcherRef = Arc<WriteDispatcher>;

impl WriteDispatcher {
    /// Creates an owned-request dispatcher without retries, including for standalone.
    pub fn new(node_manager: NodeManagerRef, partition_manager: PartitionRuleManagerRef) -> Self {
        Self {
            node_manager,
            partition_manager,
        }
    }

    /// Writes planned requests without repeating admission, default values, or Flow side effects.
    pub async fn dispatch(&self, requests: Vec<RegionRequest>) -> Result<RegionResponse> {
        self.dispatch_inner(requests, None).await
    }

    /// Records each datanode send in the supplied histogram.
    pub async fn dispatch_with_metrics(
        &self,
        requests: Vec<RegionRequest>,
        histogram: Histogram,
    ) -> Result<RegionResponse> {
        self.dispatch_inner(requests, Some(histogram)).await
    }

    async fn dispatch_inner(
        &self,
        requests: Vec<RegionRequest>,
        histogram: Option<Histogram>,
    ) -> Result<RegionResponse> {
        self.dispatch_once(requests, histogram.as_ref()).await
    }

    async fn dispatch_once(
        &self,
        requests: Vec<RegionRequest>,
        histogram: Option<&Histogram>,
    ) -> Result<RegionResponse> {
        let requests = requests.into_iter().map(split_request).collect::<Vec<_>>();
        let regions = requests
            .iter()
            .flatten()
            .map(write_region_id)
            .collect::<Result<Vec<_>>>()?;
        let peers = self.resolve_peers(&regions).await?;
        let mut writes = Vec::new();
        // Preserve each original envelope and peer batching when retries are disabled.
        for requests in requests {
            let mut by_peer: HashMap<Peer, RegionRequest> = HashMap::new();
            for request in requests {
                let peer = get_peer(&peers, write_region_id(&request)?)?;
                match by_peer.entry(peer) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(request);
                    }
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        match (&mut entry.get_mut().body, request.body) {
                            (Some(Body::Inserts(existing)), Some(Body::Inserts(next))) => {
                                existing.requests.extend(next.requests)
                            }
                            (Some(Body::Deletes(existing)), Some(Body::Deletes(next))) => {
                                existing.requests.extend(next.requests)
                            }
                            _ => {
                                return error::UnexpectedSnafu {
                                    err_msg: "Cannot combine different write kinds",
                                }
                                .fail();
                            }
                        }
                    }
                }
            }
            writes.extend(by_peer);
        }
        let results = future::join_all(writes.into_iter().map(|(peer, request)| async move {
            let datanode = self.node_manager.datanode(&peer).await;
            let _timer = histogram.map(Histogram::start_timer);
            datanode.handle(request).await
        }))
        .await;
        let mut response = RegionResponse::new(0);
        for result in results {
            merge_response(&mut response, result?);
        }
        Ok(response)
    }

    async fn resolve_peers(&self, regions: &[RegionId]) -> Result<HashMap<RegionId, Peer>> {
        let table_ids = regions
            .iter()
            .map(|region| region.table_id())
            .collect::<HashSet<_>>();
        let mut physical_tables = HashMap::new();
        let mut leaders = HashMap::new();
        for table_id in table_ids {
            let (physical_id, route) = self
                .partition_manager
                .find_physical_table_route_with_id(table_id)
                .await
                .map_err(BoxedError::new)
                .context(error::ExternalSnafu)?;
            physical_tables.insert(table_id, physical_id);
            leaders.entry(physical_id).or_insert_with(|| {
                router::convert_to_region_leader_map(&route.region_routes)
                    .into_iter()
                    .map(|(region, peer)| (region, peer.clone()))
                    .collect::<HashMap<_, _>>()
            });
        }
        let mut peers = HashMap::with_capacity(regions.len());
        for region in regions {
            if peers.contains_key(region) {
                continue;
            }
            let peer = physical_tables
                .get(&region.table_id())
                .and_then(|physical_id| leaders.get(physical_id))
                .and_then(|leaders| leaders.get(&region.region_number()))
                .cloned()
                .context(partition::error::FindLeaderSnafu {
                    region_id: *region,
                    table_id: region.table_id(),
                })
                .map_err(BoxedError::new)
                .context(error::ExternalSnafu)?;
            peers.insert(*region, peer);
        }
        Ok(peers)
    }
}

fn get_peer(peers: &HashMap<RegionId, Peer>, region: RegionId) -> Result<Peer> {
    peers.get(&region).cloned().context(error::UnexpectedSnafu {
        err_msg: "Planned write has no resolved peer",
    })
}

fn merge_response(response: &mut RegionResponse, result: RegionResponse) {
    response.affected_rows += result.affected_rows;
    response.extensions.extend(result.extensions);
    response.metadata = result.metadata;
}

fn split_request(request: RegionRequest) -> Vec<RegionRequest> {
    let RegionRequest { header, body } = request;
    let bodies: Vec<Body> = match body {
        Some(Body::Inserts(inserts)) => inserts
            .requests
            .into_iter()
            .map(|request| {
                Body::Inserts(InsertRequests {
                    requests: vec![request],
                })
            })
            .collect(),
        Some(Body::Deletes(deletes)) => deletes
            .requests
            .into_iter()
            .map(|request| {
                Body::Deletes(DeleteRequests {
                    requests: vec![request],
                })
            })
            .collect(),
        body => return vec![RegionRequest { header, body }],
    };
    bodies
        .into_iter()
        .map(|body| RegionRequest {
            header: header.clone(),
            body: Some(body),
        })
        .collect()
}

fn write_region_id(request: &RegionRequest) -> Result<RegionId> {
    let region_id = match &request.body {
        Some(Body::Inserts(inserts)) => match inserts.requests.as_slice() {
            [request] => Some(request.region_id),
            _ => None,
        },
        Some(Body::Deletes(deletes)) => match deletes.requests.as_slice() {
            [request] => Some(request.region_id),
            _ => None,
        },
        Some(Body::BulkInsert(bulk)) => Some(bulk.region_id),
        _ => None,
    };
    region_id.map(RegionId::from_u64).ok_or_else(|| {
        error::UnexpectedSnafu {
            err_msg: "Expected a single-region write request",
        }
        .build()
    })
}
