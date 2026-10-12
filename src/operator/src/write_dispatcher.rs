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

mod options;
#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::sync::Arc;

use api::region::RegionResponse;
use api::v1::region::{DeleteRequests, InsertRequests, RegionRequest, region_request::Body};
use backon::{BackoffBuilder, ExponentialBuilder};
use client::client_manager::NodeClients;
use common_error::ext::{BoxedError, ErrorExt, RetryHint};
use common_meta::cache::TableInfoCacheRef;
use common_meta::cache_invalidator::{CacheInvalidatorRef, Context};
use common_meta::error::{ExternalSnafu, Result, TableInfoNotFoundSnafu, UnexpectedSnafu};
use common_meta::instruction::CacheIdent;
use common_meta::node_manager::NodeManagerRef;
use common_meta::peer::Peer;
use common_meta::rpc::router;
use futures::future;
use partition::manager::PartitionRuleManagerRef;
use prometheus::Histogram;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;

pub use self::options::WriteRetryOptions;
use crate::error::{self, FindRegionLeaderSnafu, RequestRegionSnafu};
use crate::req_convert::TablePartitioner;

/// Dispatches already partitioned writes, retaining only failed payloads for retries.
pub struct WriteDispatcher {
    node_manager: NodeManagerRef,
    partition_manager: PartitionRuleManagerRef,
    remote: Option<RemoteRetry>,
}

/// Shared reference to a [`WriteDispatcher`].
pub type WriteDispatcherRef = Arc<WriteDispatcher>;

/// A completed send, retaining its request only when a retry is possible.
pub(crate) struct WriteOutcome {
    result: Result<RegionResponse>,
    pending: Option<PendingWrite>,
}

impl WriteDispatcher {
    /// Creates an owned-request dispatcher without retries, including for standalone.
    pub fn new(node_manager: NodeManagerRef, partition_manager: PartitionRuleManagerRef) -> Self {
        Self {
            node_manager,
            partition_manager,
            remote: None,
        }
    }

    /// Enables shared remote sends and retries using frontend configuration and caches.
    pub fn with_retry(
        mut self,
        clients: Arc<NodeClients>,
        options: WriteRetryOptions,
        table_info_cache: TableInfoCacheRef,
        cache_invalidator: CacheInvalidatorRef,
    ) -> Self {
        if options.max_retries > 0 {
            self.remote = Some(RemoteRetry {
                clients,
                options,
                table_info_cache,
                cache_invalidator,
            });
        } else {
            self.remote = None;
        }
        self
    }

    /// Writes planned requests without repeating admission, default values, or Flow side effects.
    pub async fn dispatch(&self, requests: Vec<RegionRequest>) -> Result<RegionResponse> {
        self.dispatch_inner(requests, None).await
    }

    /// Records each datanode send attempt, including retries, in the supplied histogram.
    pub async fn dispatch_with_metrics(
        &self,
        requests: Vec<RegionRequest>,
        histogram: Histogram,
    ) -> Result<RegionResponse> {
        self.dispatch_inner(requests, Some(histogram)).await
    }

    /// Resolves routes before writes are detached from the caller.
    pub(crate) async fn prepare(
        &self,
        requests: Vec<RegionRequest>,
    ) -> error::Result<Vec<(Peer, RegionRequest)>> {
        let requests = requests.into_iter().map(split_request).collect::<Vec<_>>();
        let regions = requests
            .iter()
            .flatten()
            .map(write_region_id)
            .collect::<Result<Vec<_>>>()
            .context(RequestRegionSnafu)?;
        let peers = self
            .resolve_peers(&regions)
            .await
            .context(FindRegionLeaderSnafu)?;
        let mut writes = Vec::new();
        // Preserve each original envelope and peer batching when retries are disabled.
        for requests in requests {
            let mut by_peer: HashMap<Peer, RegionRequest> = HashMap::new();
            for request in requests {
                let peer = get_peer(
                    &peers,
                    write_region_id(&request).context(RequestRegionSnafu)?,
                )
                .context(RequestRegionSnafu)?;
                match by_peer.entry(peer) {
                    Entry::Vacant(entry) => {
                        entry.insert(request);
                    }
                    Entry::Occupied(mut entry) => match (&mut entry.get_mut().body, request.body) {
                        (Some(Body::Inserts(existing)), Some(Body::Inserts(next))) => {
                            existing.requests.extend(next.requests)
                        }
                        (Some(Body::Deletes(existing)), Some(Body::Deletes(next))) => {
                            existing.requests.extend(next.requests)
                        }
                        _ => {
                            return UnexpectedSnafu {
                                err_msg: "Cannot combine different write kinds",
                            }
                            .fail()
                            .context(RequestRegionSnafu);
                        }
                    },
                }
            }
            writes.extend(by_peer);
        }
        Ok(writes)
    }

    /// Sends requests using routes already resolved by the caller.
    pub(crate) async fn dispatch_prepared(
        &self,
        writes: Vec<(Peer, RegionRequest)>,
        histogram: Option<Histogram>,
    ) -> Result<RegionResponse> {
        let histogram = histogram.as_ref();
        let writes = if self.remote.is_some() {
            writes
                .into_iter()
                .flat_map(|(peer, request)| {
                    split_request(request)
                        .into_iter()
                        .map(move |request| (peer.clone(), request))
                })
                .collect()
        } else {
            writes
        };
        let outcomes = future::join_all(
            writes
                .into_iter()
                .map(|(peer, request)| self.dispatch_initial(peer, request, histogram)),
        )
        .await;
        self.finish_dispatch(outcomes, histogram).await
    }

    /// Sends the first attempt immediately so bulk encoding can overlap other sends.
    pub(crate) async fn dispatch_initial(
        &self,
        peer: Peer,
        request: RegionRequest,
        histogram: Option<&Histogram>,
    ) -> WriteOutcome {
        if let Some(remote) = &self.remote {
            return Self::send_write(
                remote,
                PendingWrite {
                    peer,
                    request: Arc::new(request),
                },
                histogram,
            )
            .await;
        }
        let _timer = histogram.map(Histogram::start_timer);
        let datanode = self.node_manager.datanode(&peer).await;
        WriteOutcome {
            result: datanode.handle(request).await,
            pending: None,
        }
    }

    /// Collects first attempts and retries their failures together, refreshing routes once per round.
    pub(crate) async fn finish_dispatch(
        &self,
        outcomes: Vec<WriteOutcome>,
        histogram: Option<&Histogram>,
    ) -> Result<RegionResponse> {
        if let Some(remote) = &self.remote {
            return self.dispatch_with_retry(remote, outcomes, histogram).await;
        }
        let mut response = RegionResponse::new(0);
        for outcome in outcomes {
            merge_response(&mut response, outcome.result?);
        }
        Ok(response)
    }

    /// Resolves only the requested peers, loading each table route once.
    pub(crate) async fn resolve_peers(
        &self,
        regions: &[RegionId],
    ) -> partition::error::Result<HashMap<RegionId, Peer>> {
        let table_ids = regions
            .iter()
            .map(|region| region.table_id())
            .collect::<HashSet<_>>();
        let mut physical_tables = HashMap::new();
        let mut routes = HashMap::new();
        for table_id in table_ids {
            let (physical_id, route) = self
                .partition_manager
                .find_physical_table_route_with_id(table_id)
                .await?;
            physical_tables.insert(table_id, physical_id);
            routes.entry(physical_id).or_insert(route);
        }
        let mut peers = HashMap::with_capacity(regions.len());
        for region in regions {
            if peers.contains_key(region) {
                continue;
            }
            let peer = physical_tables
                .get(&region.table_id())
                .and_then(|physical_id| routes.get(physical_id))
                .and_then(|route| {
                    router::find_region_leader(&route.region_routes, region.region_number())
                })
                .context(partition::error::FindLeaderSnafu {
                    region_id: *region,
                    table_id: region.table_id(),
                })?;
            peers.insert(*region, peer);
        }
        Ok(peers)
    }

    async fn send_write(
        remote: &RemoteRetry,
        write: PendingWrite,
        histogram: Option<&Histogram>,
    ) -> WriteOutcome {
        let _timer = histogram.map(Histogram::start_timer);
        let requester = remote.clients.make_region_requester(&write.peer).await;
        let result = requester.handle(Arc::clone(&write.request)).await;
        let pending = if result
            .as_ref()
            .is_err_and(|error| error.retry_hint().is_retryable())
        {
            Some(write)
        } else {
            None
        };
        WriteOutcome {
            result: result.map_err(Into::into),
            pending,
        }
    }

    async fn dispatch_with_retry(
        &self,
        remote: &RemoteRetry,
        mut outcomes: Vec<WriteOutcome>,
        histogram: Option<&Histogram>,
    ) -> Result<RegionResponse> {
        let mut response = RegionResponse::new(0);
        let mut backoff = ExponentialBuilder::default()
            .with_min_delay(remote.options.initial_backoff)
            .with_max_delay(remote.options.max_backoff)
            .with_factor(remote.options.backoff_multiplier)
            .with_max_times(remote.options.max_retries)
            .with_jitter()
            .build();

        loop {
            let mut failures = Vec::new();
            let mut terminal_error = None;
            for outcome in outcomes {
                match (outcome.pending, outcome.result) {
                    (_, Ok(result)) => {
                        merge_response(&mut response, result);
                    }
                    (Some(write), Err(error)) => {
                        failures.push((write, error));
                    }
                    (None, Err(error)) => {
                        terminal_error.get_or_insert(error);
                    }
                }
            }
            // All in-flight requests finish before returning an error. Dropping a
            // sibling future does not cancel a write already accepted by a datanode.
            if let Some(error) = terminal_error {
                return Err(error);
            }
            if failures.is_empty() {
                return Ok(response);
            }
            let Some(delay) = backoff.next() else {
                let (_, error) = failures.into_iter().next().context(UnexpectedSnafu {
                    err_msg: "Write retry budget exhausted without a failed request",
                })?;
                return Err(error);
            };
            tokio::time::sleep(delay).await;

            let mut refresh = Vec::new();
            let mut pending = Vec::new();
            for (write, error) in failures {
                if error.retry_hint() == RetryHint::RetryAfterRouteRefresh {
                    refresh.push(write);
                } else {
                    pending.push(write);
                }
            }
            if !refresh.is_empty() {
                pending.extend(self.reroute_failed_writes(remote, refresh).await?);
            }
            outcomes = future::join_all(
                pending
                    .into_iter()
                    .map(|write| Self::send_write(remote, write, histogram)),
            )
            .await;
        }
    }

    async fn reroute_failed_writes(
        &self,
        remote: &RemoteRetry,
        writes: Vec<PendingWrite>,
    ) -> Result<Vec<PendingWrite>> {
        let regions = writes
            .iter()
            .map(|write| write_region_id(&write.request))
            .collect::<Result<Vec<_>>>()?;
        self.invalidate_routes(&regions, &remote.cache_invalidator)
            .await?;
        let mut partitioners = HashMap::new();
        for table_id in regions
            .iter()
            .map(|region| region.table_id())
            .collect::<HashSet<_>>()
        {
            let table_info = remote
                .table_info_cache
                .get(table_id)
                .await?
                .with_context(|| TableInfoNotFoundSnafu {
                    table: table_id.to_string(),
                })?;
            let partitioner = TablePartitioner::try_new(&self.partition_manager, &table_info)
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;
            partitioners.insert(table_id, partitioner);
        }
        let mut repartitioned = Vec::new();
        for write in writes {
            let request = Arc::try_unwrap(write.request).map_err(|_| {
                UnexpectedSnafu {
                    err_msg: "Write request is still shared after its RPC completed",
                }
                .build()
            })?;
            let table_id = write_region_id(&request)?.table_id();
            let partitioner = partitioners.get(&table_id).context(UnexpectedSnafu {
                err_msg: "Refreshed write has no partition snapshot",
            })?;
            let requests = partitioner
                .repartition_request(request)
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;
            repartitioned.extend(requests);
        }
        self.prepare_writes(repartitioned).await
    }

    async fn prepare_writes(&self, requests: Vec<RegionRequest>) -> Result<Vec<PendingWrite>> {
        let regions = requests
            .iter()
            .map(write_region_id)
            .collect::<Result<Vec<_>>>()?;
        let peers = self
            .resolve_peers(&regions)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        requests
            .into_iter()
            .zip(regions)
            .map(|(request, region)| {
                Ok(PendingWrite {
                    peer: get_peer(&peers, region)?,
                    request: Arc::new(request),
                })
            })
            .collect()
    }

    async fn dispatch_inner(
        &self,
        requests: Vec<RegionRequest>,
        histogram: Option<Histogram>,
    ) -> Result<RegionResponse> {
        let writes = self
            .prepare(requests)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        self.dispatch_prepared(writes, histogram).await
    }
    async fn invalidate_routes(
        &self,
        regions: &[RegionId],
        invalidator: &CacheInvalidatorRef,
    ) -> Result<()> {
        let mut table_ids = regions
            .iter()
            .map(|region| region.table_id())
            .collect::<HashSet<_>>();
        let mut physical_ids = HashSet::new();
        for table_id in &table_ids {
            let (physical_id, _) = self
                .partition_manager
                .find_physical_table_route_with_id(*table_id)
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;
            physical_ids.insert(physical_id);
        }
        table_ids.extend(physical_ids);
        let caches = table_ids
            .into_iter()
            .map(CacheIdent::TableId)
            .collect::<Vec<_>>();
        invalidator.invalidate(&Context::default(), &caches).await
    }
}

struct RemoteRetry {
    clients: Arc<NodeClients>,
    options: WriteRetryOptions,
    table_info_cache: TableInfoCacheRef,
    cache_invalidator: CacheInvalidatorRef,
}

struct PendingWrite {
    peer: Peer,
    request: Arc<RegionRequest>,
}

fn get_peer(peers: &HashMap<RegionId, Peer>, region: RegionId) -> Result<Peer> {
    peers.get(&region).cloned().context(UnexpectedSnafu {
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
        UnexpectedSnafu {
            err_msg: "Expected a single-region write request",
        }
        .build()
    })
}
