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
mod progress;
pub use progress::TrackedWrite;
#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::sync::Arc;

use api::region::RegionResponse;
use api::v1::region::{DeleteRequests, InsertRequests, RegionRequest, region_request::Body};
use backon::{BackoffBuilder, ExponentialBuilder};
use catalog::CatalogManagerRef;
use common_error::ext::{BoxedError, ErrorExt, RetryHint};
use common_meta::cache_invalidator::{CacheInvalidatorRef, Context};
use common_meta::error::{
    BatchRequestSnafu, Error, ExternalSnafu, Result, TableInfoNotFoundSnafu, UnexpectedSnafu,
};
use common_meta::instruction::CacheIdent;
use common_meta::node_manager::NodeManagerRef;
use common_meta::peer::{Peer, PeerRef};
use common_meta::rpc::router;
use futures::future;
use partition::manager::{PartitionRuleManager, PartitionRuleManagerRef};
use prometheus::Histogram;
use snafu::{IntoError, OptionExt, ResultExt, ensure};
use store_api::storage::RegionId;
use table::table_name::TableName;
use tokio_util::sync::CancellationToken;

pub use self::options::WriteRetryOptions;
use crate::error::{self, FindRegionLeaderSnafu, RequestRegionSnafu};
use crate::req_convert::TablePartitioner;

/// Dispatches already partitioned writes, retaining only failed payloads for retries.
pub struct WriteDispatcher {
    node_manager: NodeManagerRef,
    partition_manager: PartitionRuleManagerRef,
    retry: Option<WriteRetry>,
}

/// Shared reference to a [`WriteDispatcher`].
pub type WriteDispatcherRef = Arc<WriteDispatcher>;

/// The result of one write RPC attempt.
pub(crate) enum WriteAttempt {
    Success(RegionResponse),
    Failed {
        error: Error,
        /// Retained when retries are enabled and the original client error allows retry.
        /// The remaining retry budget is checked separately.
        request: Option<RoutedWrite>,
    },
}

impl WriteDispatcher {
    /// Creates an owned-request dispatcher without retries, including for standalone.
    pub fn new(node_manager: NodeManagerRef, partition_manager: PartitionRuleManagerRef) -> Self {
        Self {
            node_manager,
            partition_manager,
            retry: None,
        }
    }

    /// Enables shared remote sends and retries using frontend configuration and caches.
    pub fn with_retry(
        mut self,
        options: WriteRetryOptions,
        catalog_manager: CatalogManagerRef,
        cache_invalidator: CacheInvalidatorRef,
    ) -> Self {
        if options.max_retries > 0 {
            self.retry = Some(WriteRetry {
                node_manager: self.node_manager.clone(),
                partition_manager: self.partition_manager.clone(),
                options,
                catalog_manager,
                cache_invalidator,
            });
        } else {
            self.retry = None;
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
    ) -> error::Result<Vec<(PeerRef, RegionRequest)>> {
        let requests = requests
            .into_iter()
            .map(|mut request| {
                let header = request.header.take();
                (header, split_request(request))
            })
            .collect::<Vec<_>>();
        let regions = requests
            .iter()
            .flat_map(|(_, requests)| requests)
            .map(write_region_id)
            .collect::<Result<Vec<_>>>()
            .context(RequestRegionSnafu)?;
        let peers = self
            .resolve_peers(&regions)
            .await
            .context(FindRegionLeaderSnafu)?;
        let mut writes = Vec::new();
        // Preserve each original envelope and peer batching when retries are disabled.
        for (header, requests) in requests {
            let mut by_peer: HashMap<PeerRef, RegionRequest> = HashMap::new();
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
            for (peer, mut request) in by_peer {
                request.header = header.clone();
                writes.push((peer, request));
            }
        }
        Ok(writes)
    }

    /// Sends requests using routes already resolved by the caller.
    /// Cancellation stops future retries while allowing in-flight requests to complete.
    pub(crate) async fn execute(
        &self,
        writes: Vec<(PeerRef, RegionRequest)>,
        histogram: Option<Histogram>,
        cancellation: Option<&CancellationToken>,
    ) -> Result<RegionResponse> {
        let histogram = histogram.as_ref();
        let attempts = future::join_all(writes.into_iter().map(|(peer, request)| async move {
            if self.retry.is_some() {
                self.send_batch(peer, request, histogram).await
            } else {
                vec![self.send(peer, request, histogram).await]
            }
        }))
        .await
        .into_iter()
        .flatten()
        .collect();
        self.finish(attempts, histogram, cancellation).await
    }

    /// Sends the first attempt immediately so bulk encoding can overlap other sends.
    pub(crate) async fn send(
        &self,
        peer: PeerRef,
        mut request: RegionRequest,
        histogram: Option<&Histogram>,
    ) -> WriteAttempt {
        if let Some(retry) = &self.retry {
            if matches!(&request.body, Some(Body::Inserts(_) | Body::Deletes(_))) {
                api::region::set_accept_partial_result(request.header.get_or_insert_default());
            }
            return retry
                .send(
                    RoutedWrite {
                        peer,
                        request: Arc::new(request),
                        owners: None,
                    },
                    histogram,
                )
                .await;
        }
        let _timer = histogram.map(Histogram::start_timer);
        let datanode = self.node_manager.datanode(&peer).await;
        match datanode.handle(request).await {
            Ok(response) => WriteAttempt::Success(response),
            Err(error) => WriteAttempt::Failed {
                error,
                request: None,
            },
        }
    }

    /// Collects first attempts and retries their failures together, refreshing routes once per round.
    pub(crate) async fn finish(
        &self,
        attempts: Vec<WriteAttempt>,
        histogram: Option<&Histogram>,
        cancellation: Option<&CancellationToken>,
    ) -> Result<RegionResponse> {
        if let Some(retry) = &self.retry {
            if retry.should_retry(&attempts, 0) {
                return retry.retry(attempts, histogram, cancellation).await;
            }
            let mut response = RegionResponse::new(0);
            let failures = merge_attempts(attempts, &mut response)?;
            return match failures.into_iter().next() {
                Some((_, error)) => Err(error),
                None => Ok(response),
            };
        }
        let mut response = RegionResponse::new(0);
        for attempt in attempts {
            match attempt {
                WriteAttempt::Success(result) => merge_response(&mut response, result),
                WriteAttempt::Failed { error, .. } => return Err(error),
            }
        }
        Ok(response)
    }

    /// Resolves only the requested peers, loading each table route once.
    pub(crate) async fn resolve_peers(
        &self,
        regions: &[RegionId],
    ) -> partition::error::Result<HashMap<RegionId, PeerRef>> {
        resolve_peers(&self.partition_manager, regions).await
    }

    async fn send_batch(
        &self,
        peer: PeerRef,
        mut request: RegionRequest,
        histogram: Option<&Histogram>,
    ) -> Vec<WriteAttempt> {
        if !matches!(&request.body, Some(Body::Inserts(_) | Body::Deletes(_))) {
            return vec![self.send(peer, request, histogram).await];
        }
        // Upgrade datanodes first and roll back frontends first: old datanodes
        // cannot report which entries succeeded, so retrying a batch may replay writes.
        api::region::set_accept_partial_result(request.header.get_or_insert_default());
        let request = Arc::new(request);
        let requester = self.node_manager.datanode(&peer).await;
        let result = {
            let _timer = histogram.map(Histogram::start_timer);
            requester.handle(Arc::clone(&request)).await
        };
        let error = match result {
            Ok(response) => return vec![WriteAttempt::Success(response)],
            Err(error) => error,
        };
        let request = match Arc::try_unwrap(request) {
            Ok(request) => request,
            Err(_) => {
                return vec![WriteAttempt::Failed {
                    error: UnexpectedSnafu {
                        err_msg: "Region request is still shared after its RPC completed",
                    }
                    .build(),
                    request: None,
                }];
            }
        };
        let requests = split_request(request);
        match error {
            Error::PartialRegion { response } => {
                let mut failures: HashMap<_, _> = response.failures.into_iter().collect();
                let mut attempts = vec![WriteAttempt::Success(RegionResponse {
                    affected_rows: response.affected_rows,
                    extensions: response.extensions,
                    metadata: Vec::new(),
                })];
                for (index, request) in requests.into_iter().enumerate() {
                    if let Some(error) = failures.remove(&index) {
                        let request = error.retry_hint().is_retryable().then(|| RoutedWrite {
                            peer: Arc::clone(&peer),
                            request: Arc::new(request),
                        });
                        attempts.push(WriteAttempt::Failed { error, request });
                    }
                }
                attempts
            }
            error => {
                // A whole-RPC failure cannot confirm success for any entry.
                let error = Arc::new(error);
                requests
                    .into_iter()
                    .map(|request| {
                        let request = error.retry_hint().is_retryable().then(|| RoutedWrite {
                            peer: Arc::clone(&peer),
                            request: Arc::new(request),
                        });
                        WriteAttempt::Failed {
                            error: BatchRequestSnafu.into_error(Arc::clone(&error)),
                            request,
                        }
                    })
                    .collect()
            }
        }
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
        self.execute(writes, histogram, None).await
    }
}

/// Sends shared requests and coordinates retries with refreshed routes.
struct WriteRetry {
    node_manager: NodeManagerRef,
    partition_manager: PartitionRuleManagerRef,
    options: WriteRetryOptions,
    catalog_manager: CatalogManagerRef,
    cache_invalidator: CacheInvalidatorRef,
}

/// A single-region write request paired with its destination peer.
pub(crate) struct RoutedWrite {
    peer: PeerRef,
    request: Arc<RegionRequest>,
    owners: Option<Vec<usize>>,
}

impl WriteRetry {
    async fn send(&self, write: RoutedWrite, histogram: Option<&Histogram>) -> WriteAttempt {
        let _timer = histogram.map(Histogram::start_timer);
        let requester = self.node_manager.datanode(&write.peer).await;
        match requester.handle(Arc::clone(&write.request)).await {
            Ok(response) => WriteAttempt::Success(response),
            Err(error) => {
                let request = error.retry_hint().is_retryable().then_some(write);
                WriteAttempt::Failed { error, request }
            }
        }
    }

    fn should_retry(&self, attempts: &[WriteAttempt], retries: usize) -> bool {
        if retries >= self.options.max_retries {
            return false;
        }
        let mut retryable = false;
        for attempt in attempts {
            match attempt {
                WriteAttempt::Success(_) => {}
                WriteAttempt::Failed {
                    request: Some(_), ..
                } => retryable = true,
                WriteAttempt::Failed { request: None, .. } => return false,
            }
        }
        retryable
    }

    async fn retry(
        &self,
        mut attempts: Vec<WriteAttempt>,
        histogram: Option<&Histogram>,
        cancellation: Option<&CancellationToken>,
    ) -> Result<RegionResponse> {
        let mut response = RegionResponse::new(0);
        let mut backoff = ExponentialBuilder::default()
            .with_min_delay(self.options.initial_backoff)
            .with_max_delay(self.options.max_backoff)
            .with_factor(self.options.backoff_multiplier)
            .with_max_times(self.options.max_retries)
            .with_jitter()
            .build();
        let mut retries = 0;
        while self.should_retry(&attempts, retries) {
            let Some(delay) = backoff.next() else {
                break;
            };
            if let Some(cancellation) = cancellation {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => break,
                    _ = tokio::time::sleep(delay) => {}
                }
            } else {
                tokio::time::sleep(delay).await;
            }
            if cancellation.is_some_and(CancellationToken::is_cancelled) {
                break;
            }
            let failures = merge_attempts(attempts, &mut response)?;
            let mut first_error = None;
            let mut refresh = Vec::new();
            let mut pending = Vec::new();
            for (write, error) in failures {
                if error.retry_hint() == RetryHint::RetryAfterRouteRefresh {
                    refresh.push(write);
                } else {
                    pending.push(write);
                }
                first_error.get_or_insert(error);
            }
            if !refresh.is_empty() {
                pending.extend(self.reroute_failed_writes(refresh).await?);
            }
            if let Some(error) =
                first_error.filter(|_| cancellation.is_some_and(CancellationToken::is_cancelled))
            {
                return Err(error);
            }
            attempts =
                future::join_all(pending.into_iter().map(|write| self.send(write, histogram)))
                    .await;
            retries += 1;
        }
        match merge_attempts(attempts, &mut response)?.into_iter().next() {
            Some((_, error)) => Err(error),
            None => Ok(response),
        }
    }

    async fn reroute_failed_writes(&self, writes: Vec<RoutedWrite>) -> Result<Vec<RoutedWrite>> {
        let regions = writes
            .iter()
            .map(|write| write_region_id(&write.request))
            .collect::<Result<Vec<_>>>()?;
        self.invalidate_routes(&regions).await?;
        let mut table_names = HashMap::new();
        for table_id in regions
            .iter()
            .map(|region| region.table_id())
            .collect::<HashSet<_>>()
        {
            // Raw metadata is only used to recover the name of the failed request's table.
            let table_info = self
                .catalog_manager
                .table_info_by_id(table_id)
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?
                .with_context(|| TableInfoNotFoundSnafu {
                    table: table_id.to_string(),
                })?;
            table_names.insert(
                table_id,
                TableName::new(
                    &table_info.catalog_name,
                    &table_info.schema_name,
                    &table_info.name,
                ),
            );
        }
        // TableId invalidation refreshes metadata and routes, but TableCache is keyed by name.
        // Use the refreshed names in case a table was renamed while the write was pending.
        let caches = table_names
            .values()
            .cloned()
            .map(CacheIdent::TableName)
            .collect::<Vec<_>>();
        self.cache_invalidator
            .invalidate(&Context::default(), &caches)
            .await?;
        let mut partitioners = HashMap::new();
        for (table_id, name) in table_names {
            let table = self
                .catalog_manager
                .table(
                    &name.catalog_name,
                    &name.schema_name,
                    &name.table_name,
                    None,
                )
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?
                .with_context(|| TableInfoNotFoundSnafu {
                    table: name.to_string(),
                })?;
            let table_info = table.table_info();
            ensure!(
                table_info.table_id() == table_id,
                UnexpectedSnafu {
                    err_msg: format!(
                        "Table {name} changed ID from {table_id} to {} while retrying writes",
                        table_info.table_id()
                    ),
                }
            );
            let partitioner = TablePartitioner::try_new(&self.partition_manager, &table_info)
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;
            partitioners.insert(table_id, (table_info, partitioner));
        }
        let mut rebuilt_requests = Vec::new();
        for write in writes {
            let request = Arc::try_unwrap(write.request).map_err(|_| {
                UnexpectedSnafu {
                    err_msg: "Write request is still shared after its RPC completed",
                }
                .build()
            })?;
            let table_id = write_region_id(&request)?.table_id();
            let (table_info, partitioner) =
                partitioners.get(&table_id).context(UnexpectedSnafu {
                    err_msg: "Refreshed write has no partition snapshot",
                })?;
            let requests = partitioner
                .rebuild_tracked_write_requests(request, table_info, write.owners)
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;
            rebuilt_requests.extend(requests);
        }
        let (requests, owners): (Vec<_>, Vec<_>) = rebuilt_requests.into_iter().unzip();
        let mut writes = self.prepare(requests).await?;
        for (write, owners) in writes.iter_mut().zip(owners) {
            write.owners = owners;
        }
        Ok(writes)
    }

    async fn prepare(&self, requests: Vec<RegionRequest>) -> Result<Vec<RoutedWrite>> {
        let regions = requests
            .iter()
            .map(write_region_id)
            .collect::<Result<Vec<_>>>()?;
        let peers = resolve_peers(&self.partition_manager, &regions)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        requests
            .into_iter()
            .zip(regions)
            .map(|(request, region)| {
                Ok(RoutedWrite {
                    peer: get_peer(&peers, region)?,
                    request: Arc::new(request),
                    owners: None,
                })
            })
            .collect()
    }

    async fn invalidate_routes(&self, regions: &[RegionId]) -> Result<()> {
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
        self.cache_invalidator
            .invalidate(&Context::default(), &caches)
            .await
    }
}

async fn resolve_peers(
    partition_manager: &PartitionRuleManager,
    regions: &[RegionId],
) -> partition::error::Result<HashMap<RegionId, PeerRef>> {
    let table_ids = regions
        .iter()
        .map(|region| region.table_id())
        .collect::<HashSet<_>>();
    let mut physical_tables = HashMap::new();
    let mut routes = HashMap::new();
    for table_id in table_ids {
        let (physical_id, route) = partition_manager
            .find_physical_table_route_with_id(table_id)
            .await?;
        physical_tables.insert(table_id, physical_id);
        routes.entry(physical_id).or_insert(route);
    }
    let mut shared_peers: HashMap<&Peer, PeerRef> = HashMap::new();
    let mut peers = HashMap::with_capacity(regions.len());
    for region in regions {
        if peers.contains_key(region) {
            continue;
        }
        let peer = physical_tables
            .get(&region.table_id())
            .and_then(|physical_id| routes.get(physical_id))
            .and_then(|route| {
                router::find_region_leader_ref(&route.region_routes, region.region_number())
            })
            .context(partition::error::FindLeaderSnafu {
                region_id: *region,
                table_id: region.table_id(),
            })?;
        let peer = shared_peers
            .entry(peer)
            .or_insert_with(|| Arc::new(peer.clone()));
        peers.insert(*region, Arc::clone(peer));
    }
    Ok(peers)
}

fn get_peer(peers: &HashMap<RegionId, PeerRef>, region: RegionId) -> Result<PeerRef> {
    peers.get(&region).cloned().context(UnexpectedSnafu {
        err_msg: "Planned write has no resolved peer",
    })
}

fn merge_attempts(
    attempts: Vec<WriteAttempt>,
    response: &mut RegionResponse,
) -> Result<Vec<(RoutedWrite, Error)>> {
    let mut failures = Vec::new();
    let mut terminal_error = None;
    for attempt in attempts {
        match attempt {
            WriteAttempt::Success(result) => merge_response(response, result),
            WriteAttempt::Failed {
                error,
                request: Some(write),
            } => failures.push((write, error)),
            WriteAttempt::Failed {
                error,
                request: None,
            } => {
                terminal_error.get_or_insert(error);
            }
        }
    }
    // A terminal error takes precedence over retained retryable failures.
    match terminal_error {
        Some(error) => Err(error),
        None => Ok(failures),
    }
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
