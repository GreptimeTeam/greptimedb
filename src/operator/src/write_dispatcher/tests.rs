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
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;
use std::time::{Duration, Instant};

use api::v1::region::InsertRequest;
use client::client_manager::NodeClients;
use common_error::ext::ErrorExt;
use common_grpc::channel_manager::ChannelConfig;
use common_meta::cache as meta_cache;
use common_meta::key::table_route::TableRouteValue;
use common_meta::node_manager::{
    Datanode, DatanodeManager, DatanodeRef, FlownodeManager, FlownodeRef,
};
use common_query::request::QueryRequest;
use common_recordbatch::SendableRecordBatchStream;
use partition::manager::PartitionRuleManager;
use tokio::sync::{Barrier, Notify};

use super::*;
use crate::test_util;

fn build_insert_request(regions: &[u32]) -> RegionRequest {
    RegionRequest {
        body: Some(Body::Inserts(InsertRequests {
            requests: regions
                .iter()
                .map(|region| InsertRequest {
                    region_id: RegionId::new(1, *region).as_u64(),
                    ..Default::default()
                })
                .collect(),
        })),
        ..Default::default()
    }
}

#[tokio::test]
async fn test_resolve_peers_batches_logical_and_physical_regions() {
    let backend = test_util::prepare_mocked_backend().await;
    let partition_manager = test_util::create_partition_rule_manager(backend.clone()).await;
    common_meta::key::TableMetadataManager::new(backend)
        .create_table_metadata(
            crate::test_util::new_test_table_info(42, "dispatcher_logical", [1, 2, 3].into_iter()),
            TableRouteValue::logical(1),
            Default::default(),
        )
        .await
        .unwrap();
    let clients = Arc::new(NodeClients::new(ChannelConfig::default()));
    let dispatcher = WriteDispatcher::new(clients, partition_manager.clone());
    let regions = [
        RegionId::new(1, 1),
        RegionId::new(1, 2),
        RegionId::new(42, 1),
        RegionId::new(42, 2),
        RegionId::new(42, 1),
    ];
    let peers = dispatcher.resolve_peers(&regions).await.unwrap();
    assert_eq!(peers.len(), 4);
    for region in regions {
        assert_eq!(
            peers[&region],
            partition_manager.find_region_leader(region).await.unwrap()
        );
    }
    assert_eq!(peers[&RegionId::new(1, 1)], peers[&RegionId::new(42, 1)]);
    let missing = RegionId::new(42, 999);
    let expected = partition_manager
        .find_region_leader(missing)
        .await
        .unwrap_err();
    let actual = dispatcher.resolve_peers(&[missing]).await.unwrap_err();
    assert_eq!(actual.status_code(), expected.status_code());
    assert_eq!(actual.retry_hint(), expected.retry_hint());
    assert!(dispatcher.resolve_peers(&[]).await.unwrap().is_empty());
}

struct GatedNodeManager {
    entered: Arc<Barrier>,
    rejected: Arc<AtomicBool>,
    completed: Arc<AtomicBool>,
    release: Arc<Notify>,
}

struct GatedDatanode {
    reject: bool,
    entered: Arc<Barrier>,
    rejected: Arc<AtomicBool>,
    completed: Arc<AtomicBool>,
    release: Arc<Notify>,
}

#[async_trait::async_trait]
impl DatanodeManager for GatedNodeManager {
    async fn datanode(&self, peer: &Peer) -> DatanodeRef {
        Arc::new(GatedDatanode {
            reject: peer.id == 1,
            entered: self.entered.clone(),
            rejected: self.rejected.clone(),
            completed: self.completed.clone(),
            release: self.release.clone(),
        })
    }
}

#[async_trait::async_trait]
impl FlownodeManager for GatedNodeManager {
    async fn flownode(&self, _peer: &Peer) -> FlownodeRef {
        unimplemented!()
    }
}

#[async_trait::async_trait]
impl Datanode for GatedDatanode {
    async fn handle(&self, _request: RegionRequest) -> Result<RegionResponse> {
        self.entered.wait().await;
        if self.reject {
            self.rejected.store(true, Ordering::SeqCst);
            return error::UnexpectedSnafu {
                err_msg: "first region rejected",
            }
            .fail();
        }
        self.release.notified().await;
        self.completed.store(true, Ordering::SeqCst);
        Ok(RegionResponse::new(1))
    }

    async fn handle_query(&self, _request: QueryRequest) -> Result<SendableRecordBatchStream> {
        unimplemented!()
    }
}

#[tokio::test]
async fn test_dispatch_sends_concurrently_and_waits_for_siblings_after_error() {
    let backend = test_util::prepare_mocked_backend().await;
    let partition_manager = test_util::create_partition_rule_manager(backend).await;
    let nodes = Arc::new(GatedNodeManager {
        entered: Arc::new(Barrier::new(2)),
        rejected: Arc::default(),
        completed: Arc::default(),
        release: Arc::default(),
    });
    let dispatcher = WriteDispatcher::new(nodes.clone(), partition_manager);
    let dispatch = dispatcher.dispatch(vec![build_insert_request(&[1, 2])]);
    tokio::pin!(dispatch);
    tokio::time::timeout(
        Duration::from_secs(1),
        futures::future::poll_fn(|cx| {
            assert!(
                dispatch.as_mut().poll(cx).is_pending(),
                "dispatch returned while a sibling remained blocked"
            );
            if nodes.rejected.load(Ordering::SeqCst) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await
    .expect("both region sends must reach the barrier concurrently");
    assert!(!nodes.completed.load(Ordering::SeqCst));
    nodes.release.notify_one();
    let error = tokio::time::timeout(Duration::from_secs(1), dispatch)
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("first region rejected"));
    assert!(nodes.completed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_resolve_peers_cache_lookup_ablation() {
    let backend = test_util::prepare_mocked_backend().await;
    test_util::create_partition_rule_manager(backend.clone()).await;
    common_meta::key::TableMetadataManager::new(backend.clone())
        .create_table_metadata(
            crate::test_util::new_test_table_info(42, "resolution_ablation", [1, 2, 3].into_iter()),
            TableRouteValue::logical(1),
            Default::default(),
        )
        .await
        .unwrap();
    // A dedicated cache label isolates counters from other concurrently running tests.
    let cache_name = "write_dispatcher_resolution_ablation_table_route";
    let routes = Arc::new(meta_cache::new_table_route_cache(
        cache_name.to_string(),
        moka::future::Cache::new(16),
        backend.clone(),
    ));
    let partitions = Arc::new(partition::cache::new_partition_info_cache(
        "write_dispatcher_resolution_ablation_partitions".to_string(),
        moka::future::Cache::new(16),
        routes.clone(),
    ));
    let partition_manager = Arc::new(PartitionRuleManager::new(backend, routes, partitions));
    let dispatcher =
        WriteDispatcher::new(Arc::new(NodeClients::default()), partition_manager.clone());
    let regions = (0..256)
        .map(|index| RegionId::new(if index % 2 == 0 { 1 } else { 42 }, index % 3 + 1))
        .collect::<Vec<_>>();
    let expected = dispatcher.resolve_peers(&regions).await.unwrap();
    let counter = common_meta::metrics::CACHE_CONTAINER_CACHE_GET.with_label_values(&[cache_name]);
    for run in 0..6 {
        // Alternate order; compare the same warm cache, routes and deterministic workload.
        let modes = if run % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        };
        for batched in modes {
            let before = counter.get();
            let started = Instant::now();
            let peers = if batched {
                dispatcher.resolve_peers(&regions).await.unwrap()
            } else {
                let mut peers = HashMap::new();
                for region in &regions {
                    peers.insert(
                        *region,
                        partition_manager.find_region_leader(*region).await.unwrap(),
                    );
                }
                peers
            };
            let elapsed = started.elapsed();
            let gets = counter.get() - before;
            assert_eq!(peers, expected);
            assert_eq!(gets, if batched { 3 } else { 384 });
            eprintln!(
                "route_resolution_ablation run={run} batched={batched} regions={} cache_gets={gets} elapsed_ns={}",
                regions.len(),
                elapsed.as_nanos()
            );
        }
    }
}
