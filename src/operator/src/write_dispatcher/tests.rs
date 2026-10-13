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

use api::v1::region::{
    BulkInsertRequest, DeleteRequest, InsertRequest, RegionRequestHeader, bulk_insert_request,
};
use api::v1::{
    ArrowIpc, ColumnDataType, PartitionExprVersion, Row, Rows, helper, value::ValueData,
};
use client::client_manager::NodeClients;
use common_error::ext::ErrorExt;
use common_grpc::channel_manager::ChannelConfig;
use common_meta::cache as meta_cache;
use common_meta::ddl::test_util::datanode_handler::DatanodeWatcher;
use common_meta::key::TableMetadataManager;
use common_meta::key::table_route::TableRouteValue;
use common_meta::test_util::{MockDatanodeHandler, MockDatanodeManager};
use common_query::request::QueryRequest;
use common_recordbatch::SendableRecordBatchStream;
use partition::manager::PartitionRuleManager;
use tokio::sync::{Barrier, Notify};

use super::*;
use crate::error::Error;
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
    TableMetadataManager::new(backend)
        .create_table_metadata(
            test_util::new_test_table_info(42, "dispatcher_logical", [1, 2, 3].into_iter()),
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

#[tokio::test]
async fn test_dispatch_preserves_write_payloads_and_envelopes() {
    let backend = test_util::prepare_mocked_backend().await;
    let partition_manager = test_util::create_partition_rule_manager(backend.clone()).await;
    TableMetadataManager::new(backend)
        .create_table_metadata(
            test_util::new_test_table_info(42, "dispatcher_logical", [1, 2, 3].into_iter()),
            TableRouteValue::logical(1),
            Default::default(),
        )
        .await
        .unwrap();
    let (sender, mut received) = tokio::sync::mpsc::channel(8);
    let watcher = DatanodeWatcher::new(sender).with_handler(|_, _| Ok(RegionResponse::new(1)));
    let dispatcher = WriteDispatcher::new(
        Arc::new(MockDatanodeManager::new(watcher)),
        partition_manager,
    );
    // Physical and logical regions share a peer but retain distinct region IDs.
    let regions = [RegionId::new(1, 1), RegionId::new(42, 1)];
    let rows = Rows {
        schema: vec![helper::tag_column_schema("a", ColumnDataType::Int32)],
        rows: vec![Row {
            values: vec![ValueData::I32Value(7).into()],
        }],
    };
    let version = Some(PartitionExprVersion { value: 17 });
    let bodies = [
        Body::Inserts(InsertRequests {
            requests: regions
                .iter()
                .map(|region| InsertRequest {
                    region_id: region.as_u64(),
                    rows: Some(rows.clone()),
                    partition_expr_version: version,
                    skip_wal: true,
                })
                .collect(),
        }),
        Body::Deletes(DeleteRequests {
            requests: regions
                .iter()
                .map(|region| DeleteRequest {
                    region_id: region.as_u64(),
                    rows: Some(rows.clone()),
                    partition_expr_version: version,
                })
                .collect(),
        }),
    ];
    for body in bodies {
        let requests = ["first", "second"].map(|dbname| RegionRequest {
            header: Some(RegionRequestHeader {
                dbname: dbname.to_string(),
                ..Default::default()
            }),
            body: Some(body.clone()),
        });
        let writes = dispatcher.prepare(requests.to_vec()).await.unwrap();
        assert!(received.try_recv().is_err());
        let response = dispatcher.dispatch_prepared(writes, None).await.unwrap();
        assert_eq!(response.affected_rows, 2);
        for request in requests {
            assert_eq!(received.try_recv().unwrap(), (Peer::new(1, ""), request));
        }
        assert!(received.try_recv().is_err());
    }
    let requests = regions.map(|region| RegionRequest {
        header: Some(RegionRequestHeader {
            dbname: "bulk".to_string(),
            ..Default::default()
        }),
        body: Some(Body::BulkInsert(BulkInsertRequest {
            region_id: region.as_u64(),
            partition_expr_version: version,
            aligned_schema_version: None,
            skip_wal: true,
            body: Some(bulk_insert_request::Body::ArrowIpc(ArrowIpc {
                schema: "schema".into(),
                data_header: "header".into(),
                payload: "payload".into(),
            })),
        })),
    });
    let response = dispatcher.dispatch(requests.to_vec()).await.unwrap();
    assert_eq!(response.affected_rows, 2);
    for request in requests {
        assert_eq!(received.try_recv().unwrap(), (Peer::new(1, ""), request));
    }
    assert!(received.try_recv().is_err());

    let error = dispatcher
        .prepare(vec![build_insert_request(&[1, 999])])
        .await
        .unwrap_err();
    assert!(matches!(error, Error::FindRegionLeader { .. }));
    assert!(received.try_recv().is_err());
}

#[derive(Clone)]
struct GatedDatanodeHandler {
    entered: Arc<Barrier>,
    rejected: Arc<AtomicBool>,
    completed: Arc<AtomicBool>,
    release: Arc<Notify>,
}

#[async_trait::async_trait]
impl MockDatanodeHandler for GatedDatanodeHandler {
    async fn handle(&self, peer: &Peer, _request: RegionRequest) -> Result<RegionResponse> {
        self.entered.wait().await;
        if peer.id == 1 {
            self.rejected.store(true, Ordering::SeqCst);
            return UnexpectedSnafu {
                err_msg: "first region rejected",
            }
            .fail();
        }
        self.release.notified().await;
        self.completed.store(true, Ordering::SeqCst);
        Ok(RegionResponse::new(1))
    }

    async fn handle_query(
        &self,
        _peer: &Peer,
        _request: QueryRequest,
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!()
    }
}

#[tokio::test]
async fn test_dispatch_sends_concurrently_and_waits_for_siblings_after_error() {
    let backend = test_util::prepare_mocked_backend().await;
    let partition_manager = test_util::create_partition_rule_manager(backend).await;
    let handler = GatedDatanodeHandler {
        entered: Arc::new(Barrier::new(2)),
        rejected: Arc::default(),
        completed: Arc::default(),
        release: Arc::default(),
    };
    let dispatcher = WriteDispatcher::new(
        Arc::new(MockDatanodeManager::new(handler.clone())),
        partition_manager,
    );
    let dispatch = dispatcher.dispatch(vec![build_insert_request(&[1, 2])]);
    tokio::pin!(dispatch);
    tokio::time::timeout(
        Duration::from_secs(1),
        futures::future::poll_fn(|cx| {
            assert!(
                dispatch.as_mut().poll(cx).is_pending(),
                "dispatch returned while a sibling remained blocked"
            );
            if handler.rejected.load(Ordering::SeqCst) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await
    .expect("both region sends must reach the barrier concurrently");
    assert!(!handler.completed.load(Ordering::SeqCst));
    handler.release.notify_one();
    let error = tokio::time::timeout(Duration::from_secs(1), dispatch)
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("first region rejected"));
    assert!(handler.completed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_resolve_peers_cache_lookup_ablation() {
    common_telemetry::init_default_ut_logging();
    let backend = test_util::prepare_mocked_backend().await;
    test_util::create_partition_rule_manager(backend.clone()).await;
    TableMetadataManager::new(backend.clone())
        .create_table_metadata(
            test_util::new_test_table_info(42, "resolution_ablation", [1, 2, 3].into_iter()),
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
            ["delete", "insert", "batched"]
        } else {
            ["batched", "insert", "delete"]
        };
        for mode in modes {
            let before = counter.get();
            let started = Instant::now();
            let peers = if mode == "batched" {
                dispatcher.resolve_peers(&regions).await.unwrap()
            } else {
                let mut peers = HashMap::new();
                for region in &regions {
                    if mode == "insert" && peers.contains_key(region) {
                        continue;
                    }
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
            let expected_gets = match mode {
                "delete" => 384,
                "insert" => 9,
                "batched" => 3,
                _ => unreachable!(),
            };
            assert_eq!(gets, expected_gets);
            common_telemetry::info!(
                "route_resolution_ablation run={run} mode={mode} regions={} cache_gets={gets} elapsed_ns={}",
                regions.len(),
                elapsed.as_nanos()
            );
        }
    }
}
