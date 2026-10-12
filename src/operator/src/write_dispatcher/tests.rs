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

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::{Duration, Instant};

use api::v1::region::region_server::{Region, RegionServer};
use api::v1::region::{
    AlignedSchemaVersion, BulkInsertRequest, DeleteRequest, InsertRequest, RegionRequestHeader,
    RegionResponse as PbRegionResponse, bulk_insert_request,
};
use api::v1::value::ValueData;
use api::v1::{
    ArrowIpc, ColumnDataType, ColumnSchema, PartitionExprVersion, Row, Rows, Value, helper,
};
use api::v1::{ResponseHeader, Status as PbStatus};
use client::client_manager::NodeClients;
use common_error::ext::{ErrorExt, RetryHint};
use common_error::status_code::StatusCode;
use common_grpc::channel_manager::ChannelConfig;
use common_grpc::flight::{self, FlightDecoder};
use common_meta::cache::{
    self as meta_cache, LayeredCacheRegistryBuilder, TableInfoCacheRef, TableRouteCacheRef,
};
use common_meta::cache_invalidator::{CacheInvalidator, CacheInvalidatorRef, Context};
use common_meta::ddl::test_util::datanode_handler::DatanodeWatcher;
use common_meta::error::{Result, UnexpectedSnafu};
use common_meta::instruction::CacheIdent;
use common_meta::key::TableMetadataManager;
use common_meta::key::table_route::{TableRouteManager, TableRouteValue};
use common_meta::kv_backend::KvBackendRef;
use common_meta::peer::Peer;
use common_meta::test_util::{MockDatanodeHandler, MockDatanodeManager};
use common_query::request::QueryRequest;
use common_recordbatch::SendableRecordBatchStream;
use datatypes::arrow::array::Int32Array;
use datatypes::arrow::datatypes::{DataType, Field, Schema};
use datatypes::arrow::record_batch::RecordBatch;
use partition::cache::PartitionInfoCacheRef;
use partition::expr::{Operand, PartitionExpr, RestrictedOp};
use partition::manager::{PartitionRuleManager, PartitionRuleManagerRef};
use store_api::storage::RegionId;
use tokio::net::TcpListener;
use tokio::sync::{Barrier, Notify};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

use super::*;
use crate::error::Error;
use crate::test_util;

#[derive(Clone, Default)]
struct RecordingRegionService {
    requests: Arc<Mutex<Vec<RegionRequest>>>,
    failures: Arc<Mutex<HashMap<u64, VecDeque<RetryHint>>>>,
}

#[async_trait::async_trait]
impl Region for RecordingRegionService {
    async fn handle(
        &self,
        request: Request<RegionRequest>,
    ) -> std::result::Result<Response<PbRegionResponse>, Status> {
        let request = request.into_inner();
        let region_id = write_region_id(&request).unwrap().as_u64();
        self.requests.lock().unwrap().push(request);
        if let Some(hint) = self
            .failures
            .lock()
            .unwrap()
            .get_mut(&region_id)
            .and_then(VecDeque::pop_front)
        {
            let mut status = Status::unavailable("scripted write rejection");
            status.metadata_mut().insert(
                common_error::GREPTIME_DB_HEADER_ERROR_RETRY_HINT,
                hint.as_str().parse().unwrap(),
            );
            return Err(status);
        }
        Ok(Response::new(PbRegionResponse {
            header: Some(ResponseHeader {
                status: Some(PbStatus {
                    status_code: StatusCode::Success as u32,
                    ..Default::default()
                }),
            }),
            affected_rows: 1,
            ..Default::default()
        }))
    }
}

#[derive(Default)]
struct RecordingCacheInvalidator {
    calls: Mutex<Vec<Vec<CacheIdent>>>,
    inner: Option<CacheInvalidatorRef>,
    route_update: Mutex<Option<(KvBackendRef, TableRouteValue)>>,
}

#[async_trait::async_trait]
impl CacheInvalidator for RecordingCacheInvalidator {
    async fn invalidate(&self, ctx: &Context, caches: &[CacheIdent]) -> Result<()> {
        self.calls.lock().unwrap().push(caches.to_vec());
        let route_update = self.route_update.lock().unwrap().take();
        if let Some((backend, updated)) = route_update {
            let manager = TableRouteManager::new(backend.clone());
            let storage = manager.table_route_storage();
            let current = storage.get_with_raw_bytes(1).await?.unwrap();
            let (txn, _) = storage.build_update_txn(1, &current, &updated)?;
            assert!(backend.txn(txn).await?.succeeded);
        }
        if let Some(inner) = &self.inner {
            inner.invalidate(ctx, caches).await
        } else {
            UnexpectedSnafu {
                err_msg: "stop after recording route refresh",
            }
            .fail()
        }
    }

    fn invalidate_all(&self) -> Result<()> {
        // This recorder owns no cache entries.
        Ok(())
    }
}

struct RegionTestServer {
    peer: Peer,
    service: Arc<RecordingRegionService>,
    invalidator: Arc<RecordingCacheInvalidator>,
    task: JoinHandle<()>,
}

impl RegionTestServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = Peer::new(1, listener.local_addr().unwrap().to_string());
        let service = Arc::new(RecordingRegionService::default());
        let server_service = service.clone();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(RegionServer::new(server_service.as_ref().clone()))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        Self {
            peer,
            service,
            invalidator: Arc::default(),
            task,
        }
    }

    async fn create_dispatcher(&self, max_retries: usize) -> WriteDispatcher {
        let backend = test_util::prepare_mocked_backend().await;
        let partition_manager = test_util::create_partition_rule_manager(backend.clone()).await;
        update_table_leader(&backend, &self.peer).await;
        let registry = cache::build_fundamental_cache_registry(backend);
        let table_info_cache = registry.get::<TableInfoCacheRef>().unwrap();
        let clients = Arc::new(NodeClients::new(ChannelConfig::default()));
        WriteDispatcher::new(clients.clone(), partition_manager).with_retry(
            clients,
            build_retry_options(max_retries),
            table_info_cache,
            self.invalidator.clone(),
        )
    }

    fn fail_region(&self, region_number: u32, hints: impl IntoIterator<Item = RetryHint>) {
        self.service.failures.lock().unwrap().insert(
            RegionId::new(1, region_number).as_u64(),
            hints.into_iter().collect(),
        );
    }

    fn attempt_counts(&self) -> HashMap<u64, usize> {
        let mut counts = HashMap::new();
        for request in self.service.requests.lock().unwrap().iter() {
            *counts
                .entry(write_region_id(request).unwrap().as_u64())
                .or_default() += 1;
        }
        counts
    }
}

impl Drop for RegionTestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn build_retry_options(max_retries: usize) -> WriteRetryOptions {
    WriteRetryOptions {
        max_retries,
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(2),
        ..Default::default()
    }
}

fn create_routing_caches(
    backend: KvBackendRef,
) -> (
    PartitionRuleManagerRef,
    TableInfoCacheRef,
    CacheInvalidatorRef,
) {
    let registry = Arc::new(
        cache::with_default_composite_cache_registry(
            LayeredCacheRegistryBuilder::default()
                .add_cache_registry(cache::build_fundamental_cache_registry(backend.clone())),
        )
        .unwrap()
        .build(),
    );
    let partition_manager = Arc::new(PartitionRuleManager::new(
        backend.clone(),
        registry.get::<TableRouteCacheRef>().unwrap(),
        registry.get::<PartitionInfoCacheRef>().unwrap(),
    ));
    let table_info_cache = registry.get::<TableInfoCacheRef>().unwrap();
    (partition_manager, table_info_cache, registry)
}

async fn update_table_leader(backend: &KvBackendRef, peer: &Peer) {
    let manager = TableRouteManager::new(backend.clone());
    let storage = manager.table_route_storage();
    let current = storage.get_with_raw_bytes(1).await.unwrap().unwrap();
    let mut routes = current.region_routes().unwrap().clone();
    for route in &mut routes {
        route.leader_peer = Some(peer.clone());
    }
    let updated = current.update(routes).unwrap();
    let (txn, _) = storage.build_update_txn(1, &current, &updated).unwrap();
    assert!(backend.txn(txn).await.unwrap().succeeded);
}

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
async fn test_write_retry_disabled_and_enabled_ablation() {
    common_telemetry::init_default_ut_logging();
    for run in 0..3 {
        for max_retries in [0, 1] {
            let server = RegionTestServer::start().await;
            server.fail_region(1, [RetryHint::Retryable]);
            let dispatcher = server.create_dispatcher(max_retries).await;
            let result = dispatcher.dispatch(vec![build_insert_request(&[1])]).await;
            let attempts = server.attempt_counts()[&RegionId::new(1, 1).as_u64()];
            common_telemetry::info!(
                "write_retry_ablation run={run} max_retries={max_retries} success={} attempts={attempts}",
                result.is_ok()
            );
            assert_eq!(result.is_ok(), max_retries > 0);
            assert_eq!(attempts, max_retries + 1);
            assert!(server.invalidator.calls.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn test_write_retry_does_not_replay_successful_regions() {
    let server = RegionTestServer::start().await;
    server.fail_region(2, [RetryHint::Retryable]);
    let dispatcher = server.create_dispatcher(2).await;
    let response = dispatcher
        .dispatch(vec![build_insert_request(&[1, 2])])
        .await
        .unwrap();
    assert_eq!(response.affected_rows, 2);
    assert_eq!(
        server.attempt_counts(),
        HashMap::from([
            (RegionId::new(1, 1).as_u64(), 1),
            (RegionId::new(1, 2).as_u64(), 2),
        ])
    );
    assert!(server.invalidator.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_write_retry_attempt_budget_and_nonretryable_errors() {
    for (hint, expected_attempts) in [(RetryHint::Retryable, 3), (RetryHint::NonRetryable, 1)] {
        let server = RegionTestServer::start().await;
        server.fail_region(1, [hint; 4]);
        let dispatcher = server.create_dispatcher(2).await;
        let error = dispatcher
            .dispatch(vec![build_insert_request(&[1])])
            .await
            .unwrap_err();
        // The dispatcher uses the explicit client hint for its attempt budget.
        // The existing common-meta conversion promotes Unavailable to RetryLater.
        assert_eq!(error.retry_hint(), RetryHint::Retryable);
        assert_eq!(
            server.attempt_counts()[&RegionId::new(1, 1).as_u64()],
            expected_attempts
        );
        assert!(server.invalidator.calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn test_write_retry_refreshes_routes_once_per_failed_round() {
    let server = RegionTestServer::start().await;
    for region in [1, 2] {
        server.fail_region(region, [RetryHint::RetryAfterRouteRefresh]);
    }
    let dispatcher = server.create_dispatcher(2).await;
    let error = dispatcher
        .dispatch(vec![build_insert_request(&[1, 2])])
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("stop after recording route refresh")
    );
    let calls = server.invalidator.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0], vec![CacheIdent::TableId(1)]);
    assert_eq!(server.service.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn test_finish_dispatch_retries_initial_failures_in_one_round() {
    let server = RegionTestServer::start().await;
    for region in [2, 3] {
        server.fail_region(region, [RetryHint::RetryAfterRouteRefresh]);
    }
    let backend = test_util::prepare_mocked_backend().await;
    test_util::create_partition_rule_manager(backend.clone()).await;
    update_table_leader(&backend, &server.peer).await;
    let (partition_manager, table_info_cache, registry) = create_routing_caches(backend);
    let table_info = table_info_cache.get(1).await.unwrap().unwrap();
    let (_, versions) = partition_manager
        .find_table_partition_rule(&table_info)
        .await
        .unwrap();
    let invalidator = Arc::new(RecordingCacheInvalidator {
        inner: Some(registry),
        ..Default::default()
    });
    let clients = Arc::new(NodeClients::new(ChannelConfig::default()));
    let dispatcher = WriteDispatcher::new(clients.clone(), partition_manager).with_retry(
        clients,
        build_retry_options(2),
        table_info_cache,
        invalidator.clone(),
    );
    let mut outcomes = Vec::new();
    for (region, value) in [(1, 101), (2, 11), (3, 1)] {
        let request =
            build_partitioned_request(WriteKind::Bulk, region, versions[&region], &[value]);
        outcomes.push(
            dispatcher
                .dispatch_initial(server.peer.clone(), request, None)
                .await,
        );
    }
    assert!(invalidator.calls.lock().unwrap().is_empty());
    assert_eq!(
        server.attempt_counts(),
        HashMap::from([
            (RegionId::new(1, 1).as_u64(), 1),
            (RegionId::new(1, 2).as_u64(), 1),
            (RegionId::new(1, 3).as_u64(), 1),
        ])
    );
    let response = dispatcher.finish_dispatch(outcomes, None).await.unwrap();
    assert_eq!(response.affected_rows, 3);
    assert_eq!(
        server.attempt_counts(),
        HashMap::from([
            (RegionId::new(1, 1).as_u64(), 1),
            (RegionId::new(1, 2).as_u64(), 2),
            (RegionId::new(1, 3).as_u64(), 2),
        ])
    );
    assert_eq!(
        *invalidator.calls.lock().unwrap(),
        vec![vec![CacheIdent::TableId(1)]]
    );
}

#[tokio::test]
async fn test_write_retry_refreshes_stale_route_and_sends_only_failed_region_to_new_leader() {
    let old_server = RegionTestServer::start().await;
    let new_server = RegionTestServer::start().await;
    old_server.fail_region(2, [RetryHint::RetryAfterRouteRefresh]);
    let backend = test_util::prepare_mocked_backend().await;
    // Reuse the operator fixture to populate table metadata and partition rules.
    test_util::create_partition_rule_manager(backend.clone()).await;
    update_table_leader(&backend, &old_server.peer).await;

    let (partition_manager, table_info_cache, registry) = create_routing_caches(backend.clone());
    let failed_region = RegionId::new(1, 2);
    assert_eq!(
        partition_manager
            .find_region_leader(failed_region)
            .await
            .unwrap(),
        old_server.peer
    );
    let table_info = table_info_cache.get(1).await.unwrap().unwrap();
    let (_, versions) = partition_manager
        .find_table_partition_rule(&table_info)
        .await
        .unwrap();

    // Metadata has migrated while the frontend still holds the old route.
    update_table_leader(&backend, &new_server.peer).await;
    assert_eq!(
        partition_manager
            .find_region_leader(failed_region)
            .await
            .unwrap(),
        old_server.peer
    );

    let invalidator = Arc::new(RecordingCacheInvalidator {
        calls: Mutex::default(),
        inner: Some(registry),
        ..Default::default()
    });
    let clients = Arc::new(NodeClients::new(ChannelConfig::default()));
    let dispatcher = WriteDispatcher::new(clients.clone(), partition_manager).with_retry(
        clients,
        build_retry_options(2),
        table_info_cache,
        invalidator.clone(),
    );
    let mut request = build_insert_request(&[1, 2]);
    if let Some(Body::Inserts(inserts)) = &mut request.body {
        for insert in &mut inserts.requests {
            insert.partition_expr_version = versions
                [&RegionId::from_u64(insert.region_id).region_number()]
                .map(|value| api::v1::PartitionExprVersion { value });
        }
    }
    let response = dispatcher.dispatch(vec![request]).await.unwrap();
    assert_eq!(response.affected_rows, 2);
    assert_eq!(
        old_server.attempt_counts(),
        HashMap::from([
            (RegionId::new(1, 1).as_u64(), 1),
            (failed_region.as_u64(), 1),
        ])
    );
    assert_eq!(
        new_server.attempt_counts(),
        HashMap::from([(failed_region.as_u64(), 1)])
    );
    assert_eq!(
        *invalidator.calls.lock().unwrap(),
        vec![vec![CacheIdent::TableId(1)]]
    );
}

#[derive(Clone, Copy, Debug)]
enum WriteKind {
    Insert,
    Delete,
    Bulk,
}

fn build_partitioned_request(
    kind: WriteKind,
    region: u32,
    version: Option<u64>,
    values: &[i32],
) -> RegionRequest {
    let region_id = RegionId::new(1, region).as_u64();
    let partition_expr_version = version.map(|value| PartitionExprVersion { value });
    let rows = Rows {
        schema: vec![ColumnSchema {
            column_name: "a".to_string(),
            datatype: ColumnDataType::Int32 as i32,
            ..Default::default()
        }],
        rows: values
            .iter()
            .map(|value| Row {
                values: vec![Value {
                    value_data: Some(ValueData::I32Value(*value)),
                }],
            })
            .collect(),
    };
    let body = match kind {
        WriteKind::Insert => Body::Inserts(InsertRequests {
            requests: vec![InsertRequest {
                region_id,
                partition_expr_version,
                rows: Some(rows),
                skip_wal: true,
            }],
        }),
        WriteKind::Delete => Body::Deletes(DeleteRequests {
            requests: vec![DeleteRequest {
                region_id,
                partition_expr_version,
                rows: Some(rows),
            }],
        }),
        WriteKind::Bulk => {
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                vec![Arc::new(Int32Array::from(values.to_vec()))],
            )
            .unwrap();
            let (schema, data_header, payload) = flight::record_batch_to_ipc(batch).unwrap();
            Body::BulkInsert(BulkInsertRequest {
                region_id,
                partition_expr_version,
                skip_wal: true,
                aligned_schema_version: Some(AlignedSchemaVersion {
                    schema_version: 123,
                }),
                body: Some(bulk_insert_request::Body::ArrowIpc(ArrowIpc {
                    schema,
                    data_header,
                    payload,
                })),
            })
        }
    };
    RegionRequest {
        header: None,
        body: Some(body),
    }
}

fn request_values(request: &RegionRequest) -> (u32, Option<u64>, Vec<i32>) {
    let region = write_region_id(request).unwrap().region_number();
    let (version, rows) = match request.body.as_ref().unwrap() {
        Body::Inserts(inserts) => {
            assert!(inserts.requests[0].skip_wal);
            (
                inserts.requests[0].partition_expr_version,
                inserts.requests[0].rows.as_ref().unwrap(),
            )
        }
        Body::Deletes(deletes) => (
            deletes.requests[0].partition_expr_version,
            deletes.requests[0].rows.as_ref().unwrap(),
        ),
        Body::BulkInsert(bulk) => {
            assert!(bulk.skip_wal);
            assert_eq!(bulk.aligned_schema_version, None);
            let Some(bulk_insert_request::Body::ArrowIpc(ipc)) = &bulk.body else {
                panic!("expected IPC")
            };
            let batch = FlightDecoder::try_from_schema_bytes(&ipc.schema)
                .unwrap()
                .try_decode_record_batch(&ipc.data_header, &ipc.payload)
                .unwrap();
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            return (
                region,
                bulk.partition_expr_version.map(|v| v.value),
                values.values().to_vec(),
            );
        }
        _ => panic!("expected write request"),
    };
    (
        region,
        version.map(|v| v.value),
        rows.rows
            .iter()
            .map(|row| {
                let Some(ValueData::I32Value(value)) = row.values[0].value_data else {
                    panic!("expected int32")
                };
                value
            })
            .collect(),
    )
}

#[tokio::test]
async fn test_write_retry_repartitions_only_failed_rows_after_route_refresh() {
    for kind in [WriteKind::Insert, WriteKind::Delete, WriteKind::Bulk] {
        let old_server = RegionTestServer::start().await;
        let new_server = RegionTestServer::start().await;
        old_server.fail_region(2, [RetryHint::RetryAfterRouteRefresh]);
        let backend = test_util::prepare_mocked_backend().await;
        test_util::create_partition_rule_manager(backend.clone()).await;
        update_table_leader(&backend, &old_server.peer).await;
        let (partition_manager, table_info_cache, registry) =
            create_routing_caches(backend.clone());
        let table_info = table_info_cache.get(1).await.unwrap().unwrap();
        let (_, old_versions) = partition_manager
            .find_table_partition_rule(&table_info)
            .await
            .unwrap();
        let manager = TableRouteManager::new(backend.clone());
        let current = manager
            .table_route_storage()
            .get_with_raw_bytes(1)
            .await
            .unwrap()
            .unwrap();
        let old_routes = current.region_routes().unwrap();
        let replaced = old_routes
            .iter()
            .find(|route| route.region.id.region_number() == 2)
            .unwrap();
        let mut routes = old_routes
            .iter()
            .filter(|route| route.region.id.region_number() != 2)
            .cloned()
            .collect::<Vec<_>>();
        for (region, lower, upper) in [(4, 10, 30), (5, 30, 50)] {
            let mut route = replaced.clone();
            route.region.id = RegionId::new(1, region);
            route.leader_peer = Some(new_server.peer.clone());
            route.region.partition_expr = PartitionExpr::new(
                Operand::Expr(PartitionExpr::new(
                    Operand::Column("a".to_string()),
                    RestrictedOp::GtEq,
                    Operand::Value(datatypes::value::Value::Int32(lower)),
                )),
                RestrictedOp::And,
                Operand::Expr(PartitionExpr::new(
                    Operand::Column("a".to_string()),
                    RestrictedOp::Lt,
                    Operand::Value(datatypes::value::Value::Int32(upper)),
                )),
            )
            .as_json_str()
            .unwrap();
            routes.push(route);
        }
        let invalidator = Arc::new(RecordingCacheInvalidator {
            calls: Mutex::default(),
            inner: Some(registry),
            route_update: Mutex::new(Some((backend, current.update(routes).unwrap()))),
        });
        let clients = Arc::new(NodeClients::new(ChannelConfig::default()));
        let dispatcher = WriteDispatcher::new(clients.clone(), partition_manager.clone())
            .with_retry(
                clients,
                build_retry_options(2),
                table_info_cache,
                invalidator.clone(),
            );
        let response = dispatcher
            .dispatch(vec![
                build_partitioned_request(kind, 1, old_versions[&1], &[70]),
                build_partitioned_request(kind, 2, old_versions[&2], &[20, 40]),
            ])
            .await
            .unwrap();
        assert_eq!(response.affected_rows, 3, "{kind:?}");
        assert_eq!(
            old_server.attempt_counts(),
            HashMap::from([
                (RegionId::new(1, 1).as_u64(), 1),
                (RegionId::new(1, 2).as_u64(), 1),
            ]),
            "{kind:?}"
        );
        let (_, versions) = partition_manager
            .find_table_partition_rule(&table_info)
            .await
            .unwrap();
        let mut writes = new_server
            .service
            .requests
            .lock()
            .unwrap()
            .iter()
            .map(request_values)
            .collect::<Vec<_>>();
        writes.sort_by_key(|write| write.0);
        assert_eq!(
            writes,
            vec![(4, versions[&4], vec![20]), (5, versions[&5], vec![40])],
            "{kind:?}"
        );
        assert_eq!(
            *invalidator.calls.lock().unwrap(),
            vec![vec![CacheIdent::TableId(1)]]
        );
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

#[tokio::test]
async fn test_with_retry_can_disable_previously_enabled_retries() {
    let server = RegionTestServer::start().await;
    let dispatcher = server.create_dispatcher(1).await;
    let remote = dispatcher.remote.as_ref().unwrap();
    let clients = remote.clients.clone();
    let table_info_cache = remote.table_info_cache.clone();
    let cache_invalidator = remote.cache_invalidator.clone();
    let dispatcher = dispatcher.with_retry(
        clients,
        build_retry_options(0),
        table_info_cache,
        cache_invalidator,
    );
    assert!(dispatcher.remote.is_none());
    server.fail_region(1, [RetryHint::Retryable]);
    assert!(
        dispatcher
            .dispatch(vec![build_insert_request(&[1])])
            .await
            .is_err()
    );
    assert_eq!(server.attempt_counts()[&RegionId::new(1, 1).as_u64()], 1);
}
