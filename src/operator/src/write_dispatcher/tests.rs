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
    AlignedSchemaVersion, BulkInsertRequest, DeleteRequest, DeleteRequests, InsertRequest,
    InsertRequests, RegionRequestHeader, RegionResponse as PbRegionResponse, bulk_insert_request,
};
use api::v1::value::ValueData;
use api::v1::{
    ArrowIpc, ColumnDataType, ColumnSchema, PartitionExprVersion, Row, Rows, Value, helper,
};
use api::v1::{ResponseHeader, Status as PbStatus};
use catalog::information_schema::NoopInformationExtension;
use catalog::kvbackend::KvBackendCatalogManagerBuilder;
use client::client_manager::NodeClients;
use common_error::ext::{ErrorExt, RetryHint};
use common_error::status_code::StatusCode;
use common_grpc::channel_manager::ChannelConfig;
use common_grpc::flight::{self, FlightDecoder};
use common_meta::cache::{
    self as meta_cache, LayeredCacheRegistryBuilder, LayeredCacheRegistryRef, TableInfoCacheRef,
    TableRouteCacheRef,
};
use common_meta::cache_invalidator::{CacheInvalidator, CacheInvalidatorRef, Context};
use common_meta::ddl::test_util::datanode_handler::DatanodeWatcher;
use common_meta::error::{Result, UnexpectedSnafu};
use common_meta::instruction::CacheIdent;
use common_meta::key::TableMetadataManager;
use common_meta::key::table_route::{TableRouteManager, TableRouteValue};
use common_meta::kv_backend::KvBackendRef;
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::peer::Peer;
use common_meta::rpc::router::{self, RegionRoute};
use common_meta::test_util::{MockDatanodeHandler, MockDatanodeManager};
use common_query::request::QueryRequest;
use common_recordbatch::SendableRecordBatchStream;
use datatypes::arrow::array::{BinaryArray, Int32Array, StringArray};
use datatypes::arrow::datatypes::{DataType, Field, Schema};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema;
use datatypes::value::{self, ValueRef};
use mito_codec::row_converter::sparse;
use mito_codec::row_converter::{PrimaryKeyCodec, SparsePrimaryKeyCodec};
use partition::cache::PartitionInfoCacheRef;
use partition::expr::{self, Operand, PartitionExpr, RestrictedOp};
use partition::manager::{PartitionRuleManager, PartitionRuleManagerRef};
use store_api::metric_engine_consts;
use store_api::storage::{RegionId, consts};
use tokio::net::TcpListener;
use tokio::sync::{Barrier, Notify, Semaphore};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Request, Response, Status};

use super::*;
use crate::error::Error;
use crate::test_util;

#[derive(Clone, Default)]
struct RecordingRegionService {
    request_sizes: Arc<Mutex<Vec<usize>>>,
    rpc_rejections: Arc<Mutex<VecDeque<Status>>>,
    requests: Arc<Mutex<Vec<RegionRequest>>>,
    rejections: Arc<Mutex<HashMap<u64, VecDeque<Status>>>>,
    release: Arc<Mutex<Option<Arc<Semaphore>>>>,
}

#[async_trait::async_trait]
impl Region for RecordingRegionService {
    async fn handle(
        &self,
        request: Request<RegionRequest>,
    ) -> std::result::Result<Response<PbRegionResponse>, Status> {
        let request = request.into_inner();
        if !matches!(&request.body, Some(Body::Inserts(_) | Body::Deletes(_))) {
            return self.handle_entry(request).await;
        }
        let accepts_partial = request
            .header
            .as_ref()
            .and_then(|header| header.query_context.as_ref())
            .and_then(|context| context.extensions.get(api::region::ACCEPT_PARTIAL_RESULT))
            .is_some_and(|value| value == "true");
        let entries = split_request(request);
        let return_tonic_error = !accepts_partial || entries.len() == 1;
        self.request_sizes.lock().unwrap().push(entries.len());
        if let Some(status) = self.rpc_rejections.lock().unwrap().pop_front() {
            self.requests.lock().unwrap().extend(entries);
            return Err(status);
        }
        let mut response = PbRegionResponse {
            header: Some(ResponseHeader {
                status: Some(PbStatus::default()),
            }),
            ..Default::default()
        };
        let results =
            future::join_all(entries.into_iter().map(|entry| self.handle_entry(entry))).await;
        for (index, result) in results.into_iter().enumerate() {
            match result {
                Ok(result) => response.affected_rows += result.into_inner().affected_rows,
                Err(status) => {
                    if return_tonic_error {
                        return Err(status);
                    }
                    let error = client::Error::from(status);
                    response
                        .failures
                        .push(api::v1::region::RegionRequestFailure {
                            request_index: index as u32,
                            status: Some(PbStatus {
                                status_code: error.status_code() as u32,
                                err_msg: error.output_msg(),
                                retry_hint: error.retry_hint().as_str().to_string(),
                            }),
                        });
                }
            }
        }
        if !response.failures.is_empty() {
            response
                .header
                .as_mut()
                .unwrap()
                .status
                .as_mut()
                .unwrap()
                .status_code = StatusCode::PartialFailure as u32;
        }
        Ok(Response::new(response))
    }
}

impl RecordingRegionService {
    async fn handle_entry(
        &self,
        request: RegionRequest,
    ) -> std::result::Result<Response<PbRegionResponse>, Status> {
        let region_id = write_region_id(&request).unwrap().as_u64();
        self.requests.lock().unwrap().push(request);
        let release = self.release.lock().unwrap().clone();
        if let Some(release) = release {
            release.acquire().await.unwrap().forget();
        }
        if let Some(status) = self
            .rejections
            .lock()
            .unwrap()
            .get_mut(&region_id)
            .and_then(VecDeque::pop_front)
        {
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
    rename_and_recreate: Mutex<Option<KvBackendRef>>,
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
        let rename_and_recreate = if caches
            .iter()
            .any(|cache| matches!(cache, CacheIdent::TableName(_)))
        {
            self.rename_and_recreate.lock().unwrap().take()
        } else {
            None
        };
        if let Some(backend) = rename_and_recreate {
            // Reuse the name after the retry has recovered it from the original table ID.
            let metadata = TableMetadataManager::new(backend);
            let current = metadata.table_info_manager().get(1).await?.unwrap();
            metadata
                .rename_table(&current, "renamed_table".to_string())
                .await?;
            let mut replacement = current.table_info.clone();
            replacement.ident.table_id = 42;
            let route = metadata
                .table_route_manager()
                .table_route_storage()
                .get_with_raw_bytes(1)
                .await?
                .unwrap();
            let mut routes = route.region_routes().unwrap().clone();
            for route in &mut routes {
                route.region.id = RegionId::new(42, route.region.id.region_number());
            }
            metadata
                .create_table_metadata(
                    replacement,
                    TableRouteValue::physical(routes),
                    Default::default(),
                )
                .await?;
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
        test_util::create_partition_rule_manager(backend.clone()).await;
        update_table_leader(&backend, &self.peer).await;
        let (partition_manager, _, _, catalog_manager) = create_routing_caches(backend);
        let clients = Arc::new(NodeClients::new(ChannelConfig::default()));
        WriteDispatcher::new(clients, partition_manager).with_retry(
            build_retry_options(max_retries),
            catalog_manager,
            self.invalidator.clone(),
        )
    }

    fn fail_region(&self, region_number: u32, hints: impl IntoIterator<Item = RetryHint>) {
        let region_id = RegionId::new(1, region_number).as_u64();
        self.service.rejections.lock().unwrap().insert(
            region_id,
            hints
                .into_iter()
                .map(|hint| {
                    let code = match hint {
                        RetryHint::Retryable => StatusCode::RegionBusy,
                        RetryHint::RetryAfterRouteRefresh => StatusCode::Cancelled,
                        RetryHint::NonRetryable => StatusCode::InvalidArguments,
                    };
                    let mut status = Status::new(
                        common_error::status_code::status_to_tonic_code(code),
                        format!("region {region_id} rejected the write"),
                    );
                    status.metadata_mut().insert(
                        common_error::GREPTIME_DB_HEADER_ERROR_CODE,
                        (code as u32).to_string().parse().unwrap(),
                    );
                    status.metadata_mut().insert(
                        common_error::GREPTIME_DB_HEADER_ERROR_RETRY_HINT,
                        hint.as_str().parse().unwrap(),
                    );
                    status
                })
                .collect(),
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
    LayeredCacheRegistryRef,
    CatalogManagerRef,
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
    let catalog_manager = KvBackendCatalogManagerBuilder::new(
        Arc::new(NoopInformationExtension),
        backend,
        registry.clone(),
    )
    .build();
    (
        partition_manager,
        table_info_cache,
        registry,
        catalog_manager,
    )
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
async fn test_direct_partial_progress_with_and_without_retries() {
    for max_retries in [0, 1] {
        let server = RegionTestServer::start().await;
        server.fail_region(1, [RetryHint::Retryable; 2]);
        let dispatcher = server.create_dispatcher(max_retries).await;
        let mut request = build_insert_request(&[1, 2]);
        if let Some(Body::Inserts(inserts)) = &mut request.body {
            for insert in &mut inserts.requests {
                insert.rows = Some(Rows {
                    rows: vec![Row::default()],
                    ..Default::default()
                });
            }
        }
        let writes = dispatcher.prepare(vec![request]).await.unwrap();
        let progress = Arc::new(WriteProgress::default());
        assert!(
            dispatcher
                .execute_with_progress(writes, None, None, Some(progress.clone()))
                .await
                .is_err()
        );
        assert_eq!(progress.written(), 1);
        assert_eq!(server.attempt_counts()[&RegionId::new(1, 2).as_u64()], 1);
    }
}

#[tokio::test]
async fn test_shared_batch_partial_progress_credits_only_its_owners() {
    for max_retries in [0, 1] {
        let server = RegionTestServer::start().await;
        server.fail_region(1, [RetryHint::Retryable; 2]);
        let dispatcher = server.create_dispatcher(max_retries).await;
        let first = Arc::new(WriteProgress::default());
        let second = Arc::new(WriteProgress::default());
        let writes = vec![
            (
                build_partitioned_request(WriteKind::BulkInsert, 1, None, &[70]),
                WriteAttribution::new(1, Some(first.clone())),
            ),
            (
                build_partitioned_request(WriteKind::BulkInsert, 2, None, &[20]),
                WriteAttribution::new(1, Some(second.clone())),
            ),
        ];
        assert!(dispatcher.dispatch_attributed(writes, None).await.is_err());
        assert_eq!(first.written(), 0);
        assert_eq!(second.written(), 1);
        assert_eq!(server.attempt_counts()[&RegionId::new(1, 2).as_u64()], 1);
    }
}

#[tokio::test]
async fn test_write_retry_disabled_and_enabled_ablation() {
    common_telemetry::init_default_ut_logging();
    for regions in [vec![1], vec![1, 2]] {
        for max_retries in [0, 1] {
            let server = RegionTestServer::start().await;
            server.fail_region(1, [RetryHint::Retryable]);
            let dispatcher = server.create_dispatcher(max_retries).await;
            let result = dispatcher
                .dispatch(vec![build_insert_request(&regions)])
                .await;
            let attempts = server.attempt_counts()[&RegionId::new(1, 1).as_u64()];
            common_telemetry::info!(
                "write_retry_ablation regions={regions:?} max_retries={max_retries} success={} attempts={attempts}",
                result.is_ok()
            );
            if max_retries == 0 {
                let error = result.unwrap_err();
                assert_eq!(error.status_code(), StatusCode::RegionBusy);
                assert_eq!(error.retry_hint(), RetryHint::Retryable);
            } else {
                assert_eq!(result.unwrap().affected_rows, regions.len());
            }
            assert_eq!(attempts, max_retries + 1);
            for request in server.service.requests.lock().unwrap().iter() {
                let accepts_partial = request
                    .header
                    .as_ref()
                    .and_then(|header| header.query_context.as_ref())
                    .and_then(|context| context.extensions.get(api::region::ACCEPT_PARTIAL_RESULT))
                    .is_some_and(|value| value == "true");
                assert_eq!(accepts_partial, max_retries > 0);
            }
            assert!(server.invalidator.calls.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn test_write_retry_does_not_replay_successful_regions() {
    for regions in [vec![1, 2], vec![1, 1, 1]] {
        let server = RegionTestServer::start().await;
        server.fail_region(1, [RetryHint::Retryable]);
        let dispatcher = server.create_dispatcher(2).await;
        let response = dispatcher
            .dispatch(vec![build_insert_request(&regions)])
            .await
            .unwrap();
        assert_eq!(response.affected_rows, regions.len());
        let mut expected = HashMap::new();
        for region in &regions {
            *expected
                .entry(RegionId::new(1, *region).as_u64())
                .or_insert(0) += 1;
        }
        *expected.get_mut(&RegionId::new(1, 1).as_u64()).unwrap() += 1;
        assert_eq!(server.attempt_counts(), expected);
        assert_eq!(
            *server.service.request_sizes.lock().unwrap(),
            vec![regions.len(), 1]
        );
        assert!(server.invalidator.calls.lock().unwrap().is_empty());
    }
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
        assert_eq!(error.retry_hint(), hint);
        assert_eq!(
            server.attempt_counts()[&RegionId::new(1, 1).as_u64()],
            expected_attempts
        );
        assert!(server.invalidator.calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn test_write_retry_transport_errors() {
    for code in [Code::Unavailable, Code::Cancelled, Code::DeadlineExceeded] {
        for hint in [
            None,
            Some(RetryHint::NonRetryable),
            Some(RetryHint::Retryable),
            Some(RetryHint::RetryAfterRouteRefresh),
        ] {
            let server = RegionTestServer::start().await;
            let region_id = RegionId::new(1, 1).as_u64();
            let mut status = Status::new(code, "write failed");
            if let Some(hint) = hint {
                status.metadata_mut().insert(
                    common_error::GREPTIME_DB_HEADER_ERROR_RETRY_HINT,
                    hint.as_str().parse().unwrap(),
                );
            }
            server
                .service
                .rpc_rejections
                .lock()
                .unwrap()
                .push_back(status);
            let dispatcher = server.create_dispatcher(2).await;
            let attempt = dispatcher
                .send(
                    Arc::new(server.peer.clone()),
                    build_insert_request(&[1]),
                    None,
                )
                .await;
            let WriteAttempt::Failed { error, request } = attempt else {
                panic!("expected a failed write");
            };
            assert!(request.is_some(), "{code:?}, {hint:?}");
            assert_eq!(
                error.retry_hint(),
                hint.filter(|hint| hint.is_retryable())
                    .unwrap_or(RetryHint::Retryable)
            );
            assert_eq!(server.attempt_counts(), HashMap::from([(region_id, 1)]));
            assert!(server.invalidator.calls.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn test_write_retry_transport_attempt_budget() {
    for code in [Code::Unavailable, Code::Cancelled, Code::DeadlineExceeded] {
        for failures in [1, 3] {
            let server = RegionTestServer::start().await;
            let region_id = RegionId::new(1, 1).as_u64();
            server
                .service
                .rpc_rejections
                .lock()
                .unwrap()
                .extend((0..failures).map(|_| Status::new(code, "write failed")));
            let dispatcher = server.create_dispatcher(2).await;
            let response = dispatcher.dispatch(vec![build_insert_request(&[1])]).await;
            let expected_attempts = if failures == 1 {
                assert_eq!(response.unwrap().affected_rows, 1);
                2
            } else {
                assert_eq!(response.unwrap_err().retry_hint(), RetryHint::Retryable);
                3
            };
            assert_eq!(
                server.attempt_counts(),
                HashMap::from([(region_id, expected_attempts)]),
                "{code:?}, failures: {failures}"
            );
            assert!(server.invalidator.calls.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn test_write_retry_stops_when_a_sibling_is_nonretryable() {
    for hint in [RetryHint::Retryable, RetryHint::RetryAfterRouteRefresh] {
        for regions in [[1, 2], [2, 1]] {
            let server = RegionTestServer::start().await;
            server.fail_region(1, [hint]);
            server.fail_region(2, [RetryHint::NonRetryable]);
            let dispatcher = server.create_dispatcher(2).await;
            let error = dispatcher
                .dispatch(vec![build_insert_request(&regions)])
                .await
                .unwrap_err();
            let terminal_region = RegionId::new(1, 2).as_u64();
            assert_eq!(error.retry_hint(), RetryHint::NonRetryable);
            assert_eq!(error.status_code(), StatusCode::InvalidArguments);
            assert!(
                error
                    .output_msg()
                    .contains(&format!("region {terminal_region} rejected the write")),
                "{error:?}"
            );
            assert_eq!(
                server.attempt_counts(),
                HashMap::from([(RegionId::new(1, 1).as_u64(), 1), (terminal_region, 1),])
            );
            assert!(server.invalidator.calls.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn test_should_retry_requires_a_retained_request_and_budget() {
    let backend = test_util::prepare_mocked_backend().await;
    let (partition_manager, _, cache_invalidator, catalog_manager) = create_routing_caches(backend);
    let retry = WriteRetry {
        node_manager: Arc::new(NodeClients::default()),
        partition_manager,
        options: build_retry_options(2),
        catalog_manager,
        cache_invalidator,
    };
    let failure = |request| {
        let error: client::Error = Status::unavailable("write rejected").into();
        WriteAttempt::Failed {
            error: error.into(),
            request,
        }
    };
    let retryable = || {
        failure(Some(RoutedWrite {
            attribution: Default::default(),
            peer: Arc::new(Peer::new(1, "peer")),
            request: Arc::new(build_insert_request(&[1])),
        }))
    };
    for (name, attempts, retries, expected) in [
        ("empty", vec![], 0, false),
        (
            "all succeeded",
            vec![WriteAttempt::Success(RegionResponse::new(1))],
            0,
            false,
        ),
        ("retryable", vec![retryable()], 0, true),
        ("last retry", vec![retryable()], 1, true),
        ("budget exhausted", vec![retryable()], 2, false),
        ("terminal first", vec![failure(None), retryable()], 0, false),
        ("terminal last", vec![retryable(), failure(None)], 0, false),
    ] {
        assert_eq!(retry.should_retry(&attempts, retries), expected, "{name}");
    }
}

#[tokio::test]
async fn test_write_retry_cancellation_stops_backoff() {
    for hint in [RetryHint::Retryable, RetryHint::RetryAfterRouteRefresh] {
        let server = RegionTestServer::start().await;
        server.fail_region(1, [hint]);
        let mut dispatcher = server.create_dispatcher(2).await;
        let options = &mut dispatcher.retry.as_mut().unwrap().options;
        options.initial_backoff = Duration::from_secs(60);
        options.max_backoff = Duration::from_secs(60);
        let attempt = dispatcher
            .send(
                Arc::new(server.peer.clone()),
                build_insert_request(&[1]),
                None,
            )
            .await;
        let cancellation = CancellationToken::new();
        let guard = cancellation.clone().drop_guard();
        let finish = dispatcher.finish(vec![attempt], None, Some(&cancellation));
        tokio::pin!(finish);
        futures::future::poll_fn(|cx| {
            assert!(finish.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(guard);
        let error = tokio::time::timeout(Duration::from_secs(1), finish)
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.retry_hint(), hint);
        assert_eq!(
            server.attempt_counts(),
            HashMap::from([(RegionId::new(1, 1).as_u64(), 1)])
        );
        assert!(server.invalidator.calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn test_write_retry_cancellation_drains_inflight_requests() {
    let server = RegionTestServer::start().await;
    server.fail_region(2, [RetryHint::RetryAfterRouteRefresh]);
    let release = Arc::new(Semaphore::new(0));
    *server.service.release.lock().unwrap() = Some(release.clone());
    let dispatcher = server.create_dispatcher(2).await;
    let writes = dispatcher
        .prepare(vec![build_insert_request(&[1, 2])])
        .await
        .unwrap();
    let cancellation = CancellationToken::new();
    let guard = cancellation.clone().drop_guard();
    let execute = dispatcher.execute(writes, None, Some(&cancellation));
    tokio::pin!(execute);
    tokio::time::timeout(
        Duration::from_secs(1),
        futures::future::poll_fn(|cx| {
            assert!(execute.as_mut().poll(cx).is_pending());
            if server.service.requests.lock().unwrap().len() == 2 {
                Poll::Ready(())
            } else {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }),
    )
    .await
    .unwrap();
    drop(guard);
    futures::future::poll_fn(|cx| {
        assert!(execute.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    release.add_permits(2);
    let error = tokio::time::timeout(Duration::from_secs(1), execute)
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.retry_hint(), RetryHint::RetryAfterRouteRefresh);
    assert_eq!(
        server.attempt_counts(),
        HashMap::from([
            (RegionId::new(1, 1).as_u64(), 1),
            (RegionId::new(1, 2).as_u64(), 1),
        ])
    );
    assert!(server.invalidator.calls.lock().unwrap().is_empty());
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
async fn test_finish_retries_initial_failures_in_one_round() {
    let server = RegionTestServer::start().await;
    for region in [2, 3] {
        server.fail_region(region, [RetryHint::RetryAfterRouteRefresh]);
    }
    let backend = test_util::prepare_mocked_backend().await;
    test_util::create_partition_rule_manager(backend.clone()).await;
    update_table_leader(&backend, &server.peer).await;
    let (partition_manager, table_info_cache, registry, catalog_manager) =
        create_routing_caches(backend);
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
    let dispatcher = WriteDispatcher::new(clients, partition_manager).with_retry(
        build_retry_options(2),
        catalog_manager,
        invalidator.clone(),
    );
    let mut attempts = Vec::new();
    for (region, value) in [(1, 101), (2, 11), (3, 1)] {
        let request =
            build_partitioned_request(WriteKind::BulkInsert, region, versions[&region], &[value]);
        attempts.push(
            dispatcher
                .send(Arc::new(server.peer.clone()), request, None)
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
    let response = dispatcher.finish(attempts, None, None).await.unwrap();
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
        vec![
            vec![CacheIdent::TableId(1)],
            vec![CacheIdent::TableName(TableName::new(
                "greptime", "public", "table_1"
            ))],
        ]
    );
}

#[tokio::test]
async fn test_write_retry_refreshes_stale_route_and_sends_only_failed_region_to_new_leader() {
    for regions in [vec![2], vec![1, 2]] {
        let old_server = RegionTestServer::start().await;
        let new_server = RegionTestServer::start().await;
        old_server.fail_region(2, [RetryHint::RetryAfterRouteRefresh]);
        let backend = test_util::prepare_mocked_backend().await;
        // Reuse the operator fixture to populate table metadata and partition rules.
        test_util::create_partition_rule_manager(backend.clone()).await;
        update_table_leader(&backend, &old_server.peer).await;

        let (partition_manager, table_info_cache, registry, catalog_manager) =
            create_routing_caches(backend.clone());
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
        let dispatcher = WriteDispatcher::new(clients, partition_manager).with_retry(
            build_retry_options(2),
            catalog_manager,
            invalidator.clone(),
        );
        let mut request = build_insert_request(&regions);
        if let Some(Body::Inserts(inserts)) = &mut request.body {
            for insert in &mut inserts.requests {
                insert.partition_expr_version = versions
                    [&RegionId::from_u64(insert.region_id).region_number()]
                    .map(|value| PartitionExprVersion { value });
            }
        }
        let response = dispatcher.dispatch(vec![request]).await.unwrap();
        assert_eq!(response.affected_rows, regions.len());
        assert_eq!(
            old_server.attempt_counts(),
            regions
                .iter()
                .map(|region| (RegionId::new(1, *region).as_u64(), 1))
                .collect()
        );
        assert_eq!(
            new_server.attempt_counts(),
            HashMap::from([(failed_region.as_u64(), 1)])
        );
        assert_eq!(
            *invalidator.calls.lock().unwrap(),
            vec![
                vec![CacheIdent::TableId(1)],
                vec![CacheIdent::TableName(TableName::new(
                    "greptime", "public", "table_1"
                ))],
            ]
        );
    }
}

#[derive(Clone, Copy, Debug)]
enum WriteKind {
    Insert,
    Delete,
    BulkInsert,
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
        WriteKind::BulkInsert => {
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

fn request_parts(request: &RegionRequest) -> (u32, Option<u64>, Vec<i32>) {
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
async fn test_write_retry_rebuilds_only_failed_rows_after_route_refresh() {
    for (kind, fail_repartitioned) in [
        (WriteKind::Insert, false),
        (WriteKind::Delete, false),
        (WriteKind::BulkInsert, false),
        (WriteKind::BulkInsert, true),
    ] {
        let old_server = RegionTestServer::start().await;
        let new_server = RegionTestServer::start().await;
        old_server.fail_region(2, [RetryHint::RetryAfterRouteRefresh]);
        if fail_repartitioned {
            new_server.fail_region(5, [RetryHint::NonRetryable]);
        }
        let backend = test_util::prepare_mocked_backend().await;
        test_util::create_partition_rule_manager(backend.clone()).await;
        update_table_leader(&backend, &old_server.peer).await;
        let (partition_manager, table_info_cache, registry, catalog_manager) =
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
            ..Default::default()
        });
        let clients = Arc::new(NodeClients::new(ChannelConfig::default()));
        let dispatcher = WriteDispatcher::new(clients, partition_manager.clone()).with_retry(
            build_retry_options(2),
            catalog_manager,
            invalidator.clone(),
        );
        let first = Arc::new(WriteProgress::default());
        let second = Arc::new(WriteProgress::default());
        let requests = vec![
            build_partitioned_request(kind, 1, old_versions[&1], &[70]),
            build_partitioned_request(kind, 2, old_versions[&2], &[20, 40]),
        ];
        let response = if matches!(kind, WriteKind::BulkInsert) {
            let mut shared = WriteAttribution::new(1, Some(first.clone()));
            shared.extend(WriteAttribution::new(1, Some(second.clone())));
            let owners = [WriteAttribution::new(1, Some(first.clone())), shared];
            let response = dispatcher
                .dispatch_attributed(requests.into_iter().zip(owners).collect(), None)
                .await;
            assert_eq!(first.written(), 2);
            assert_eq!(second.written(), if fail_repartitioned { 0 } else { 1 });
            response
        } else {
            dispatcher.dispatch(requests).await
        };
        if fail_repartitioned {
            assert!(response.is_err());
        } else {
            assert_eq!(response.unwrap().affected_rows, 3, "{kind:?}");
        }
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
            .map(request_parts)
            .collect::<Vec<_>>();
        writes.sort_by_key(|write| write.0);
        assert_eq!(
            writes,
            vec![(4, versions[&4], vec![20]), (5, versions[&5], vec![40])],
            "{kind:?}"
        );
        assert_eq!(
            *invalidator.calls.lock().unwrap(),
            vec![
                vec![CacheIdent::TableId(1)],
                vec![CacheIdent::TableName(TableName::new(
                    "greptime", "public", "table_1"
                ))],
            ]
        );
    }
}

fn build_sparse_bulk_request(
    region: u32,
    version: Option<u64>,
    tags: &[(&str, &str)],
) -> (RegionRequest, RecordBatch) {
    let codec = SparsePrimaryKeyCodec::schemaless();
    let keys = tags
        .iter()
        .enumerate()
        .map(|(index, (host, zone))| {
            let mut key = Vec::new();
            codec
                .encode_value_refs(
                    &[
                        (sparse::RESERVED_COLUMN_ID_TABLE_ID, ValueRef::UInt32(42)),
                        (
                            sparse::RESERVED_COLUMN_ID_TSID,
                            ValueRef::UInt64(index as u64),
                        ),
                        (10, ValueRef::String(host)),
                        (20, ValueRef::String(zone)),
                    ],
                    &mut key,
                )
                .unwrap();
            key
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(consts::PRIMARY_KEY_COLUMN_NAME, DataType::Binary, false),
            Field::new("host", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(BinaryArray::from_iter_values(keys)),
            Arc::new(StringArray::from_iter_values(
                tags.iter().map(|(host, _)| *host),
            )),
        ],
    )
    .unwrap();
    let (schema, data_header, payload) = flight::record_batch_to_ipc(batch.clone()).unwrap();
    let request = RegionRequest {
        body: Some(Body::BulkInsert(BulkInsertRequest {
            region_id: RegionId::new(1, region).as_u64(),
            partition_expr_version: version.map(|value| PartitionExprVersion { value }),
            body: Some(bulk_insert_request::Body::ArrowIpc(ArrowIpc {
                schema,
                data_header,
                payload,
            })),
            ..Default::default()
        })),
        ..Default::default()
    };
    (request, batch)
}

#[tokio::test]
async fn test_write_retry_restores_changed_partition_tags_for_failed_regions() {
    let old_server = RegionTestServer::start().await;
    let new_server = RegionTestServer::start().await;
    old_server.fail_region(2, [RetryHint::RetryAfterRouteRefresh]);
    let backend: KvBackendRef = Arc::new(MemoryKvBackend::new());
    let metadata = TableMetadataManager::new(backend.clone());
    let mut table_info = test_util::new_test_table_info(1, "physical", [1, 2].into_iter());
    table_info.meta.schema = Arc::new(schema::Schema::new(vec![
        schema::ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
        schema::ColumnSchema::new("zone", ConcreteDataType::string_datatype(), true),
    ]));
    table_info.meta.primary_key_indices = vec![0, 1];
    table_info.meta.partition_key_indices = vec![0];
    table_info.meta.column_ids = vec![10, 20];
    table_info.meta.options.extra_options.insert(
        metric_engine_consts::PHYSICAL_TABLE_METADATA_KEY.to_string(),
        String::new(),
    );
    let routes = [
        expr::col("host").lt(value::Value::String("m".into())),
        expr::col("host").gt_eq(value::Value::String("m".into())),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, expr)| RegionRoute {
        region: router::Region {
            id: RegionId::new(1, index as u32 + 1),
            partition_expr: expr.as_json_str().unwrap(),
            ..Default::default()
        },
        leader_peer: Some(old_server.peer.clone()),
        ..Default::default()
    })
    .collect();
    metadata
        .create_table_metadata(
            table_info,
            TableRouteValue::physical(routes),
            Default::default(),
        )
        .await
        .unwrap();
    let (partition_manager, table_info_cache, registry, catalog_manager) =
        create_routing_caches(backend.clone());
    // Warm the catalog's name-keyed TableCache before the partition column changes.
    let cached_table = catalog_manager
        .table("greptime", "public", "physical", None)
        .await
        .unwrap()
        .unwrap();
    let cached_info = cached_table.table_info();
    let (_, old_versions) = partition_manager
        .find_table_partition_rule(&cached_info)
        .await
        .unwrap();
    let current_info = metadata.table_info_manager().get(1).await.unwrap().unwrap();
    let mut updated_info = current_info.table_info.clone();
    updated_info.meta.partition_key_indices = vec![1];
    metadata
        .update_table_info(&current_info, None, updated_info)
        .await
        .unwrap();
    let current_route = metadata
        .table_route_manager()
        .table_route_storage()
        .get_with_raw_bytes(1)
        .await
        .unwrap()
        .unwrap();
    let mut routes = current_route.region_routes().unwrap().clone();
    for (route, expr) in routes.iter_mut().zip([
        expr::col("zone").lt(value::Value::String("m".into())),
        expr::col("zone").gt_eq(value::Value::String("m".into())),
    ]) {
        route.region.partition_expr = expr.as_json_str().unwrap();
        route.leader_peer = Some(new_server.peer.clone());
    }
    let invalidator = Arc::new(RecordingCacheInvalidator {
        calls: Mutex::default(),
        inner: Some(registry),
        route_update: Mutex::new(Some((backend, current_route.update(routes).unwrap()))),
        ..Default::default()
    });
    let clients = Arc::new(NodeClients::new(ChannelConfig::default()));
    let dispatcher = WriteDispatcher::new(clients, partition_manager.clone()).with_retry(
        build_retry_options(1),
        catalog_manager,
        invalidator.clone(),
    );
    // The successful row would move under the new rule, but must not be replayed.
    let (successful, _) = build_sparse_bulk_request(1, old_versions[&1], &[("a", "west")]);
    let (failed, batch) =
        build_sparse_bulk_request(2, old_versions[&2], &[("z", "east"), ("z", "west")]);
    dispatcher.dispatch(vec![successful, failed]).await.unwrap();
    let counts = HashMap::from([
        (RegionId::new(1, 1).as_u64(), 1),
        (RegionId::new(1, 2).as_u64(), 1),
    ]);
    assert_eq!(old_server.attempt_counts(), counts);
    assert_eq!(new_server.attempt_counts(), counts);
    assert_eq!(
        *invalidator.calls.lock().unwrap(),
        vec![
            vec![CacheIdent::TableId(1)],
            vec![CacheIdent::TableName(TableName::new(
                "greptime", "public", "physical"
            ))],
        ]
    );
    let refreshed_info = table_info_cache.get(1).await.unwrap().unwrap();
    assert_eq!(refreshed_info.meta.partition_key_indices, vec![1]);
    let (_, versions) = partition_manager
        .find_table_partition_rule(&refreshed_info)
        .await
        .unwrap();
    for request in new_server.service.requests.lock().unwrap().iter() {
        let Some(Body::BulkInsert(bulk)) = &request.body else {
            unreachable!()
        };
        let region = RegionId::from_u64(bulk.region_id).region_number();
        assert_eq!(
            bulk.partition_expr_version.map(|version| version.value),
            versions[&region]
        );
        let Some(bulk_insert_request::Body::ArrowIpc(ipc)) = &bulk.body else {
            unreachable!()
        };
        let actual = FlightDecoder::try_from_schema_bytes(&ipc.schema)
            .unwrap()
            .try_decode_record_batch(&ipc.data_header, &ipc.payload)
            .unwrap();
        assert_eq!(actual, batch.slice(region as usize - 1, 1));
        assert!(actual.column_by_name("zone").is_none());
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
            peers[&region].as_ref(),
            &partition_manager.find_region_leader(region).await.unwrap()
        );
    }
    assert!(Arc::ptr_eq(
        &peers[&RegionId::new(1, 1)],
        &peers[&RegionId::new(42, 1)],
    ));
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
async fn test_resolve_peers_shares_only_identical_peers() {
    let backend = test_util::prepare_mocked_backend().await;
    test_util::create_partition_rule_manager(backend.clone()).await;
    let manager = TableRouteManager::new(backend.clone());
    let storage = manager.table_route_storage();
    let current = storage.get_with_raw_bytes(1).await.unwrap().unwrap();
    let mut routes = current.region_routes().unwrap().clone();
    let peer = Peer::new(7, "same-address");
    let other = Peer::new(7, "other-address");
    for route in &mut routes {
        route.leader_peer = Some(if route.region.id.region_number() == 3 {
            other.clone()
        } else {
            peer.clone()
        });
    }
    let updated = current.update(routes).unwrap();
    let (txn, _) = storage.build_update_txn(1, &current, &updated).unwrap();
    assert!(backend.txn(txn).await.unwrap().succeeded);
    let (partition_manager, _, _, _) = create_routing_caches(backend);
    let dispatcher = WriteDispatcher::new(Arc::new(NodeClients::default()), partition_manager);
    let regions = [1, 2, 3].map(|region| RegionId::new(1, region));
    let peers = dispatcher.resolve_peers(&regions).await.unwrap();
    assert_eq!(peers.len(), 3);
    assert_eq!(peers[&regions[0]].as_ref(), &peer);
    assert_eq!(peers[&regions[1]].as_ref(), &peer);
    assert_eq!(peers[&regions[2]].as_ref(), &other);
    assert!(Arc::ptr_eq(&peers[&regions[0]], &peers[&regions[1]]));
    assert!(!Arc::ptr_eq(&peers[&regions[0]], &peers[&regions[2]]));
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
        let response = dispatcher.execute(writes, None, None).await.unwrap();
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
            let (elapsed, peers_match) = if mode == "batched" {
                let peers = dispatcher.resolve_peers(&regions).await.unwrap();
                (started.elapsed(), peers == expected)
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
                let elapsed = started.elapsed();
                let peers_match = peers.len() == expected.len()
                    && peers.iter().all(|(region, peer)| {
                        expected
                            .get(region)
                            .is_some_and(|expected| expected.as_ref() == peer)
                    });
                (elapsed, peers_match)
            };
            let gets = counter.get() - before;
            assert!(peers_match, "{mode}");
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
async fn test_insert_retry_rebuilds_logical_rows_in_mixed_table_envelope() {
    check_write_retry_rebuilds_logical_rows_in_mixed_table_envelope(WriteKind::Insert).await;
}

#[tokio::test]
async fn test_delete_retry_rebuilds_logical_rows_in_mixed_table_envelope() {
    check_write_retry_rebuilds_logical_rows_in_mixed_table_envelope(WriteKind::Delete).await;
}

async fn check_write_retry_rebuilds_logical_rows_in_mixed_table_envelope(kind: WriteKind) {
    let server = RegionTestServer::start().await;
    let backend = test_util::prepare_mocked_backend().await;
    test_util::create_partition_rule_manager(backend.clone()).await;
    let metadata = TableMetadataManager::new(backend.clone());
    let current = metadata.table_info_manager().get(1).await.unwrap().unwrap();
    let mut physical_info = current.table_info.clone();
    physical_info.meta.engine = metric_engine_consts::METRIC_ENGINE_NAME.to_string();
    physical_info.meta.options.extra_options.insert(
        metric_engine_consts::PHYSICAL_TABLE_METADATA_KEY.to_string(),
        String::new(),
    );
    metadata
        .update_table_info(&current, None, physical_info)
        .await
        .unwrap();
    let mut logical_info =
        test_util::new_test_table_info(42, "retry_logical", [1, 2, 3].into_iter());
    logical_info.meta.engine = metric_engine_consts::METRIC_ENGINE_NAME.to_string();
    logical_info.meta.options.extra_options.insert(
        metric_engine_consts::LOGICAL_TABLE_METADATA_KEY.to_string(),
        "table_1".to_string(),
    );
    // Persisted logical metadata inherits partition columns from the physical table.
    logical_info.meta.partition_key_indices.clear();
    // Physical column `a` is at index 0, but logical column `a` is at index 2.
    let mut columns = logical_info.meta.schema.column_schemas().to_vec();
    columns.swap(0, 2);
    logical_info.meta.schema = Arc::new(schema::Schema::new(columns));
    logical_info.meta.primary_key_indices = vec![2];
    metadata
        .create_table_metadata(
            logical_info,
            TableRouteValue::logical(1),
            Default::default(),
        )
        .await
        .unwrap();
    update_table_leader(&backend, &server.peer).await;
    let (partition_manager, table_info_cache, registry, catalog_manager) =
        create_routing_caches(backend);
    let table_info = table_info_cache.get(1).await.unwrap().unwrap();
    assert_eq!(table_info.meta.partition_key_indices, vec![0]);
    assert!(
        table_info_cache
            .get(42)
            .await
            .unwrap()
            .unwrap()
            .meta
            .partition_key_indices
            .is_empty()
    );
    let (_, versions) = partition_manager
        .find_table_partition_rule(&table_info)
        .await
        .unwrap();
    let invalidator = Arc::new(RecordingCacheInvalidator {
        inner: Some(registry),
        ..Default::default()
    });
    let clients = Arc::new(NodeClients::new(ChannelConfig::default()));
    let dispatcher = WriteDispatcher::new(clients, partition_manager).with_retry(
        build_retry_options(2),
        catalog_manager,
        invalidator.clone(),
    );
    let physical_id = RegionId::new(1, 1).as_u64();
    let stale_logical_id = RegionId::new(42, 2).as_u64();
    let rebuilt_logical_ids = [1, 3].map(|region| RegionId::new(42, region).as_u64());
    let mut status = Status::unavailable("partition changed");
    status.metadata_mut().insert(
        common_error::GREPTIME_DB_HEADER_ERROR_RETRY_HINT,
        RetryHint::RetryAfterRouteRefresh.as_str().parse().unwrap(),
    );
    server
        .service
        .rejections
        .lock()
        .unwrap()
        .insert(stale_logical_id, VecDeque::from([status]));

    let header = Some(RegionRequestHeader {
        dbname: "greptime.public".to_string(),
        ..Default::default()
    });
    let mut physical = build_partitioned_request(kind, 1, versions[&1], &[70]);
    physical.header = header.clone();
    // Only the failed logical rows are split again, preserving their logical table ID.
    let mut logical = build_partitioned_request(kind, 2, Some(0), &[1, 70]);
    let mut rebuilt_lower = build_partitioned_request(kind, 3, versions[&3], &[1]);
    let mut rebuilt_upper = build_partitioned_request(kind, 1, versions[&1], &[70]);
    for request in [&mut logical, &mut rebuilt_lower, &mut rebuilt_upper] {
        request.header = header.clone();
        let region_id = match request.body.as_mut().unwrap() {
            Body::Inserts(inserts) => &mut inserts.requests[0].region_id,
            Body::Deletes(deletes) => &mut deletes.requests[0].region_id,
            _ => unreachable!(),
        };
        *region_id = RegionId::new(42, RegionId::from_u64(*region_id).region_number()).as_u64();
    }
    let mut envelope = physical.clone();
    match (
        envelope.body.as_mut().unwrap(),
        logical.body.clone().unwrap(),
    ) {
        (Body::Inserts(physical), Body::Inserts(logical)) => {
            physical.requests.extend(logical.requests);
        }
        (Body::Deletes(physical), Body::Deletes(logical)) => {
            physical.requests.extend(logical.requests);
        }
        _ => unreachable!(),
    }
    let response = dispatcher.dispatch(vec![envelope]).await.unwrap();
    assert_eq!(response.affected_rows, 3, "{kind:?}");
    assert_eq!(
        server.attempt_counts(),
        HashMap::from([
            (physical_id, 1),
            (stale_logical_id, 1),
            (rebuilt_logical_ids[0], 1),
            (rebuilt_logical_ids[1], 1),
        ]),
        "{kind:?}"
    );
    let mut requests = server.service.requests.lock().unwrap().clone();
    requests.sort_by_key(|request| write_region_id(request).unwrap().as_u64());
    let mut expected = vec![physical, rebuilt_upper, logical, rebuilt_lower];
    for request in &mut expected {
        api::region::set_accept_partial_result(request.header.get_or_insert_default());
    }
    assert_eq!(requests, expected, "{kind:?}");
    let calls = invalidator.calls.lock().unwrap();
    assert_eq!(calls.len(), 2, "{kind:?}");
    assert_eq!(calls[0].len(), 2, "{kind:?}");
    assert!(calls[0].contains(&CacheIdent::TableId(1)));
    assert!(calls[0].contains(&CacheIdent::TableId(42)));
    assert_eq!(
        calls[1],
        vec![CacheIdent::TableName(TableName::new(
            "greptime",
            "public",
            "retry_logical"
        ))],
        "{kind:?}"
    );
}

#[tokio::test]
async fn test_write_retry_rejects_reused_table_name() {
    let server = RegionTestServer::start().await;
    server.fail_region(2, [RetryHint::RetryAfterRouteRefresh]);
    let backend = test_util::prepare_mocked_backend().await;
    test_util::create_partition_rule_manager(backend.clone()).await;
    update_table_leader(&backend, &server.peer).await;
    let (partition_manager, _, registry, catalog_manager) = create_routing_caches(backend.clone());
    // Prime both name-keyed caches with the old identity before the concurrent rename.
    let original = catalog_manager
        .table("greptime", "public", "table_1", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.table_info().table_id(), 1);
    let invalidator = Arc::new(RecordingCacheInvalidator {
        inner: Some(registry),
        rename_and_recreate: Mutex::new(Some(backend)),
        ..Default::default()
    });
    let dispatcher = WriteDispatcher::new(Arc::new(NodeClients::default()), partition_manager)
        .with_retry(build_retry_options(1), catalog_manager.clone(), invalidator);
    let error = dispatcher
        .dispatch(vec![build_partitioned_request(
            WriteKind::Insert,
            2,
            Some(0),
            &[20],
        )])
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("changed ID from 1 to 42"),
        "{error:?}"
    );
    assert_eq!(
        server.attempt_counts(),
        HashMap::from([(RegionId::new(1, 2).as_u64(), 1)])
    );
    let replacement = catalog_manager
        .table("greptime", "public", "table_1", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replacement.table_info().table_id(), 42);
}

#[tokio::test]
async fn test_with_retry_can_disable_previously_enabled_retries() {
    let server = RegionTestServer::start().await;
    let dispatcher = server.create_dispatcher(1).await;
    let retry = dispatcher.retry.as_ref().unwrap();
    let catalog_manager = retry.catalog_manager.clone();
    let cache_invalidator = retry.cache_invalidator.clone();
    let dispatcher =
        dispatcher.with_retry(build_retry_options(0), catalog_manager, cache_invalidator);
    assert!(dispatcher.retry.is_none());
    server.fail_region(1, [RetryHint::Retryable]);
    assert!(
        dispatcher
            .dispatch(vec![build_insert_request(&[1])])
            .await
            .is_err()
    );
    assert_eq!(server.attempt_counts()[&RegionId::new(1, 1).as_u64()], 1);
}
