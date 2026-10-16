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

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use api::v1::region::RegionRequestHeader;
use api::v1::{QueryContext as ApiQueryContext, SnapshotSequences};
use async_trait::async_trait;
use common_function::function_factory::ScalarFunctionFactory;
use common_query::Output;
use common_query::request::{
    INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY, QueryRequest,
    RAW_SERIES_REQUEST_EXTENSION_KEY, RawSeriesRequest,
};
use common_runtime::Runtime;
use common_runtime::runtime::{BuilderBuild, RuntimeTrait};
use datafusion::catalog::TableFunction;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion_expr::{AggregateUDF, LogicalPlan, WindowUDF, table_scan};
use datatypes::series::raw::RawSeriesOrdering;
use query::error::Error as QueryError;
use query::planner::LogicalPlanner;
use query::query_engine::{DescribeResult, QueryEngineState};
use query::{QueryEngine, QueryEngineContext};
use servers::grpc::FlightCompression;
use session::context::{QueryContextRef, READ_PREFERENCE_EXTENSION_KEY};
use store_api::storage::RegionId;
use table::TableRef;
use tokio::sync::{Notify, oneshot};
use tokio::time::timeout;

use super::*;
use crate::error::Error;
use crate::tests::MockRegionEngine;

const BOUND: Duration = Duration::from_secs(5);

#[derive(Default)]
struct EngineState {
    calls: AtomicUsize,
    execute_calls: AtomicUsize,
    prepare_entered: Notify,
    context: Mutex<Option<QueryContextRef>>,
    action: Mutex<EngineAction>,
}

enum EngineAction {
    Return(Option<RawSeriesOrdering>),
    Error,
    Wait(oneshot::Receiver<()>, Arc<Notify>),
}

impl Default for EngineAction {
    fn default() -> Self {
        Self::Return(None)
    }
}

struct AdmissionQueryEngine {
    state: Arc<EngineState>,
}

#[async_trait]
impl QueryEngine for AdmissionQueryEngine {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn planner(&self) -> Arc<dyn LogicalPlanner> {
        unimplemented!()
    }

    fn name(&self) -> &str {
        "AdmissionQueryEngine"
    }

    async fn describe(
        &self,
        _plan: LogicalPlan,
        _query_ctx: QueryContextRef,
    ) -> query::error::Result<DescribeResult> {
        unimplemented!()
    }

    async fn execute(
        &self,
        _plan: LogicalPlan,
        _query_ctx: QueryContextRef,
    ) -> query::error::Result<Output> {
        self.state.execute_calls.fetch_add(1, Ordering::SeqCst);
        panic!("raw-series admission must not execute a query")
    }

    async fn prepare_raw_series(
        &self,
        _plan: LogicalPlan,
        query_ctx: QueryContextRef,
        _binding: RawSeriesRequest,
    ) -> query::error::Result<Option<RawSeriesOrdering>> {
        self.state.calls.fetch_add(1, Ordering::SeqCst);
        self.state.prepare_entered.notify_one();
        *self.state.context.lock().unwrap() = Some(query_ctx);
        let action = std::mem::take(&mut *self.state.action.lock().unwrap());
        match action {
            EngineAction::Return(ordering) => Ok(ordering),
            EngineAction::Error => Err(QueryError::DataFusion {
                error: DataFusionError::Plan("injected prepare error".to_string()),
                location: snafu::location!(),
            }),
            EngineAction::Wait(receiver, dropped) => {
                struct Dropped(Arc<Notify>);
                impl Drop for Dropped {
                    fn drop(&mut self) {
                        self.0.notify_one();
                    }
                }
                let _dropped = Dropped(dropped);
                receiver.await.map_err(|_| QueryError::DataFusion {
                    error: DataFusionError::Plan("released".into()),
                    location: snafu::location!(),
                })?;
                Ok(None)
            }
        }
    }

    async fn execute_raw_series(
        &self,
        _plan: LogicalPlan,
        _query_ctx: QueryContextRef,
        _binding: RawSeriesRequest,
    ) -> query::error::Result<Output> {
        self.state.execute_calls.fetch_add(1, Ordering::SeqCst);
        panic!("raw-series admission must not execute a query")
    }

    fn register_aggregate_function(&self, _func: AggregateUDF) {}
    fn register_scalar_function(&self, _func: ScalarFunctionFactory) {}
    fn register_table_function(&self, _func: Arc<TableFunction>) {}
    fn register_window_function(&self, _func: WindowUDF) {}

    fn read_table(&self, _table: TableRef) -> query::error::Result<DataFrame> {
        unimplemented!()
    }

    fn engine_context(&self, _query_ctx: QueryContextRef) -> QueryEngineContext {
        unimplemented!()
    }

    fn engine_state(&self) -> &QueryEngineState {
        unimplemented!()
    }
}

#[derive(Default)]
struct ProviderState {
    calls: AtomicUsize,
    fail: AtomicUsize,
    action: Mutex<Option<ProviderWait>>,
}

struct ProviderWait {
    entered: Arc<Notify>,
    release: oneshot::Receiver<()>,
    dropped: Arc<Notify>,
}

struct AdmissionProviderFactory {
    state: Arc<ProviderState>,
    provider: Arc<dyn TableProvider>,
}

#[async_trait]
impl query::dummy_catalog::TableProviderFactory for AdmissionProviderFactory {
    async fn create(
        &self,
        _region_id: RegionId,
        _engine: store_api::region_engine::RegionEngineRef,
        _ctx: Option<QueryContextRef>,
    ) -> query::error::Result<Arc<dyn TableProvider>> {
        self.state.calls.fetch_add(1, Ordering::SeqCst);
        if self.state.fail.load(Ordering::SeqCst) > 0 {
            return Err(QueryError::DataFusion {
                error: DataFusionError::Plan("injected provider error".into()),
                location: snafu::location!(),
            });
        }
        let wait = self.state.action.lock().unwrap().take();
        if let Some(wait) = wait {
            wait.entered.notify_one();
            struct Dropped(Arc<Notify>);
            impl Drop for Dropped {
                fn drop(&mut self) {
                    self.0.notify_one();
                }
            }
            let _dropped = Dropped(wait.dropped);
            wait.release.await.map_err(|_| QueryError::DataFusion {
                error: DataFusionError::Plan("released".into()),
                location: snafu::location!(),
            })?;
        }
        Ok(self.provider.clone())
    }
}

struct Fixture {
    server: RegionServer,
    engine: Arc<EngineState>,
    provider: Arc<ProviderState>,
    raw: RawSeriesRequest,
}

fn fixture(engine_action: EngineAction, provider_wait: Option<ProviderWait>) -> Fixture {
    let raw = raw_fixture();
    let region_id = raw.physical_metadata.region_id;
    let schema = raw.physical_metadata.schema.arrow_schema();
    let source = Arc::new(MemTable::try_new(schema.clone(), vec![vec![]]).unwrap());
    let provider = Arc::new(ProviderState::default());
    *provider.action.lock().unwrap() = provider_wait;
    let engine_state = Arc::new(EngineState::default());
    *engine_state.action.lock().unwrap() = engine_action;
    let engine: QueryEngineRef = Arc::new(AdmissionQueryEngine {
        state: engine_state.clone(),
    });
    let factory: TableProviderFactoryRef = Arc::new(AdmissionProviderFactory {
        state: provider.clone(),
        provider: source,
    });
    let mut server = RegionServer::with_table_provider(
        engine,
        Runtime::builder().build().unwrap(),
        Box::new(crate::event_listener::NoopRegionServerEventListener),
        factory,
        1,
        BOUND,
        FlightCompression::default(),
    );
    let metadata = Arc::new(raw.physical_metadata.clone());
    let (region_engine, _) = MockRegionEngine::with_metadata_mock_fn(
        MITO_ENGINE_NAME,
        Box::new(move |_| Ok(metadata.clone())),
    );
    server.register_engine(region_engine.clone());
    server
        .inner
        .region_map
        .insert(region_id, RegionEngineWithStatus::Ready(region_engine));
    Fixture {
        server,
        engine: engine_state,
        provider,
        raw,
    }
}

fn raw_fixture() -> RawSeriesRequest {
    RawSeriesRequest::from_extension_value(include_str!(
        "../../../../tests/compatibility/wire/raw_series_request/v1_request.json"
    ))
    .unwrap()
}

fn logical_request(raw: &RawSeriesRequest) -> QueryRequest {
    let schema = raw.physical_metadata.schema.arrow_schema();
    let plan = table_scan(Some("cpu"), schema.as_ref(), None)
        .unwrap()
        .build()
        .unwrap();
    QueryRequest {
        header: None,
        region_id: raw.physical_metadata.region_id,
        plan,
    }
}

fn bound_request(raw: &RawSeriesRequest) -> QueryRequest {
    with_marker(
        logical_request(raw),
        Some(raw.to_extension_value().unwrap()),
    )
}

fn with_marker(mut request: QueryRequest, marker: Option<String>) -> QueryRequest {
    let mut header = RegionRequestHeader::default();
    if let Some(marker) = marker {
        header.query_context = Some(ApiQueryContext {
            extensions: std::collections::HashMap::from([(
                RAW_SERIES_REQUEST_EXTENSION_KEY.to_string(),
                marker,
            )]),
            ..Default::default()
        });
    }
    request.header = Some(header);
    request
}

async fn assert_available(fixture: &Fixture) {
    let parallelism = fixture.server.inner.parallelism.as_ref().unwrap();
    let permit = timeout(BOUND, parallelism.semaphore.clone().acquire_owned())
        .await
        .expect("query permit should be released")
        .unwrap();
    drop(permit);
    assert_eq!(parallelism.semaphore.available_permits(), 1);
}

#[tokio::test]
async fn raw_series_admission_prepares_without_execution_and_releases_permit() {
    for action in [
        EngineAction::Return(Some(RawSeriesOrdering::Contiguous)),
        EngineAction::Return(Some(RawSeriesOrdering::ReadKeyThenTime)),
        EngineAction::Return(None),
        EngineAction::Error,
    ] {
        let expected = match &action {
            EngineAction::Return(Some(ordering)) => Some(Some(*ordering)),
            EngineAction::Return(None) => Some(None),
            EngineAction::Error => None,
            EngineAction::Wait(..) => unreachable!(),
        };
        let fixture = fixture(action, None);
        let result = fixture
            .server
            .prepare_raw_series_query(bound_request(&fixture.raw))
            .await;
        match expected {
            Some(Some(expected)) => assert_eq!(result.unwrap(), Some(expected)),
            Some(None) => assert_eq!(result.unwrap(), None),
            None => match result.unwrap_err() {
                Error::ExecuteLogicalPlan {
                    source:
                        QueryError::DataFusion {
                            error: DataFusionError::Plan(reason),
                            ..
                        },
                    ..
                } => assert_eq!(reason, "injected prepare error"),
                error => panic!("expected injected DataFusion planning error, got: {error:?}"),
            },
        }
        assert_eq!(fixture.engine.calls.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.engine.execute_calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 1);
        assert_available(&fixture).await;
    }

    let provider_error_fixture = fixture(EngineAction::Return(None), None);
    provider_error_fixture
        .provider
        .fail
        .store(1, Ordering::SeqCst);
    let provider_error = provider_error_fixture
        .server
        .prepare_raw_series_query(bound_request(&provider_error_fixture.raw))
        .await
        .expect_err("provider preparation error should propagate");
    match provider_error {
        Error::ExecuteLogicalPlan {
            source:
                QueryError::DataFusion {
                    error: DataFusionError::Plan(reason),
                    ..
                },
            ..
        } => assert_eq!(reason, "injected provider error"),
        error => panic!("expected injected DataFusion provider error, got: {error:?}"),
    }
    assert_eq!(
        provider_error_fixture.engine.calls.load(Ordering::SeqCst),
        0
    );
    assert_available(&provider_error_fixture).await;

    let wait_entered = Arc::new(Notify::new());
    let (release, receiver) = oneshot::channel();
    let provider_fixture = fixture(
        EngineAction::Return(None),
        Some(ProviderWait {
            entered: wait_entered.clone(),
            release: receiver,
            dropped: Arc::new(Notify::new()),
        }),
    );
    let caller = {
        let server = provider_fixture.server.clone();
        let request = bound_request(&provider_fixture.raw);
        tokio::spawn(async move { server.prepare_raw_series_query(request).await })
    };
    timeout(BOUND, wait_entered.notified()).await.unwrap();
    assert_eq!(
        provider_fixture
            .server
            .inner
            .parallelism
            .as_ref()
            .unwrap()
            .semaphore
            .available_permits(),
        0
    );
    release.send(()).unwrap();
    assert!(timeout(BOUND, caller).await.unwrap().unwrap().is_ok());
    assert_available(&provider_fixture).await;

    let (release, receiver) = oneshot::channel();
    let engine_fixture = fixture(EngineAction::Wait(receiver, Arc::new(Notify::new())), None);
    let caller = {
        let server = engine_fixture.server.clone();
        let request = bound_request(&engine_fixture.raw);
        tokio::spawn(async move { server.prepare_raw_series_query(request).await })
    };
    timeout(BOUND, engine_fixture.engine.prepare_entered.notified())
        .await
        .unwrap();
    assert_eq!(
        engine_fixture
            .server
            .inner
            .parallelism
            .as_ref()
            .unwrap()
            .semaphore
            .available_permits(),
        0
    );
    release.send(()).unwrap();
    assert!(timeout(BOUND, caller).await.unwrap().unwrap().is_ok());
    assert_available(&engine_fixture).await;
}

#[tokio::test]
async fn raw_series_admission_cancellation_aborts_owned_preparation_task() {
    let entered = Arc::new(Notify::new());
    let dropped = Arc::new(Notify::new());
    let (_provider_release, receiver) = oneshot::channel();
    let provider_fixture = fixture(
        EngineAction::Return(None),
        Some(ProviderWait {
            entered: entered.clone(),
            release: receiver,
            dropped: dropped.clone(),
        }),
    );
    let caller = {
        let server = provider_fixture.server.clone();
        let request = bound_request(&provider_fixture.raw);
        tokio::spawn(async move { server.prepare_raw_series_query(request).await })
    };
    timeout(BOUND, entered.notified()).await.unwrap();
    assert_eq!(
        provider_fixture
            .server
            .inner
            .parallelism
            .as_ref()
            .unwrap()
            .semaphore
            .available_permits(),
        0
    );
    caller.abort();
    assert!(timeout(BOUND, caller).await.unwrap().is_err());
    timeout(BOUND, dropped.notified()).await.unwrap();
    assert_available(&provider_fixture).await;
    assert_eq!(provider_fixture.engine.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        provider_fixture.engine.execute_calls.load(Ordering::SeqCst),
        0
    );

    let dropped = Arc::new(Notify::new());
    let (_engine_release, receiver) = oneshot::channel();
    let engine_fixture = fixture(EngineAction::Wait(receiver, dropped.clone()), None);
    let caller = {
        let server = engine_fixture.server.clone();
        let request = bound_request(&engine_fixture.raw);
        tokio::spawn(async move { server.prepare_raw_series_query(request).await })
    };
    timeout(BOUND, engine_fixture.engine.prepare_entered.notified())
        .await
        .unwrap();
    assert_eq!(
        engine_fixture
            .server
            .inner
            .parallelism
            .as_ref()
            .unwrap()
            .semaphore
            .available_permits(),
        0
    );
    caller.abort();
    assert!(timeout(BOUND, caller).await.unwrap().is_err());
    timeout(BOUND, dropped.notified()).await.unwrap();
    assert_available(&engine_fixture).await;
    assert_eq!(
        engine_fixture.engine.execute_calls.load(Ordering::SeqCst),
        0
    );
}

#[tokio::test]
async fn raw_series_admission_rejects_bad_context_before_provider_creation() {
    let raw = raw_fixture();
    let valid = raw.to_extension_value().unwrap();
    let mut malformed = raw.clone();
    malformed.version = 2;
    let malformed = serde_json::to_string(&malformed).unwrap();

    let cases = [
        (Some("{}".to_string()), RegionId::new(9, 1), false, false),
        (Some(valid.clone()), RegionId::new(9, 1), false, false),
        (
            Some(valid.clone()),
            raw.physical_metadata.region_id,
            true,
            false,
        ),
        (
            Some(malformed),
            raw.physical_metadata.region_id,
            false,
            false,
        ),
        (
            Some(valid.clone()),
            raw.physical_metadata.region_id,
            false,
            true,
        ),
    ];
    let missing = fixture(EngineAction::Return(None), None);
    let missing_error = missing
        .server
        .prepare_raw_series_query(with_marker(logical_request(&missing.raw), None))
        .await
        .expect_err("a missing raw-series binding must not be an ordinary query");
    assert!(matches!(missing_error, Error::InvalidQueryContext { .. }));
    assert_eq!(missing.provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(missing.engine.calls.load(Ordering::SeqCst), 0);
    assert_available(&missing).await;
    for (marker, region_id, dynamic_intent, invalid_preference) in cases {
        let fixture = fixture(EngineAction::Return(None), None);
        let mut request = logical_request(&fixture.raw);
        request.region_id = region_id;
        let mut request = with_marker(request, marker);
        if dynamic_intent || invalid_preference {
            request
                .header
                .as_mut()
                .unwrap()
                .query_context
                .as_mut()
                .unwrap()
                .extensions
                .insert(
                    if dynamic_intent {
                        INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY.to_string()
                    } else {
                        READ_PREFERENCE_EXTENSION_KEY.to_string()
                    },
                    if dynamic_intent {
                        "{}".to_string()
                    } else {
                        "invalid".to_string()
                    },
                );
        }
        let error = fixture
            .server
            .prepare_raw_series_query(request)
            .await
            .expect_err("invalid raw-series request must be rejected");
        assert!(matches!(error, Error::InvalidQueryContext { .. }));
        assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.engine.calls.load(Ordering::SeqCst), 0);
        assert_available(&fixture).await;
    }
}

#[tokio::test]
async fn raw_series_admission_preserves_unrelated_context_and_consumes_marker() {
    let fixture = fixture(EngineAction::Return(None), None);
    let mut request = logical_request(&fixture.raw);
    let raw_value = fixture.raw.to_extension_value().unwrap();
    let mut header = RegionRequestHeader::default();
    header.query_context = Some(ApiQueryContext {
        current_catalog: "observability".to_string(),
        current_schema: "metrics".to_string(),
        timezone: "Asia/Tokyo".to_string(),
        channel: session::context::Channel::Internal as u32,
        snapshot_seqs: Some(SnapshotSequences {
            snapshot_seqs: std::collections::HashMap::from([(
                fixture.raw.physical_metadata.region_id.as_u64(),
                41,
            )]),
            sst_min_sequences: std::collections::HashMap::from([(
                fixture.raw.physical_metadata.region_id.as_u64(),
                35,
            )]),
        }),
        extensions: std::collections::HashMap::from([
            (RAW_SERIES_REQUEST_EXTENSION_KEY.to_string(), raw_value),
            ("unrelated.extension".to_string(), "kept".to_string()),
        ]),
        explain: None,
    });
    request.header = Some(header);
    fixture
        .server
        .prepare_raw_series_query(request)
        .await
        .unwrap();
    let ctx = fixture.engine.context.lock().unwrap().clone().unwrap();
    assert!(ctx.extension(RAW_SERIES_REQUEST_EXTENSION_KEY).is_none());
    assert_eq!(
        ctx.extension("unrelated.extension").as_deref(),
        Some("kept")
    );
    assert_eq!(ctx.current_catalog(), "observability");
    assert_eq!(ctx.current_schema(), "metrics");
    assert_eq!(
        ctx.timezone(),
        common_time::Timezone::from_tz_string("Asia/Tokyo").unwrap()
    );
    assert_eq!(ctx.channel(), session::context::Channel::Internal);
    let region_id = fixture.raw.physical_metadata.region_id.as_u64();
    assert_eq!(
        ctx.snapshots(),
        std::collections::HashMap::from([(region_id, 41)])
    );
    assert_eq!(
        ctx.sst_min_sequences(),
        std::collections::HashMap::from([(region_id, 35)])
    );
    assert_eq!(fixture.engine.execute_calls.load(Ordering::SeqCst), 0);
    assert_available(&fixture).await;
}
