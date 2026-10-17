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

//! Transport tests for the named raw-series preflight action.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use api::v1::QueryContext as ApiQueryContext;
use api::v1::SnapshotSequences;
use api::v1::region::{QueryRequest as PbQueryRequest, RegionRequestHeader};
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, Result as FlightResult, SchemaResult,
    Ticket,
};
use common_error::GREPTIME_DB_HEADER_ERROR_CODE;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_meta::node_manager::Datanode;
use common_query::request::{
    PREPARE_RAW_SERIES_ACTION, PreparedNamedRawSeries, QueryRequest, RawSeriesRequest,
    encode_prepare_raw_series_response,
};
use datatypes::series::SeriesOrdering;
use futures_util::{Stream, stream};
use prost::Message;
use query::dummy_catalog::DummyCatalogManager;
use query::options::QueryOptions;
use query::query_engine::{DefaultSerializer, QueryEngineFactory};
use store_api::storage::RegionId;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::codec::Streaming;
use tonic::{Request, Response, Status};

use crate::region::RegionRequester;
use crate::{Client, Error};

/// One response stream item of the action contract.
type FlightItem = std::result::Result<FlightResult, Status>;

/// A server-streaming response of the test service.
type FlightStream<T> = Pin<Box<dyn Stream<Item = std::result::Result<T, Status>> + Send + 'static>>;

/// Budget of the deadline tests.
///
/// A concrete short value rather than the production budget: paused time would need the
/// `tokio/test-util` feature, which this crate cannot enable without a manifest change.
/// The test still proves the deadline covers the body reads, because it first waits until
/// the service reports the poll that leaves the client waiting on the response.
const DEADLINE_TEST_BUDGET: Duration = Duration::from_millis(500);

/// Budget of the cancellation tests: long enough that the deadline cannot interfere.
const CANCELLATION_TEST_BUDGET: Duration = Duration::from_secs(30);

/// Bound on the test service reporting a poll or a stream release to the test.
const SERVICE_REPORT_BOUND: Duration = Duration::from_secs(5);

/// The production budget also bounds the body reads of the preflight.
#[test]
fn test_preflight_budget_is_the_existing_flight_budget() {
    assert_eq!(
        Duration::from_secs(10),
        super::FLIGHT_DO_GET_TIMEOUT,
        "the preflight must reuse the existing Flight response budget"
    );
}

/// SQL whose plan is a real projection over a real filter.
const REQUEST_PLAN_SQL: &str = "SELECT 1 AS value WHERE 1 > 0";

/// Wire key of the session read-preference extension.
const READ_PREFERENCE_EXTENSION_KEY: &str = "query.read_preference";

/// One scripted answer of the test service.
enum ScriptedReply {
    /// The action call itself fails before any response is produced.
    RpcError(Status),
    /// The response carries these items and then ends.
    Items(Vec<FlightItem>),
    /// The response stalls after its headers, reports what the client waits for, and
    /// yields its scripted items only once the test releases it.
    Waking {
        blocked: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
        items: Vec<FlightItem>,
    },
    /// The response delivers at most one item and then stalls forever, reporting the
    /// poll that leaves the client waiting and the drop that releases the stream.
    Stalling {
        first: Option<FlightResult>,
        blocked: oneshot::Sender<()>,
        dropped: oneshot::Sender<()>,
        delivered: Arc<AtomicUsize>,
    },
}

/// A response stream that holds its headers, reports the first poll, and yields its
/// items only after the test releases it.
///
/// The released items are therefore delivered after the response started, so an error
/// among them is a stream error and not an initial call rejection.
struct WakingStream {
    items: VecDeque<FlightItem>,
    blocked: Option<oneshot::Sender<()>>,
    release: Option<oneshot::Receiver<()>>,
}

impl Stream for WakingStream {
    type Item = FlightItem;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(blocked) = this.blocked.take() {
            let _ = blocked.send(());
        }
        if let Some(mut release) = this.release.take()
            && Pin::new(&mut release).poll(cx).is_pending()
        {
            this.release = Some(release);
            return Poll::Pending;
        }
        Poll::Ready(this.items.pop_front())
    }
}

/// A response stream that delivers at most one item and then stalls forever.
///
/// It reports the poll that leaves the client waiting for the next item and the drop
/// that releases the stream, so a test can prove that cancelling the client future
/// really releases the server-side response instead of merely abandoning a local
/// future.
struct StallingStream {
    first: Option<FlightResult>,
    blocked: Option<oneshot::Sender<()>>,
    dropped: Option<oneshot::Sender<()>>,
    delivered: Arc<AtomicUsize>,
}

impl Stream for StallingStream {
    type Item = FlightItem;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(first) = this.first.take() {
            this.delivered.fetch_add(1, Ordering::SeqCst);
            return Poll::Ready(Some(Ok(first)));
        }
        if let Some(blocked) = this.blocked.take() {
            let _ = blocked.send(());
        }
        Poll::Pending
    }
}

impl Drop for StallingStream {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped.take() {
            let _ = dropped.send(());
        }
    }
}

/// An action observed by the test service.
#[derive(Clone, Debug)]
struct RecordedAction {
    action_type: String,
    body: Vec<u8>,
    timeout: Option<String>,
}

/// A Flight service that records preflight actions and answers them from a script.
#[derive(Clone, Default)]
struct RecordingFlightActionService {
    actions: Arc<Mutex<Vec<RecordedAction>>>,
    replies: Arc<Mutex<VecDeque<ScriptedReply>>>,
    do_get_calls: Arc<AtomicUsize>,
}

impl RecordingFlightActionService {
    fn script(&self, reply: ScriptedReply) {
        self.replies.lock().unwrap().push_back(reply);
    }

    fn actions(&self) -> Vec<RecordedAction> {
        self.actions.lock().unwrap().clone()
    }

    fn do_get_calls(&self) -> usize {
        self.do_get_calls.load(Ordering::SeqCst)
    }
}

#[tonic::async_trait]
impl FlightService for RecordingFlightActionService {
    type HandshakeStream = FlightStream<HandshakeResponse>;

    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    type ListFlightsStream = FlightStream<FlightInfo>;

    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list flights"))
    }

    async fn get_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get flight info"))
    }

    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll flight info"))
    }

    async fn get_schema(
        &self,
        _: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get schema"))
    }

    type DoGetStream = FlightStream<FlightData>;

    async fn do_get(
        &self,
        _: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        self.do_get_calls.fetch_add(1, Ordering::SeqCst);
        Err(Status::unimplemented("do get"))
    }

    type DoPutStream = FlightStream<PutResult>;

    async fn do_put(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do put"))
    }

    type DoExchangeStream = FlightStream<FlightData>;

    async fn do_exchange(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do exchange"))
    }

    type DoActionStream = FlightStream<FlightResult>;

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        let timeout = request
            .metadata()
            .get("grpc-timeout")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let action = request.into_inner();
        self.actions.lock().unwrap().push(RecordedAction {
            action_type: action.r#type,
            body: action.body.to_vec(),
            timeout,
        });

        match self.replies.lock().unwrap().pop_front() {
            Some(ScriptedReply::RpcError(status)) => Err(status),
            Some(ScriptedReply::Items(items)) => Ok(Response::new(Box::pin(stream::iter(items)))),
            Some(ScriptedReply::Waking {
                blocked,
                release,
                items,
            }) => Ok(Response::new(Box::pin(WakingStream {
                items: items.into(),
                blocked: Some(blocked),
                release: Some(release),
            }))),
            Some(ScriptedReply::Stalling {
                first,
                blocked,
                dropped,
                delivered,
            }) => Ok(Response::new(Box::pin(StallingStream {
                first,
                blocked: Some(blocked),
                dropped: Some(dropped),
                delivered,
            }))),
            None => Err(Status::internal("no scripted reply")),
        }
    }

    type ListActionsStream = FlightStream<ActionType>;

    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list actions"))
    }
}

/// A Flight test server on an ephemeral port.
struct FlightTestServer {
    addr: String,
    service: RecordingFlightActionService,
    task: JoinHandle<()>,
}

impl FlightTestServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let service = RecordingFlightActionService::default();
        let served = service.clone();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(FlightServiceServer::new(served))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        Self {
            addr,
            service,
            task,
        }
    }

    /// Builds a region requester whose query lane points at this test server.
    fn requester(&self) -> RegionRequester {
        RegionRequester::new(
            Client::with_query_and_control_managers(
                ChannelManager::with_config(ChannelConfig::default(), None),
                ChannelManager::with_config(ChannelConfig::default(), None),
                [self.addr.clone()],
            ),
            false,
            false,
        )
    }
}

impl Drop for FlightTestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn binding_fixture() -> RawSeriesRequest {
    RawSeriesRequest::from_extension_value(include_str!(
        "../../../../tests/compatibility/wire/raw_series_request/v1_request.json"
    ))
    .unwrap()
}

fn prepare_body(ordering: SeriesOrdering) -> Vec<u8> {
    encode_prepare_raw_series_response(Some(&PreparedNamedRawSeries {
        binding: binding_fixture(),
        ordering,
    }))
    .unwrap()
}

fn unsupported_body() -> Vec<u8> {
    encode_prepare_raw_series_response(None).unwrap()
}

fn result_item(body: Vec<u8>) -> FlightItem {
    Ok(FlightResult { body: body.into() })
}

/// Builds the region request header, including its context, snapshot, and read preference.
fn request_header(region_id: RegionId) -> RegionRequestHeader {
    let mut context = ApiQueryContext {
        current_catalog: "greptime".to_string(),
        current_schema: "public".to_string(),
        timezone: "UTC".to_string(),
        channel: 1,
        snapshot_seqs: Some(SnapshotSequences {
            sst_min_sequences: HashMap::from([(region_id.as_u64(), 10)]),
            snapshot_seqs: HashMap::from([(region_id.as_u64(), 10)]),
        }),
        ..Default::default()
    };
    context.extensions.insert(
        READ_PREFERENCE_EXTENSION_KEY.to_string(),
        "leader".to_string(),
    );
    RegionRequestHeader {
        tracing_context: HashMap::from([("traceparent".to_string(), "00-trace".to_string())]),
        dbname: "greptime-public".to_string(),
        query_context: Some(context),
    }
}

/// Builds a query request with a context, a snapshot, and a real plan.
///
/// The `client` crate cannot name DataFusion's `LogicalPlanBuilder`, so the plan comes
/// from the SQL planner through the query engine's session state. The result is a real
/// Substrait-encodable plan -- a `Projection` over a `Filter` over an `EmptyRelation`
/// with `produce_one_row` -- rather than a placeholder empty relation.
async fn request_with_real_plan(region_id: RegionId) -> QueryRequest {
    let engine = QueryEngineFactory::new(
        DummyCatalogManager::arc(),
        None,
        None,
        None,
        None,
        false,
        QueryOptions::default(),
    )
    .query_engine();
    let session = engine.engine_state().session_state();
    let plan = session
        .create_logical_plan(REQUEST_PLAN_SQL)
        .await
        .expect("the request plan must be plannable");

    QueryRequest {
        header: Some(request_header(region_id)),
        region_id,
        plan,
    }
}

/// Returns the client error behind a preflight failure.
fn client_error(error: &common_meta::error::Error) -> &Error {
    let common_meta::error::Error::External { source, .. } = error else {
        panic!("expected the preflight failure to wrap a client error, got {error:?}");
    };
    source
        .as_any()
        .downcast_ref::<Error>()
        .expect("expected the wrapped error to be the client error")
}

fn assert_illegal_flight_messages(error: &common_meta::error::Error) {
    assert!(
        matches!(client_error(error), Error::IllegalFlightMessages { .. }),
        "expected an illegal Flight message error, got {error:?}"
    );
}

fn assert_region_server_error(
    error: &common_meta::error::Error,
    addr: &str,
    tonic_code: tonic::Code,
) {
    match client_error(error) {
        Error::RegionServer {
            addr: error_addr,
            code,
            ..
        } => {
            assert_eq!(addr, error_addr.as_str());
            assert_eq!(tonic_code, *code);
        }
        other => panic!("expected a region server error, got {other:?}"),
    }
}

/// Waits until the scripted response stream reports that the client waits for an item.
async fn wait_until_blocked(blocked: oneshot::Receiver<()>) {
    timeout(SERVICE_REPORT_BOUND, blocked)
        .await
        .expect("the response stream must be polled before the preflight budget elapses")
        .expect("the test service must report the stalled response stream");
}

#[tokio::test]
async fn named_preflight_forwards_existing_query_request() {
    let server = FlightTestServer::start().await;
    server
        .service
        .script(ScriptedReply::Items(vec![result_item(prepare_body(
            SeriesOrdering::Contiguous,
        ))]));
    let requester = server.requester();

    let request = request_with_real_plan(RegionId::new(1, 1)).await;
    let rendered = request.plan.display_indent().to_string();
    for node in ["Projection", "Filter", "EmptyRelation"] {
        assert!(
            rendered.contains(node),
            "the fixture plan must contain a {node} node: {rendered}"
        );
    }
    let expected_header = request.header.clone();
    let expected_plan = DFLogicalSubstraitConvertor
        .encode(&request.plan, DefaultSerializer)
        .expect("the fixture plan must encode")
        .to_vec();
    assert!(!expected_plan.is_empty());

    let prepared = Datanode::prepare_named_raw_series_query(&requester, request)
        .await
        .expect("a supported preflight must succeed")
        .expect("a supported preflight must carry a binding");

    assert_eq!(SeriesOrdering::Contiguous, prepared.ordering);
    assert_eq!(
        binding_fixture().to_extension_value().unwrap(),
        prepared.binding.to_extension_value().unwrap()
    );

    // The action carries the existing region query request unmodified.
    let actions = server.service.actions();
    assert_eq!(1, actions.len());
    assert_eq!(PREPARE_RAW_SERIES_ACTION, actions[0].action_type.as_str());
    assert!(
        actions[0].timeout.is_some(),
        "the request timeout header must be retained"
    );
    let received = PbQueryRequest::decode(actions[0].body.as_slice()).unwrap();
    assert_eq!(RegionId::new(1, 1).as_u64(), received.region_id);
    assert_eq!(expected_header, received.header);
    assert_eq!(expected_plan, received.plan);

    // The preflight never falls back to a Flight DoGet.
    assert_eq!(0, server.service.do_get_calls());
}

#[tokio::test]
async fn named_preflight_returns_both_supported_orderings() {
    let server = FlightTestServer::start().await;
    let requester = server.requester();
    let region_id = binding_fixture().physical_metadata.region_id;

    for ordering in [SeriesOrdering::Contiguous, SeriesOrdering::KeyThenTime] {
        server
            .service
            .script(ScriptedReply::Items(vec![result_item(prepare_body(
                ordering,
            ))]));

        let prepared = Datanode::prepare_named_raw_series_query(
            &requester,
            request_with_real_plan(region_id).await,
        )
        .await
        .expect("a supported preflight must succeed")
        .expect("a supported preflight must carry a binding");

        assert_eq!(ordering, prepared.ordering);
        assert_eq!(
            binding_fixture().to_extension_value().unwrap(),
            prepared.binding.to_extension_value().unwrap()
        );
    }

    assert_eq!(2, server.service.actions().len());
}

#[tokio::test]
async fn named_preflight_returns_none_for_unsupported_body() {
    let server = FlightTestServer::start().await;
    server
        .service
        .script(ScriptedReply::Items(vec![result_item(unsupported_body())]));
    let requester = server.requester();
    let region_id = binding_fixture().physical_metadata.region_id;

    let prepared = Datanode::prepare_named_raw_series_query(
        &requester,
        request_with_real_plan(region_id).await,
    )
    .await
    .expect("an explicit null body with a clean end of stream is not a failure")
    .is_none();

    assert!(prepared, "the explicit null body must be reported as None");
    assert_eq!(1, server.service.actions().len());
    assert_eq!(0, server.service.do_get_calls());
}

#[tokio::test]
async fn named_preflight_rejects_missing_result() {
    let server = FlightTestServer::start().await;
    server.service.script(ScriptedReply::Items(Vec::new()));
    let requester = server.requester();
    let region_id = binding_fixture().physical_metadata.region_id;

    let error = Datanode::prepare_named_raw_series_query(
        &requester,
        request_with_real_plan(region_id).await,
    )
    .await
    .expect_err("a missing result must fail");

    assert_illegal_flight_messages(&error);
}

#[tokio::test]
async fn named_preflight_rejects_extra_results() {
    let server = FlightTestServer::start().await;
    let requester = server.requester();
    let region_id = binding_fixture().physical_metadata.region_id;

    // A supported body plus an extra result.
    server.service.script(ScriptedReply::Items(vec![
        result_item(prepare_body(SeriesOrdering::Contiguous)),
        result_item(prepare_body(SeriesOrdering::KeyThenTime)),
    ]));
    let error = Datanode::prepare_named_raw_series_query(
        &requester,
        request_with_real_plan(region_id).await,
    )
    .await
    .expect_err("an extra result must fail");
    assert_illegal_flight_messages(&error);

    // An explicit null body plus an extra result: still not the unsupported answer.
    server.service.script(ScriptedReply::Items(vec![
        result_item(unsupported_body()),
        result_item(unsupported_body()),
    ]));
    let error = Datanode::prepare_named_raw_series_query(
        &requester,
        request_with_real_plan(region_id).await,
    )
    .await
    .expect_err("an explicit null body with an extra result must fail");
    assert_illegal_flight_messages(&error);

    assert_eq!(2, server.service.actions().len());
}

#[tokio::test]
async fn named_preflight_rejects_invalid_response_bodies() {
    let server = FlightTestServer::start().await;
    let requester = server.requester();
    let region_id = binding_fixture().physical_metadata.region_id;
    let supported = String::from_utf8(prepare_body(SeriesOrdering::Contiguous)).unwrap();

    let cases = [
        ("empty body", String::new()),
        ("not json", "{".to_string()),
        (
            "unknown version",
            supported.replace(r#""version":1"#, r#""version":2"#),
        ),
        (
            "missing ordering",
            supported.replace(r#","ordering":"contiguous""#, ""),
        ),
        (
            "unknown ordering",
            supported.replace(r#""ordering":"contiguous""#, r#""ordering":"future""#),
        ),
        (
            "interleaved ordering",
            supported.replace(r#""ordering":"contiguous""#, r#""ordering":"interleaved""#),
        ),
        (
            "unknown envelope field",
            supported.replace(r#""version":1"#, r#""version":1,"extra":true"#),
        ),
    ];

    let case_count = cases.len();
    for (name, candidate) in cases {
        server.service.script(ScriptedReply::Items(vec![result_item(
            candidate.into_bytes(),
        )]));
        let error = Datanode::prepare_named_raw_series_query(
            &requester,
            request_with_real_plan(region_id).await,
        )
        .await
        .err()
        .unwrap_or_else(|| panic!("an invalid response body must fail: {name}"));
        assert_illegal_flight_messages(&error);
    }

    assert_eq!(case_count, server.service.actions().len());
}

#[tokio::test]
async fn named_preflight_declines_bare_unimplemented() {
    let server = FlightTestServer::start().await;
    server
        .service
        .script(ScriptedReply::RpcError(Status::unimplemented(
            "no named preflight here",
        )));
    let requester = server.requester();
    let region_id = binding_fixture().physical_metadata.region_id;

    let prepared = Datanode::prepare_named_raw_series_query(
        &requester,
        request_with_real_plan(region_id).await,
    )
    .await
    .expect("a bare Unimplemented means unsupported, not failed")
    .is_none();

    assert!(prepared);
    assert_eq!(1, server.service.actions().len());
}

#[tokio::test]
async fn named_preflight_reports_unimplemented_with_error_metadata() {
    let server = FlightTestServer::start().await;
    let requester = server.requester();
    let region_id = binding_fixture().physical_metadata.region_id;

    // A valid error code in the metadata is a real failure.
    let mut status = Status::unimplemented("preflight failed");
    status.metadata_mut().insert(
        GREPTIME_DB_HEADER_ERROR_CODE,
        (StatusCode::RegionNotReady as u32)
            .to_string()
            .parse()
            .unwrap(),
    );
    server.service.script(ScriptedReply::RpcError(status));
    let error = Datanode::prepare_named_raw_series_query(
        &requester,
        request_with_real_plan(region_id).await,
    )
    .await
    .expect_err("an Unimplemented status with error metadata must fail");
    assert_eq!(StatusCode::RegionNotReady, error.status_code());
    assert_region_server_error(&error, &server.addr, tonic::Code::Unimplemented);

    // An error-code metadata that cannot be parsed is still a real failure.
    let mut status = Status::unimplemented("preflight failed");
    status.metadata_mut().insert(
        GREPTIME_DB_HEADER_ERROR_CODE,
        "not-a-status-code".parse().unwrap(),
    );
    server.service.script(ScriptedReply::RpcError(status));
    let error = Datanode::prepare_named_raw_series_query(
        &requester,
        request_with_real_plan(region_id).await,
    )
    .await
    .expect_err("error metadata must never be read as unsupported");
    assert_eq!(StatusCode::Internal, error.status_code());
    assert_region_server_error(&error, &server.addr, tonic::Code::Unimplemented);
}

#[tokio::test]
async fn named_preflight_propagates_transport_error() {
    let server = FlightTestServer::start().await;
    server
        .service
        .script(ScriptedReply::RpcError(Status::unavailable(
            "datanode is down",
        )));
    let requester = server.requester();
    let region_id = binding_fixture().physical_metadata.region_id;

    let error = Datanode::prepare_named_raw_series_query(
        &requester,
        request_with_real_plan(region_id).await,
    )
    .await
    .expect_err("a transport failure must not be read as unsupported");

    assert_eq!(StatusCode::Internal, error.status_code());
    assert_region_server_error(&error, &server.addr, tonic::Code::Unavailable);
    assert!(format!("{error:?}").contains("datanode is down"));
}

#[tokio::test]
async fn named_preflight_propagates_stream_unimplemented_after_headers() {
    let server = FlightTestServer::start().await;
    let (blocked_tx, blocked_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    server.service.script(ScriptedReply::Waking {
        blocked: blocked_tx,
        release: release_rx,
        items: vec![Err(Status::unimplemented(
            "the action failed after the response started",
        ))],
    });
    let requester = server.requester();
    let region_id = binding_fixture().physical_metadata.region_id;
    let request = request_with_real_plan(region_id).await;

    let mut preflight = Box::pin(
        requester.prepare_named_raw_series_query_with_timeout(request, CANCELLATION_TEST_BUDGET),
    );
    // Hold the response open before the error: the client is already reading the body,
    // so the following Unimplemented is a stream error and never the initial call
    // rejection that the client reports as unsupported.
    tokio::select! {
        result = &mut preflight => panic!(
            "the held preflight must not answer before the error is released: {result:?}"
        ),
        _ = wait_until_blocked(blocked_rx) => {}
    }

    release_tx
        .send(())
        .expect("the response stream must still be waiting");

    let error = preflight
        .await
        .expect_err("a stream Unimplemented must never be read as unsupported");
    assert_region_server_error(&error, &server.addr, tonic::Code::Unimplemented);
    assert!(format!("{error:?}").contains("the action failed after the response started"));
}

#[tokio::test]
async fn named_preflight_rejects_tail_error_after_supported_body() {
    let server = FlightTestServer::start().await;
    server.service.script(ScriptedReply::Items(vec![
        result_item(prepare_body(SeriesOrdering::Contiguous)),
        Err(Status::internal("tail boom")),
    ]));
    let requester = server.requester();
    let region_id = binding_fixture().physical_metadata.region_id;

    let error = Datanode::prepare_named_raw_series_query(
        &requester,
        request_with_real_plan(region_id).await,
    )
    .await
    .expect_err("a tail error must fail the preflight");

    assert_eq!(StatusCode::Internal, error.status_code());
    assert_region_server_error(&error, &server.addr, tonic::Code::Internal);
    assert!(format!("{error:?}").contains("tail boom"));
}

#[tokio::test]
async fn named_preflight_rejects_tail_error_after_null_body() {
    let server = FlightTestServer::start().await;
    server.service.script(ScriptedReply::Items(vec![
        result_item(unsupported_body()),
        Err(Status::internal("tail boom after null")),
    ]));
    let requester = server.requester();
    let region_id = binding_fixture().physical_metadata.region_id;

    let error = Datanode::prepare_named_raw_series_query(
        &requester,
        request_with_real_plan(region_id).await,
    )
    .await
    .expect_err("an explicit null body must not be the unsupported answer without a clean end");

    assert_eq!(StatusCode::Internal, error.status_code());
    assert_region_server_error(&error, &server.addr, tonic::Code::Internal);
    assert!(format!("{error:?}").contains("tail boom after null"));
}

#[tokio::test]
async fn named_preflight_deadline_covers_stall_before_first_result() {
    let server = FlightTestServer::start().await;
    let (blocked_tx, blocked_rx) = oneshot::channel();
    let (dropped_tx, _dropped_rx) = oneshot::channel();
    server.service.script(ScriptedReply::Stalling {
        first: None,
        blocked: blocked_tx,
        dropped: dropped_tx,
        delivered: Arc::new(AtomicUsize::new(0)),
    });
    let requester = server.requester();
    let region_id = binding_fixture().physical_metadata.region_id;
    let request = request_with_real_plan(region_id).await;

    let mut preflight = Box::pin(
        requester.prepare_named_raw_series_query_with_timeout(request, DEADLINE_TEST_BUDGET),
    );
    tokio::select! {
        result = &mut preflight => panic!(
            "the stalled preflight must not answer before its deadline is observed: {result:?}"
        ),
        _ = wait_until_blocked(blocked_rx) => {}
    }

    let error = preflight
        .await
        .expect_err("a stalled preflight must hit its deadline");

    assert_eq!(StatusCode::DeadlineExceeded, error.status_code());
    assert_region_server_error(&error, &server.addr, tonic::Code::DeadlineExceeded);
    assert!(format!("{error:?}").contains(PREPARE_RAW_SERIES_ACTION));
}

#[tokio::test]
async fn named_preflight_deadline_covers_stall_after_first_result() {
    let server = FlightTestServer::start().await;
    let (blocked_tx, blocked_rx) = oneshot::channel();
    let (dropped_tx, _dropped_rx) = oneshot::channel();
    let delivered = Arc::new(AtomicUsize::new(0));
    server.service.script(ScriptedReply::Stalling {
        first: Some(FlightResult {
            body: prepare_body(SeriesOrdering::Contiguous).into(),
        }),
        blocked: blocked_tx,
        dropped: dropped_tx,
        delivered: delivered.clone(),
    });
    let requester = server.requester();
    let region_id = binding_fixture().physical_metadata.region_id;
    let request = request_with_real_plan(region_id).await;

    let mut preflight = Box::pin(
        requester.prepare_named_raw_series_query_with_timeout(request, DEADLINE_TEST_BUDGET),
    );
    tokio::select! {
        result = &mut preflight => panic!(
            "the stalled preflight must not answer before its deadline is observed: {result:?}"
        ),
        _ = wait_until_blocked(blocked_rx) => {}
    }

    let error = preflight.await.expect_err(
        "a preflight stalled between its result and the end of the response must hit its deadline",
    );

    assert_eq!(
        1,
        delivered.load(Ordering::SeqCst),
        "the service must have delivered the first result before the client stalled on the next read"
    );
    assert_eq!(StatusCode::DeadlineExceeded, error.status_code());
    assert_region_server_error(&error, &server.addr, tonic::Code::DeadlineExceeded);
}

#[tokio::test]
async fn named_preflight_drop_releases_stream_stalled_before_first_result() {
    let server = FlightTestServer::start().await;
    let (blocked_tx, blocked_rx) = oneshot::channel();
    let (dropped_tx, dropped_rx) = oneshot::channel();
    server.service.script(ScriptedReply::Stalling {
        first: None,
        blocked: blocked_tx,
        dropped: dropped_tx,
        delivered: Arc::new(AtomicUsize::new(0)),
    });
    let requester = server.requester();
    let region_id = binding_fixture().physical_metadata.region_id;
    let request = request_with_real_plan(region_id).await;

    let mut preflight = Box::pin(
        requester.prepare_named_raw_series_query_with_timeout(request, CANCELLATION_TEST_BUDGET),
    );
    tokio::select! {
        result = &mut preflight => panic!("the stalled preflight must not answer: {result:?}"),
        _ = wait_until_blocked(blocked_rx) => {}
    }

    drop(preflight);

    timeout(SERVICE_REPORT_BOUND, dropped_rx)
        .await
        .expect("dropping the client future must release the server response stream")
        .expect("the test service must report the released response stream");
    assert_eq!(0, server.service.do_get_calls());
}

#[tokio::test]
async fn named_preflight_drop_releases_stream_stalled_after_first_result() {
    let server = FlightTestServer::start().await;
    let (blocked_tx, blocked_rx) = oneshot::channel();
    let (dropped_tx, dropped_rx) = oneshot::channel();
    let delivered = Arc::new(AtomicUsize::new(0));
    server.service.script(ScriptedReply::Stalling {
        first: Some(FlightResult {
            body: prepare_body(SeriesOrdering::Contiguous).into(),
        }),
        blocked: blocked_tx,
        dropped: dropped_tx,
        delivered: delivered.clone(),
    });
    let requester = server.requester();
    let region_id = binding_fixture().physical_metadata.region_id;
    let request = request_with_real_plan(region_id).await;

    let mut preflight = Box::pin(
        requester.prepare_named_raw_series_query_with_timeout(request, CANCELLATION_TEST_BUDGET),
    );
    tokio::select! {
        result = &mut preflight => panic!("the stalled preflight must not answer: {result:?}"),
        _ = wait_until_blocked(blocked_rx) => {}
    }

    drop(preflight);

    timeout(SERVICE_REPORT_BOUND, dropped_rx)
        .await
        .expect("dropping the client future must release the server response stream")
        .expect("the test service must report the released response stream");
    assert_eq!(
        1,
        delivered.load(Ordering::SeqCst),
        "the first result must have been delivered before the cancel"
    );
    assert_eq!(0, server.service.do_get_calls());
}
