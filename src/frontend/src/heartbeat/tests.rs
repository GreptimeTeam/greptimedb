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
use std::future::pending;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use api::v1::meta::heartbeat_request::NodeWorkloads;
use api::v1::meta::mailbox_message::Payload;
use api::v1::meta::{
    HeartbeatConfig as ApiHeartbeatConfig, HeartbeatRequest, HeartbeatResponse, MailboxMessage,
    RegionLease, ResponseHeader, Role,
};
use async_trait::async_trait;
use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_meta::cache_invalidator::KvCacheInvalidator;
use common_meta::heartbeat::handler::invalidate_table_cache::InvalidateCacheHandler;
use common_meta::heartbeat::handler::{
    HandleControl, HandlerGroupExecutor, HeartbeatResponseHandler, HeartbeatResponseHandlerContext,
    HeartbeatResponseHandlerExecutor, HeartbeatResponseHandlerRef,
};
use common_meta::heartbeat::mailbox::{HeartbeatMailbox, MessageMeta};
use common_meta::instruction::{CacheIdent, Instruction};
use common_meta::key::MetadataKey;
use common_meta::key::schema_name::{SchemaName, SchemaNameKey};
use common_meta::key::table_info::TableInfoKey;
use common_stat::ResourceStatImpl;
use common_telemetry::tracing_context::TracingContext;
use meta_client::client::MetaClient;
use prost::Message;
use tokio::sync::{Notify, mpsc};

use super::*;
use crate::frontend::FrontendOptions;

#[derive(Default)]
pub struct MockKvCacheInvalidator {
    inner: Mutex<HashMap<Vec<u8>, i32>>,
}

#[derive(Clone, PartialEq, Message)]
struct LegacyHeartbeatResponse {
    #[prost(message, optional, tag = "1")]
    header: Option<ResponseHeader>,
    #[prost(message, optional, tag = "2")]
    mailbox_message: Option<MailboxMessage>,
    #[prost(message, optional, tag = "3")]
    region_lease: Option<RegionLease>,
    #[prost(message, optional, tag = "4")]
    heartbeat_config: Option<ApiHeartbeatConfig>,
}

#[async_trait::async_trait]
impl KvCacheInvalidator for MockKvCacheInvalidator {
    async fn invalidate_key(&self, key: &[u8]) {
        let _ = self.inner.lock().unwrap().remove(key);
    }
}

pub fn test_message_meta(id: u64, subject: &str, to: &str, from: &str) -> MessageMeta {
    MessageMeta {
        id,
        subject: subject.to_string(),
        to: to.to_string(),
        from: from.to_string(),
    }
}

async fn handle_instruction(
    executor: Arc<dyn HeartbeatResponseHandlerExecutor>,
    mailbox: Arc<HeartbeatMailbox>,
    instruction: Instruction,
) {
    let response = HeartbeatResponse::default();
    let mut ctx: HeartbeatResponseHandlerContext =
        HeartbeatResponseHandlerContext::new(mailbox, response);
    ctx.incoming_message = Some((
        test_message_meta(1, "hi", "foo", "bar"),
        TracingContext::new(),
        instruction,
    ));
    executor.handle(ctx).await.unwrap();
}

#[tokio::test]
async fn test_invalidate_table_cache_handler() {
    let table_id = 1;
    let table_info_key = TableInfoKey::new(table_id);
    let inner = HashMap::from([(table_info_key.to_bytes(), 1)]);
    let backend = Arc::new(MockKvCacheInvalidator {
        inner: Mutex::new(inner),
    });

    let executor = Arc::new(HandlerGroupExecutor::new(vec![Arc::new(
        InvalidateCacheHandler::new(backend.clone()),
    )]));

    let (tx, _) = mpsc::channel(8);
    let mailbox = Arc::new(HeartbeatMailbox::new(tx));

    // removes a valid key
    handle_instruction(
        executor.clone(),
        mailbox.clone(),
        Instruction::InvalidateCaches(vec![CacheIdent::TableId(table_id)]),
    )
    .await;

    assert!(
        !backend
            .inner
            .lock()
            .unwrap()
            .contains_key(&table_info_key.to_bytes())
    );

    // removes a invalid key
    handle_instruction(
        executor,
        mailbox,
        Instruction::InvalidateCaches(vec![CacheIdent::TableId(0)]),
    )
    .await;
}

#[tokio::test]
async fn test_invalidate_schema_key_handler() {
    let (catalog, schema) = ("foo", "bar");
    let schema_key = SchemaNameKey { catalog, schema };
    let inner = HashMap::from([(schema_key.to_bytes(), 1)]);
    let backend = Arc::new(MockKvCacheInvalidator {
        inner: Mutex::new(inner),
    });

    let executor = Arc::new(HandlerGroupExecutor::new(vec![Arc::new(
        InvalidateCacheHandler::new(backend.clone()),
    )]));

    let (tx, _) = mpsc::channel(8);
    let mailbox = Arc::new(HeartbeatMailbox::new(tx));

    // removes a valid key
    let valid_key = SchemaName {
        catalog_name: catalog.to_string(),
        schema_name: schema.to_string(),
    };
    handle_instruction(
        executor.clone(),
        mailbox.clone(),
        Instruction::InvalidateCaches(vec![CacheIdent::SchemaName(valid_key.clone())]),
    )
    .await;

    assert!(
        !backend
            .inner
            .lock()
            .unwrap()
            .contains_key(&schema_key.to_bytes())
    );

    // removes a invalid key
    handle_instruction(
        executor,
        mailbox,
        Instruction::InvalidateCaches(vec![CacheIdent::SchemaName(valid_key)]),
    )
    .await;
}

#[test]
fn test_heartbeat_task_uses_resolved_peer_addr() {
    let options = FrontendOptions::default();
    let meta_client = Arc::new(MetaClient::new(0, Role::Frontend));
    let executor = Arc::new(HandlerGroupExecutor::new(vec![]));
    let stat = Arc::new(ResourceStatImpl::default());

    let task = HeartbeatTask::new(
        "10.0.0.1:4001".to_string(),
        &options,
        meta_client,
        executor,
        stat,
    );

    assert_eq!(task.runner.peer_addr, "10.0.0.1:4001");
}

enum ConnectPlan {
    Ready(MockConnectionPlan),
    Fail,
    Pending,
}

struct MockConnector {
    plans: Mutex<VecDeque<ConnectPlan>>,
    calls: AtomicUsize,
    called: Notify,
}

impl MockConnector {
    fn new(plans: impl IntoIterator<Item = ConnectPlan>) -> Arc<Self> {
        Arc::new(Self {
            plans: Mutex::new(plans.into_iter().collect()),
            calls: AtomicUsize::new(0),
            called: Notify::new(),
        })
    }

    async fn wait_for_calls(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if self.calls.load(Ordering::Acquire) >= expected {
                    return;
                }
                let notified = self.called.notified();
                if self.calls.load(Ordering::Acquire) >= expected {
                    return;
                }
                notified.await;
            }
        })
        .await
        .unwrap();
    }
}

#[async_trait]
impl HeartbeatConnector for MockConnector {
    async fn connect(&self) -> Result<HeartbeatConnection> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        self.called.notify_waiters();
        let plan = self.plans.lock().unwrap().pop_front();
        match plan {
            Some(ConnectPlan::Ready(plan)) => Ok(plan.into_connection()),
            Some(ConnectPlan::Fail) => Err(crate::error::Error::NotSupported {
                feat: "mock heartbeat connection".to_string(),
            }),
            Some(ConnectPlan::Pending) | None => pending().await,
        }
    }
}

struct MockConnectionPlan {
    sender: MockSender,
    response_rx: mpsc::UnboundedReceiver<MockStreamEvent>,
    config: HeartbeatConfig,
}

impl MockConnectionPlan {
    fn into_connection(self) -> HeartbeatConnection {
        HeartbeatConnection {
            sender: Arc::new(self.sender),
            stream: Box::new(MockStream {
                receiver: self.response_rx,
            }),
            config: self.config,
        }
    }
}

#[derive(Clone)]
struct MockConnectionHandle {
    requests: Arc<Mutex<Vec<HeartbeatRequest>>>,
    request_added: Arc<Notify>,
    response_tx: mpsc::UnboundedSender<MockStreamEvent>,
    fail_send: Arc<AtomicBool>,
}

impl MockConnectionHandle {
    async fn wait_for_requests(&self, expected: usize) -> Vec<HeartbeatRequest> {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let requests = self.requests.lock().unwrap().clone();
                if requests.len() >= expected {
                    return requests;
                }
                let notified = self.request_added.notified();
                if self.requests.lock().unwrap().len() >= expected {
                    continue;
                }
                notified.await;
            }
        })
        .await
        .unwrap()
    }

    fn close_responses(&self) {
        self.response_tx.send(MockStreamEvent::Close).unwrap();
    }

    fn fail_responses(&self) {
        self.response_tx.send(MockStreamEvent::Error).unwrap();
    }

    fn send_response(&self, response: HeartbeatResponse) {
        self.response_tx
            .send(MockStreamEvent::Response(response))
            .unwrap();
    }
}

#[derive(Clone)]
struct MockSender {
    requests: Arc<Mutex<Vec<HeartbeatRequest>>>,
    request_added: Arc<Notify>,
    fail_send: Arc<AtomicBool>,
}

#[async_trait]
impl HeartbeatRequestSender for MockSender {
    async fn send(&self, request: HeartbeatRequest) -> HeartbeatExtensionResult<()> {
        if self.fail_send.load(Ordering::Acquire) {
            return Err(test_error("mock heartbeat send failure"));
        }
        self.requests.lock().unwrap().push(request);
        self.request_added.notify_waiters();
        Ok(())
    }
}

enum MockStreamEvent {
    Response(HeartbeatResponse),
    Close,
    Error,
}

struct MockStream {
    receiver: mpsc::UnboundedReceiver<MockStreamEvent>,
}

#[async_trait]
impl HeartbeatResponseStream for MockStream {
    async fn message(&mut self) -> HeartbeatExtensionResult<Option<HeartbeatResponse>> {
        match self.receiver.recv().await {
            Some(MockStreamEvent::Response(response)) => Ok(Some(response)),
            Some(MockStreamEvent::Close) | None => Ok(None),
            Some(MockStreamEvent::Error) => Err(test_error("mock heartbeat receive failure")),
        }
    }
}

fn mock_connection(
    interval: Duration,
    retry_interval: Duration,
) -> (MockConnectionPlan, MockConnectionHandle) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_added = Arc::new(Notify::new());
    let fail_send = Arc::new(AtomicBool::new(false));
    let (response_tx, response_rx) = mpsc::unbounded_channel();
    let sender = MockSender {
        requests: requests.clone(),
        request_added: request_added.clone(),
        fail_send: fail_send.clone(),
    };
    let handle = MockConnectionHandle {
        requests,
        request_added,
        response_tx,
        fail_send,
    };
    (
        MockConnectionPlan {
            sender,
            response_rx,
            config: HeartbeatConfig {
                interval,
                retry_interval,
                gc_enabled: false,
            },
        },
        handle,
    )
}

fn test_error(message: &str) -> BoxedError {
    BoxedError::new(PlainError::new(message.to_string(), StatusCode::Unexpected))
}

struct TestExtension {
    name: String,
    static_extensions: HashMap<String, Vec<u8>>,
    dynamic_key: Option<String>,
    fail_request: AtomicBool,
    request_calls: AtomicUsize,
    connected_generations: Mutex<Vec<u64>>,
    shutdown_calls: AtomicUsize,
    response_handler: Option<HeartbeatResponseHandlerRef>,
}

impl TestExtension {
    fn new(name: &str) -> Arc<Self> {
        Arc::new(Self {
            name: name.to_string(),
            static_extensions: HashMap::new(),
            dynamic_key: None,
            fail_request: AtomicBool::new(false),
            request_calls: AtomicUsize::new(0),
            connected_generations: Mutex::new(Vec::new()),
            shutdown_calls: AtomicUsize::new(0),
            response_handler: None,
        })
    }

    fn with_static(name: &str, static_extensions: HashMap<String, Vec<u8>>) -> Arc<Self> {
        Arc::new(Self {
            static_extensions,
            ..Self::new_fields(name)
        })
    }

    fn dynamic(name: &str, key: &str) -> Arc<Self> {
        Arc::new(Self {
            dynamic_key: Some(key.to_string()),
            ..Self::new_fields(name)
        })
    }

    fn failing(name: &str) -> Arc<Self> {
        Arc::new(Self {
            fail_request: AtomicBool::new(true),
            ..Self::new_fields(name)
        })
    }

    fn with_handler(name: &str, response_handler: HeartbeatResponseHandlerRef) -> Arc<Self> {
        Arc::new(Self {
            response_handler: Some(response_handler),
            ..Self::new_fields(name)
        })
    }

    fn new_fields(name: &str) -> Self {
        Self {
            name: name.to_string(),
            static_extensions: HashMap::new(),
            dynamic_key: None,
            fail_request: AtomicBool::new(false),
            request_calls: AtomicUsize::new(0),
            connected_generations: Mutex::new(Vec::new()),
            shutdown_calls: AtomicUsize::new(0),
            response_handler: None,
        }
    }
}

#[async_trait]
impl HeartbeatExtension for TestExtension {
    fn name(&self) -> &str {
        &self.name
    }

    async fn request_extensions(&self) -> HeartbeatExtensionResult<HashMap<String, Vec<u8>>> {
        let call = self.request_calls.fetch_add(1, Ordering::AcqRel) + 1;
        if self.fail_request.load(Ordering::Acquire) {
            return Err(test_error("mock extension failure"));
        }
        let mut extensions = self.static_extensions.clone();
        if let Some(key) = &self.dynamic_key {
            extensions.insert(key.clone(), call.to_string().into_bytes());
        }
        Ok(extensions)
    }

    fn response_handler(&self) -> Option<HeartbeatResponseHandlerRef> {
        self.response_handler.clone()
    }

    async fn connected(&self, generation: u64) -> HeartbeatExtensionResult<()> {
        self.connected_generations.lock().unwrap().push(generation);
        Ok(())
    }

    async fn shutdown(&self) -> HeartbeatExtensionResult<()> {
        self.shutdown_calls.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

fn test_task(
    connector: Arc<dyn HeartbeatConnector>,
    extensions: HeartbeatExtensions,
    options: FrontendOptions,
) -> HeartbeatTask {
    let executor = heartbeat_response_handler_executor(
        &extensions,
        Arc::new(AtomicBool::new(false)),
        Arc::new(MockKvCacheInvalidator::default()),
    );
    HeartbeatTask::new_with_connector(
        "127.0.0.1:4001".to_string(),
        &options,
        connector,
        executor,
        Arc::new(ResourceStatImpl::default()),
    )
    .with_extensions(extensions)
}

#[tokio::test]
async fn test_heartbeat_without_extensions_preserves_base_behavior() {
    let (plan, handle) = mock_connection(Duration::from_secs(60), Duration::from_millis(10));
    let connector = MockConnector::new([ConnectPlan::Ready(plan)]);
    let task = test_task(
        connector,
        HeartbeatExtensions::default(),
        FrontendOptions::default(),
    );

    task.start().await.unwrap();
    let requests = handle.wait_for_requests(1).await;
    let request = &requests[0];
    assert_eq!(request.peer.as_ref().unwrap().addr, "127.0.0.1:4001");
    assert!(request.extensions.is_empty());
    assert!(matches!(
        request.node_workloads,
        Some(NodeWorkloads::Frontend(_))
    ));

    task.shutdown().await;
    assert!(!task.has_supervisor().await);
}

#[tokio::test]
async fn test_initial_connection_failure_does_not_start_extension_lifecycle() {
    let extension = TestExtension::new("lifecycle");
    let extensions = HeartbeatExtensions::default();
    assert!(extensions.register(extension.clone()));
    let connector = MockConnector::new([ConnectPlan::Fail]);
    let task = test_task(connector, extensions, FrontendOptions::default());

    assert!(task.start().await.is_err());
    assert!(extension.connected_generations.lock().unwrap().is_empty());
    assert!(!task.has_supervisor().await);
    task.shutdown().await;
}

#[tokio::test]
async fn test_request_extensions_are_regenerated_for_every_heartbeat() {
    let extension = TestExtension::dynamic("dynamic", "dynamic-key");
    let extensions = HeartbeatExtensions::default();
    extensions.register(extension.clone());
    let (plan, handle) = mock_connection(Duration::from_millis(10), Duration::from_millis(10));
    let task = test_task(
        MockConnector::new([ConnectPlan::Ready(plan)]),
        extensions,
        FrontendOptions::default(),
    );

    task.start().await.unwrap();
    let requests = handle.wait_for_requests(2).await;
    assert_eq!(requests[0].extensions["dynamic-key"], b"1");
    assert_eq!(requests[1].extensions["dynamic-key"], b"2");
    assert_eq!(extension.request_calls.load(Ordering::Acquire), 2);
    task.shutdown().await;
}

#[tokio::test]
async fn test_failed_provider_preserves_base_and_other_extensions() {
    let failing = TestExtension::failing("failing");
    let successful = TestExtension::with_static(
        "successful",
        HashMap::from([("other".to_string(), b"value".to_vec())]),
    );
    let extensions = HeartbeatExtensions::default();
    extensions.register(failing);
    extensions.register(successful);
    let task = test_task(
        MockConnector::new([ConnectPlan::Pending]),
        extensions,
        FrontendOptions::default(),
    );
    let mut request = HeartbeatRequest {
        extensions: HashMap::from([("base".to_string(), b"keep".to_vec())]),
        ..Default::default()
    };

    assert!(task.runner.add_request_extensions(&mut request).await);
    assert_eq!(request.extensions["base"], b"keep");
    assert_eq!(request.extensions["other"], b"value");
}

#[tokio::test]
async fn test_conflicting_provider_output_is_discarded_atomically() {
    let conflicting = TestExtension::with_static(
        "conflicting",
        HashMap::from([
            ("base".to_string(), b"replace".to_vec()),
            ("partial".to_string(), b"discard".to_vec()),
        ]),
    );
    let successful = TestExtension::with_static(
        "successful",
        HashMap::from([("other".to_string(), b"value".to_vec())]),
    );
    let extensions = HeartbeatExtensions::default();
    assert!(extensions.register(conflicting.clone()));
    assert!(!extensions.register(conflicting));
    assert!(extensions.register(successful));
    let task = test_task(
        MockConnector::new([ConnectPlan::Pending]),
        extensions,
        FrontendOptions::default(),
    );
    let mut request = HeartbeatRequest {
        extensions: HashMap::from([("base".to_string(), b"keep".to_vec())]),
        ..Default::default()
    };

    assert!(task.runner.add_request_extensions(&mut request).await);
    assert_eq!(request.extensions["base"], b"keep");
    assert!(!request.extensions.contains_key("partial"));
    assert_eq!(request.extensions["other"], b"value");
}

type HandlerObservations = Arc<Mutex<Vec<(bool, bool, bool, bool)>>>;

struct OrderingHandler {
    observations: HandlerObservations,
    suspend_state: Arc<AtomicBool>,
    cache: Arc<MockKvCacheInvalidator>,
    table_key: Vec<u8>,
}

enum ShortCircuitResult {
    Done,
    Error,
}

struct ShortCircuitHandler {
    result: ShortCircuitResult,
    calls: Arc<AtomicUsize>,
}

struct PendingHandler {
    started: Arc<Notify>,
}

#[async_trait]
impl HeartbeatResponseHandler for PendingHandler {
    fn is_acceptable(&self, _ctx: &HeartbeatResponseHandlerContext) -> bool {
        true
    }

    async fn handle(
        &self,
        _ctx: &mut HeartbeatResponseHandlerContext,
    ) -> common_meta::error::Result<HandleControl> {
        self.started.notify_waiters();
        pending().await
    }
}

#[async_trait]
impl HeartbeatResponseHandler for ShortCircuitHandler {
    fn is_acceptable(&self, _ctx: &HeartbeatResponseHandlerContext) -> bool {
        true
    }

    async fn handle(
        &self,
        _ctx: &mut HeartbeatResponseHandlerContext,
    ) -> common_meta::error::Result<HandleControl> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        match self.result {
            ShortCircuitResult::Done => Ok(HandleControl::Done),
            ShortCircuitResult::Error => common_meta::error::UnsupportedSnafu {
                operation: "mock extension response handler",
            }
            .fail(),
        }
    }
}

#[async_trait]
impl HeartbeatResponseHandler for OrderingHandler {
    fn is_acceptable(&self, _ctx: &HeartbeatResponseHandlerContext) -> bool {
        true
    }

    async fn handle(
        &self,
        ctx: &mut HeartbeatResponseHandlerContext,
    ) -> common_meta::error::Result<HandleControl> {
        self.observations.lock().unwrap().push((
            ctx.incoming_message.is_some(),
            self.suspend_state.load(Ordering::Acquire),
            self.cache
                .inner
                .lock()
                .unwrap()
                .contains_key(&self.table_key),
            ctx.response.extensions.contains_key("response-extension"),
        ));
        Ok(HandleControl::Continue)
    }
}

#[tokio::test]
async fn test_response_extension_handler_order() {
    let table_id = 42;
    let table_key = TableInfoKey::new(table_id).to_bytes();
    let cache = Arc::new(MockKvCacheInvalidator {
        inner: Mutex::new(HashMap::from([(table_key.clone(), 1)])),
    });
    let suspend_state = Arc::new(AtomicBool::new(true));
    let observations = Arc::new(Mutex::new(Vec::new()));
    let handler = Arc::new(OrderingHandler {
        observations: observations.clone(),
        suspend_state: suspend_state.clone(),
        cache: cache.clone(),
        table_key: table_key.clone(),
    });
    let extensions = HeartbeatExtensions::default();
    extensions.register(TestExtension::with_handler("response", handler));
    let executor =
        heartbeat_response_handler_executor(&extensions, suspend_state.clone(), cache.clone());
    let (mailbox_tx, _) = mpsc::channel(1);
    let mailbox = Arc::new(HeartbeatMailbox::new(mailbox_tx));

    executor
        .handle(HeartbeatResponseHandlerContext::new(
            mailbox.clone(),
            HeartbeatResponse {
                extensions: HashMap::from([("response-extension".to_string(), b"value".to_vec())]),
                ..Default::default()
            },
        ))
        .await
        .unwrap();
    assert!(!suspend_state.load(Ordering::Acquire));

    let response = HeartbeatResponse {
        mailbox_message: Some(MailboxMessage {
            payload: Some(Payload::Json(
                serde_json::to_string(&Instruction::InvalidateCaches(vec![CacheIdent::TableId(
                    table_id,
                )]))
                .unwrap(),
            )),
            ..Default::default()
        }),
        extensions: HashMap::from([("response-extension".to_string(), b"value".to_vec())]),
        ..Default::default()
    };
    executor
        .handle(HeartbeatResponseHandlerContext::new(mailbox, response))
        .await
        .unwrap();

    assert_eq!(
        observations.lock().unwrap().as_slice(),
        &[(false, true, true, true), (true, false, true, true)]
    );
    assert!(!suspend_state.load(Ordering::Acquire));
    assert!(!cache.inner.lock().unwrap().contains_key(&table_key));
}

#[tokio::test]
async fn test_heartbeat_response_wire_compatibility_preserves_handlers() {
    let table_id = 42;
    let table_key = TableInfoKey::new(table_id).to_bytes();
    let cache = Arc::new(MockKvCacheInvalidator {
        inner: Mutex::new(HashMap::from([(table_key.clone(), 1)])),
    });
    let suspend_state = Arc::new(AtomicBool::new(false));
    let executor = heartbeat_response_handler_executor(
        &HeartbeatExtensions::default(),
        suspend_state.clone(),
        cache.clone(),
    );
    let (mailbox_tx, _) = mpsc::channel(1);
    let mailbox = Arc::new(HeartbeatMailbox::new(mailbox_tx));
    let heartbeat_config = ApiHeartbeatConfig {
        heartbeat_interval_ms: 3_000,
        retry_interval_ms: 500,
        gc_enabled: true,
    };

    let new_response = HeartbeatResponse {
        header: Some(ResponseHeader::success()),
        mailbox_message: Some(MailboxMessage {
            payload: Some(Payload::Json(
                serde_json::to_string(&Instruction::Suspend).unwrap(),
            )),
            ..Default::default()
        }),
        region_lease: Some(RegionLease::default()),
        heartbeat_config: Some(heartbeat_config),
        extensions: HashMap::from([("response-extension".to_string(), b"value".to_vec())]),
    };
    let legacy_decoded =
        LegacyHeartbeatResponse::decode(new_response.encode_to_vec().as_slice()).unwrap();
    assert_eq!(new_response.header, legacy_decoded.header);
    assert_eq!(new_response.mailbox_message, legacy_decoded.mailbox_message);
    assert_eq!(new_response.region_lease, legacy_decoded.region_lease);
    assert_eq!(
        new_response.heartbeat_config,
        legacy_decoded.heartbeat_config
    );

    executor
        .handle(HeartbeatResponseHandlerContext::new(
            mailbox.clone(),
            HeartbeatResponse {
                header: legacy_decoded.header,
                mailbox_message: legacy_decoded.mailbox_message,
                region_lease: legacy_decoded.region_lease,
                heartbeat_config: legacy_decoded.heartbeat_config,
                extensions: HashMap::new(),
            },
        ))
        .await
        .unwrap();
    assert!(suspend_state.load(Ordering::Acquire));

    let legacy_response = LegacyHeartbeatResponse {
        header: Some(ResponseHeader::success()),
        mailbox_message: Some(MailboxMessage {
            payload: Some(Payload::Json(
                serde_json::to_string(&Instruction::InvalidateCaches(vec![CacheIdent::TableId(
                    table_id,
                )]))
                .unwrap(),
            )),
            ..Default::default()
        }),
        region_lease: Some(RegionLease::default()),
        heartbeat_config: Some(heartbeat_config),
    };
    let current_decoded =
        HeartbeatResponse::decode(legacy_response.encode_to_vec().as_slice()).unwrap();
    assert_eq!(legacy_response.header, current_decoded.header);
    assert_eq!(
        legacy_response.mailbox_message,
        current_decoded.mailbox_message
    );
    assert_eq!(legacy_response.region_lease, current_decoded.region_lease);
    assert_eq!(
        legacy_response.heartbeat_config,
        current_decoded.heartbeat_config
    );
    assert!(current_decoded.extensions.is_empty());

    executor
        .handle(HeartbeatResponseHandlerContext::new(
            mailbox,
            current_decoded,
        ))
        .await
        .unwrap();
    assert!(!cache.inner.lock().unwrap().contains_key(&table_key));
}

async fn assert_extension_short_circuit_is_isolated(result: ShortCircuitResult) {
    let table_id = 42;
    let table_key = TableInfoKey::new(table_id).to_bytes();
    let cache = Arc::new(MockKvCacheInvalidator {
        inner: Mutex::new(HashMap::from([(table_key.clone(), 1)])),
    });
    let suspend_state = Arc::new(AtomicBool::new(true));
    let short_circuit_calls = Arc::new(AtomicUsize::new(0));
    let observations = Arc::new(Mutex::new(Vec::new()));
    let extensions = HeartbeatExtensions::default();
    extensions.register(TestExtension::with_handler(
        "short-circuit",
        Arc::new(ShortCircuitHandler {
            result,
            calls: short_circuit_calls.clone(),
        }),
    ));
    extensions.register(TestExtension::with_handler(
        "remaining",
        Arc::new(OrderingHandler {
            observations: observations.clone(),
            suspend_state: suspend_state.clone(),
            cache: cache.clone(),
            table_key: table_key.clone(),
        }),
    ));
    let executor =
        heartbeat_response_handler_executor(&extensions, suspend_state.clone(), cache.clone());
    let (mailbox_tx, _) = mpsc::channel(1);
    let mailbox = Arc::new(HeartbeatMailbox::new(mailbox_tx));

    executor
        .handle(HeartbeatResponseHandlerContext::new(
            mailbox.clone(),
            HeartbeatResponse::default(),
        ))
        .await
        .unwrap();

    let invalidate_response = HeartbeatResponse {
        mailbox_message: Some(MailboxMessage {
            payload: Some(Payload::Json(
                serde_json::to_string(&Instruction::InvalidateCaches(vec![CacheIdent::TableId(
                    table_id,
                )]))
                .unwrap(),
            )),
            ..Default::default()
        }),
        ..Default::default()
    };

    executor
        .handle(HeartbeatResponseHandlerContext::new(
            mailbox,
            invalidate_response,
        ))
        .await
        .unwrap();

    assert_eq!(short_circuit_calls.load(Ordering::Acquire), 2);
    assert_eq!(observations.lock().unwrap().len(), 2);
    assert!(!suspend_state.load(Ordering::Acquire));
    assert!(!cache.inner.lock().unwrap().contains_key(&table_key));
}

#[tokio::test]
async fn test_response_extension_done_does_not_skip_remaining_handlers() {
    assert_extension_short_circuit_is_isolated(ShortCircuitResult::Done).await;
}

#[tokio::test]
async fn test_response_extension_error_does_not_skip_remaining_handlers() {
    assert_extension_short_circuit_is_isolated(ShortCircuitResult::Error).await;
}

#[tokio::test]
async fn test_closed_response_stream_reconnects_once() {
    let extension = TestExtension::new("lifecycle");
    let extensions = HeartbeatExtensions::default();
    extensions.register(extension.clone());
    let (first_plan, first) = mock_connection(Duration::from_secs(60), Duration::from_millis(10));
    let (second_plan, second) = mock_connection(Duration::from_secs(60), Duration::from_millis(10));
    let connector = MockConnector::new([
        ConnectPlan::Ready(first_plan),
        ConnectPlan::Ready(second_plan),
    ]);
    let task = test_task(connector.clone(), extensions, FrontendOptions::default());

    task.start().await.unwrap();
    first.wait_for_requests(1).await;
    first.close_responses();
    connector.wait_for_calls(2).await;
    second.wait_for_requests(1).await;
    assert_eq!(task.generation(), 2);
    assert_eq!(
        extension.connected_generations.lock().unwrap().as_slice(),
        &[1, 2]
    );
    task.shutdown().await;
}

#[tokio::test]
async fn test_send_failure_reconnects() {
    let (first_plan, first) = mock_connection(Duration::from_secs(60), Duration::from_millis(10));
    first.fail_send.store(true, Ordering::Release);
    let (second_plan, second) = mock_connection(Duration::from_secs(60), Duration::from_millis(10));
    let connector = MockConnector::new([
        ConnectPlan::Ready(first_plan),
        ConnectPlan::Ready(second_plan),
    ]);
    let task = test_task(
        connector.clone(),
        HeartbeatExtensions::default(),
        FrontendOptions::default(),
    );

    task.start().await.unwrap();
    connector.wait_for_calls(2).await;
    second.wait_for_requests(1).await;
    assert_eq!(task.generation(), 2);
    task.shutdown().await;
}

#[tokio::test]
async fn test_concurrent_send_receive_failure_creates_one_generation() {
    let (first_plan, first) = mock_connection(Duration::from_secs(60), Duration::from_millis(10));
    first.fail_send.store(true, Ordering::Release);
    first.fail_responses();
    let (second_plan, second) = mock_connection(Duration::from_secs(60), Duration::from_millis(10));
    let connector = MockConnector::new([
        ConnectPlan::Ready(first_plan),
        ConnectPlan::Ready(second_plan),
    ]);
    let task = test_task(
        connector.clone(),
        HeartbeatExtensions::default(),
        FrontendOptions::default(),
    );

    task.start().await.unwrap();
    connector.wait_for_calls(2).await;
    second.wait_for_requests(1).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(connector.calls.load(Ordering::Acquire), 2);
    assert_eq!(task.generation(), 2);
    task.shutdown().await;
}

#[tokio::test]
async fn test_shutdown_cancels_stuck_handshake() {
    let extension = TestExtension::new("lifecycle");
    let extensions = HeartbeatExtensions::default();
    extensions.register(extension.clone());
    let connector = MockConnector::new([ConnectPlan::Pending]);
    let task = test_task(connector.clone(), extensions, FrontendOptions::default());
    let start_task = task.clone();
    let start = tokio::spawn(async move { start_task.start().await });
    connector.wait_for_calls(1).await;

    tokio::time::timeout(Duration::from_secs(1), task.shutdown())
        .await
        .unwrap();
    start.await.unwrap().unwrap();
    assert!(extension.connected_generations.lock().unwrap().is_empty());
    assert!(!task.has_supervisor().await);
}

#[tokio::test]
async fn test_shutdown_cancels_retry_sleep_and_joins_supervisor() {
    let (plan, handle) = mock_connection(Duration::from_secs(60), Duration::from_secs(60));
    let connector = MockConnector::new([ConnectPlan::Ready(plan)]);
    let task = test_task(
        connector.clone(),
        HeartbeatExtensions::default(),
        FrontendOptions::default(),
    );
    task.start().await.unwrap();
    handle.wait_for_requests(1).await;
    handle.close_responses();
    tokio::time::sleep(Duration::from_millis(20)).await;

    tokio::time::timeout(Duration::from_secs(1), task.shutdown())
        .await
        .unwrap();
    assert_eq!(connector.calls.load(Ordering::Acquire), 1);
    assert!(!task.has_supervisor().await);
}

#[tokio::test]
async fn test_shutdown_cancels_inflight_response_handler() {
    let started = Arc::new(Notify::new());
    let extension = TestExtension::with_handler(
        "pending-response",
        Arc::new(PendingHandler {
            started: started.clone(),
        }),
    );
    let extensions = HeartbeatExtensions::default();
    extensions.register(extension.clone());
    let (plan, handle) = mock_connection(Duration::from_secs(60), Duration::from_secs(60));
    let task = test_task(
        MockConnector::new([ConnectPlan::Ready(plan)]),
        extensions,
        FrontendOptions::default(),
    );
    task.start().await.unwrap();
    handle.wait_for_requests(1).await;

    let handler_started = started.notified();
    handle.send_response(HeartbeatResponse::default());
    tokio::time::timeout(Duration::from_secs(1), handler_started)
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), task.shutdown())
        .await
        .unwrap();
    assert!(!task.has_supervisor().await);
    assert_eq!(extension.shutdown_calls.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn test_concurrent_start_has_one_active_generation() {
    let (plan, handle) = mock_connection(Duration::from_secs(60), Duration::from_millis(10));
    let connector = MockConnector::new([ConnectPlan::Ready(plan)]);
    let task = test_task(
        connector.clone(),
        HeartbeatExtensions::default(),
        FrontendOptions::default(),
    );
    let starts = (0..8).map(|_| {
        let task = task.clone();
        tokio::spawn(async move { task.start().await })
    });
    for start in starts {
        start.await.unwrap().unwrap();
    }
    handle.wait_for_requests(1).await;

    assert_eq!(connector.calls.load(Ordering::Acquire), 1);
    assert_eq!(task.generation(), 1);
    task.shutdown().await;
}

#[tokio::test]
async fn test_shutdown_prevents_restart_and_extension_callbacks() {
    let extension = TestExtension::dynamic("lifecycle", "dynamic");
    let extensions = HeartbeatExtensions::default();
    extensions.register(extension.clone());
    let (plan, handle) = mock_connection(Duration::from_secs(60), Duration::from_millis(10));
    let connector = MockConnector::new([ConnectPlan::Ready(plan)]);
    let task = test_task(connector.clone(), extensions, FrontendOptions::default());
    task.start().await.unwrap();
    handle.wait_for_requests(1).await;
    task.shutdown().await;
    task.shutdown().await;

    let request_calls = extension.request_calls.load(Ordering::Acquire);
    let connected = extension.connected_generations.lock().unwrap().clone();
    task.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(connector.calls.load(Ordering::Acquire), 1);
    assert_eq!(
        extension.request_calls.load(Ordering::Acquire),
        request_calls
    );
    assert_eq!(*extension.connected_generations.lock().unwrap(), connected);
    assert_eq!(extension.shutdown_calls.load(Ordering::Acquire), 1);
}
