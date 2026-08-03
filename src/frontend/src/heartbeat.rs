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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use api::v1::meta::heartbeat_request::NodeWorkloads;
use api::v1::meta::{FrontendWorkloads, HeartbeatRequest, HeartbeatResponse, NodeInfo, Peer};
use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_meta::cache_invalidator::CacheInvalidatorRef;
use common_meta::datanode::EnvVars;
use common_meta::heartbeat::handler::invalidate_table_cache::InvalidateCacheHandler;
use common_meta::heartbeat::handler::parse_mailbox_message::ParseMailboxMessageHandler;
use common_meta::heartbeat::handler::suspend::SuspendHandler;
use common_meta::heartbeat::handler::{
    HandlerGroupExecutor, HeartbeatResponseHandlerContext, HeartbeatResponseHandlerExecutorRef,
    HeartbeatResponseHandlerRef,
};
use common_meta::heartbeat::mailbox::{HeartbeatMailbox, MailboxRef, OutgoingMessage};
use common_meta::heartbeat::utils::outgoing_message_to_mailbox_message;
use common_stat::ResourceStatRef;
use common_telemetry::{debug, error, info, warn};
use meta_client::client::heartbeat::HeartbeatConfig;
use meta_client::client::{HeartbeatSender, HeartbeatStream, MetaClient};
use servers::addrs;
use snafu::ResultExt;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

use crate::error;
use crate::error::Result;
use crate::frontend::FrontendOptions;
use crate::metrics::{HEARTBEAT_RECV_COUNT, HEARTBEAT_SENT_COUNT};

/// The result type returned by a [`HeartbeatExtension`].
pub type HeartbeatExtensionResult<T> = std::result::Result<T, BoxedError>;

/// An extension to frontend heartbeat requests, responses, and lifecycle events.
///
/// [`HeartbeatExtension::request_extensions`] is called for every heartbeat. A failed call is
/// isolated from the base heartbeat and from other extensions. [`HeartbeatExtension::connected`]
/// is called once for every successfully established connection generation, including reconnects;
/// implementations must therefore be idempotent. [`HeartbeatExtension::shutdown`] is called once
/// during task shutdown after heartbeat I/O has stopped.
#[async_trait]
pub trait HeartbeatExtension: Send + Sync {
    /// Returns the stable name used to make registration idempotent.
    fn name(&self) -> &str;

    /// Generates request extensions for one heartbeat.
    async fn request_extensions(&self) -> HeartbeatExtensionResult<HashMap<String, Vec<u8>>> {
        Ok(HashMap::new())
    }

    /// Returns a handler to insert into the heartbeat response handler chain.
    ///
    /// Errors and [`common_meta::heartbeat::handler::HandleControl::Done`] are isolated to this
    /// extension so they cannot skip later extensions or mandatory OSS handlers.
    fn response_handler(&self) -> Option<HeartbeatResponseHandlerRef> {
        None
    }

    /// Notifies the extension that a heartbeat connection generation is ready.
    async fn connected(&self, _generation: u64) -> HeartbeatExtensionResult<()> {
        Ok(())
    }

    /// Stops and joins background work owned by the extension.
    async fn shutdown(&self) -> HeartbeatExtensionResult<()> {
        Ok(())
    }
}

/// A shareable, ordered registry of frontend heartbeat extensions.
#[derive(Clone, Default)]
pub struct HeartbeatExtensions {
    inner: Arc<StdMutex<HeartbeatExtensionsInner>>,
}

#[derive(Default)]
struct HeartbeatExtensionsInner {
    names: HashSet<String>,
    extensions: Vec<Arc<dyn HeartbeatExtension>>,
}

impl HeartbeatExtensions {
    /// Registers an extension without replacing an existing extension of the same name.
    ///
    /// Returns `true` for a new registration and `false` for an idempotent duplicate.
    pub fn register(&self, extension: Arc<dyn HeartbeatExtension>) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let name = extension.name().to_string();
        if !inner.names.insert(name) {
            return false;
        }
        inner.extensions.push(extension);
        true
    }

    /// Returns the registered extensions in registration order.
    pub fn extensions(&self) -> Vec<Arc<dyn HeartbeatExtension>> {
        self.inner.lock().unwrap().extensions.clone()
    }

    /// Returns response handlers in extension registration order.
    pub fn response_handlers(&self) -> Vec<HeartbeatResponseHandlerRef> {
        self.extensions()
            .into_iter()
            .filter_map(|extension| extension.response_handler())
            .collect()
    }

    /// Returns the number of registered extensions.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().extensions.len()
    }

    /// Returns whether no extension is registered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Builds the frontend heartbeat response handler chain.
///
/// Mailbox parsing always runs first, followed by extension handlers, suspension state handling,
/// and cache invalidation.
pub fn heartbeat_response_handler_executor(
    extensions: &HeartbeatExtensions,
    suspend_state: Arc<AtomicBool>,
    cache_invalidator: CacheInvalidatorRef,
) -> HeartbeatResponseHandlerExecutorRef {
    let mut handlers: Vec<HeartbeatResponseHandlerRef> = vec![Arc::new(ParseMailboxMessageHandler)];
    handlers.extend(extensions.response_handlers().into_iter().map(|handler| {
        Arc::new(IsolatedHeartbeatResponseHandler(handler)) as HeartbeatResponseHandlerRef
    }));
    handlers.extend([
        Arc::new(SuspendHandler::new(suspend_state)) as HeartbeatResponseHandlerRef,
        Arc::new(InvalidateCacheHandler::new(cache_invalidator)),
    ]);
    Arc::new(HandlerGroupExecutor::new(handlers))
}

struct IsolatedHeartbeatResponseHandler(HeartbeatResponseHandlerRef);

#[async_trait]
impl common_meta::heartbeat::handler::HeartbeatResponseHandler
    for IsolatedHeartbeatResponseHandler
{
    fn is_acceptable(&self, _ctx: &HeartbeatResponseHandlerContext) -> bool {
        true
    }

    async fn handle(
        &self,
        ctx: &mut HeartbeatResponseHandlerContext,
    ) -> common_meta::error::Result<common_meta::heartbeat::handler::HandleControl> {
        use common_meta::heartbeat::handler::HandleControl;

        if self.0.is_acceptable(ctx)
            && let Err(error) = self.0.handle(ctx).await
        {
            error!(error; "Heartbeat extension response handler failed");
        }
        Ok(HandleControl::Continue)
    }
}

#[async_trait]
trait HeartbeatConnector: Send + Sync {
    async fn connect(&self) -> Result<HeartbeatConnection>;
}

#[async_trait]
trait HeartbeatRequestSender: Send + Sync {
    async fn send(&self, request: HeartbeatRequest) -> HeartbeatExtensionResult<()>;
}

#[async_trait]
trait HeartbeatResponseStream: Send {
    async fn message(&mut self) -> HeartbeatExtensionResult<Option<HeartbeatResponse>>;
}

struct HeartbeatConnection {
    sender: Arc<dyn HeartbeatRequestSender>,
    stream: Box<dyn HeartbeatResponseStream>,
    config: HeartbeatConfig,
}

struct MetaHeartbeatConnector {
    client: Arc<MetaClient>,
}

#[async_trait]
impl HeartbeatConnector for MetaHeartbeatConnector {
    async fn connect(&self) -> Result<HeartbeatConnection> {
        let (sender, stream, config) = self
            .client
            .heartbeat()
            .await
            .context(error::CreateMetaHeartbeatStreamSnafu)?;
        Ok(HeartbeatConnection {
            sender: Arc::new(MetaHeartbeatSender(sender)),
            stream: Box::new(MetaHeartbeatStream(stream)),
            config,
        })
    }
}

struct MetaHeartbeatSender(HeartbeatSender);

#[async_trait]
impl HeartbeatRequestSender for MetaHeartbeatSender {
    async fn send(&self, request: HeartbeatRequest) -> HeartbeatExtensionResult<()> {
        self.0.send(request).await.map_err(BoxedError::new)
    }
}

struct MetaHeartbeatStream(HeartbeatStream);

#[async_trait]
impl HeartbeatResponseStream for MetaHeartbeatStream {
    async fn message(&mut self) -> HeartbeatExtensionResult<Option<HeartbeatResponse>> {
        self.0.message().await.map_err(BoxedError::new)
    }
}

#[derive(Clone)]
struct HeartbeatRunner {
    peer_addr: String,
    connector: Arc<dyn HeartbeatConnector>,
    resp_handler_executor: HeartbeatResponseHandlerExecutorRef,
    start_time_ms: u64,
    resource_stat: ResourceStatRef,
    env_vars: EnvVars,
    extensions: HeartbeatExtensions,
    cancellation: CancellationToken,
    generation: Arc<AtomicU64>,
}

impl HeartbeatRunner {
    async fn connect(&self) -> Result<Option<HeartbeatConnection>> {
        tokio::select! {
            _ = self.cancellation.cancelled() => Ok(None),
            connection = self.connector.connect() => connection.map(Some),
        }
    }

    fn next_generation(&self) -> u64 {
        self.generation
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1)
    }

    async fn notify_connected(&self, generation: u64) -> bool {
        for extension in self.extensions.extensions() {
            let result = tokio::select! {
                _ = self.cancellation.cancelled() => return false,
                result = extension.connected(generation) => result,
            };
            if let Err(error) = result {
                error!(error; "Heartbeat extension '{}' failed its connected callback", extension.name());
            }
        }
        true
    }

    async fn shutdown_extensions(&self) {
        for extension in self.extensions.extensions() {
            if let Err(error) = extension.shutdown().await {
                error!(error; "Failed to shut down heartbeat extension '{}'", extension.name());
            }
        }
    }

    async fn run(self, mut connection: HeartbeatConnection) {
        loop {
            let retry_interval = connection.config.retry_interval;
            if self.run_connection(connection).await == ConnectionEnd::Shutdown {
                return;
            }

            loop {
                if !self.wait_retry(retry_interval).await {
                    return;
                }
                info!("Try to re-establish the heartbeat connection to metasrv.");

                match self.connect().await {
                    Ok(Some(next)) => {
                        let generation = self.next_generation();
                        if !self.notify_connected(generation).await {
                            return;
                        }
                        connection = next;
                        break;
                    }
                    Ok(None) => return,
                    Err(error) => {
                        error!(error; "Failed to re-establish heartbeat connection to metasrv");
                    }
                }
            }
        }
    }

    async fn wait_retry(&self, retry_interval: Duration) -> bool {
        tokio::select! {
            _ = self.cancellation.cancelled() => false,
            _ = tokio::time::sleep(retry_interval) => true,
        }
    }

    async fn run_connection(&self, connection: HeartbeatConnection) -> ConnectionEnd {
        let (outgoing_tx, outgoing_rx) = mpsc::channel(16);
        let mailbox = Arc::new(HeartbeatMailbox::new(outgoing_tx));
        let report =
            self.report_heartbeats(connection.sender, outgoing_rx, connection.config.interval);
        let responses = self.handle_responses(connection.stream, mailbox);
        tokio::pin!(report);
        tokio::pin!(responses);

        tokio::select! {
            _ = self.cancellation.cancelled() => ConnectionEnd::Shutdown,
            end = &mut report => end,
            end = &mut responses => end,
        }
    }

    async fn handle_responses(
        &self,
        mut stream: Box<dyn HeartbeatResponseStream>,
        mailbox: MailboxRef,
    ) -> ConnectionEnd {
        loop {
            let response = tokio::select! {
                _ = self.cancellation.cancelled() => return ConnectionEnd::Shutdown,
                response = stream.message() => response,
            };
            match response {
                Ok(Some(response)) => {
                    debug!("Receiving heartbeat response: {:?}", response);
                    if let Some(message) = &response.mailbox_message {
                        info!("Received mailbox message: {message:?}");
                    }
                    let context = HeartbeatResponseHandlerContext::new(mailbox.clone(), response);
                    if let Err(error) = self.handle_response(context).await {
                        error!(error; "Error while handling heartbeat response");
                        HEARTBEAT_RECV_COUNT
                            .with_label_values(&["processing_error"])
                            .inc();
                    } else {
                        HEARTBEAT_RECV_COUNT.with_label_values(&["success"]).inc();
                    }
                }
                Ok(None) => {
                    warn!("Heartbeat response stream closed");
                    return ConnectionEnd::Reconnect;
                }
                Err(error) => {
                    HEARTBEAT_RECV_COUNT.with_label_values(&["error"]).inc();
                    error!(error; "Occur error while reading heartbeat response");
                    return ConnectionEnd::Reconnect;
                }
            }
        }
    }

    async fn report_heartbeats(
        &self,
        sender: Arc<dyn HeartbeatRequestSender>,
        mut outgoing_rx: Receiver<OutgoingMessage>,
        report_interval: Duration,
    ) -> ConnectionEnd {
        let total_cpu_millicores = self.resource_stat.get_total_cpu_millicores();
        let total_memory_bytes = self.resource_stat.get_total_memory_bytes();
        let mut extensions = HashMap::new();
        self.env_vars.clone().into_extensions(&mut extensions);
        let heartbeat_request = HeartbeatRequest {
            peer: Some(Peer {
                // Metasrv calculates the frontend id by hashing this reachable address.
                id: 0,
                addr: self.peer_addr.clone(),
            }),
            info: Self::build_node_info(
                self.start_time_ms,
                total_cpu_millicores,
                total_memory_bytes,
            ),
            node_workloads: Some(NodeWorkloads::Frontend(FrontendWorkloads { types: vec![] })),
            extensions,
            ..Default::default()
        };
        let sleep = tokio::time::sleep(Duration::ZERO);
        tokio::pin!(sleep);

        loop {
            let request = tokio::select! {
                _ = self.cancellation.cancelled() => return ConnectionEnd::Shutdown,
                message = outgoing_rx.recv() => {
                    if let Some(message) = message {
                        Self::new_heartbeat_request(&heartbeat_request, Some(message), 0, 0)
                    } else {
                        warn!("Sender has been dropped, exiting the heartbeat loop");
                        return ConnectionEnd::Reconnect;
                    }
                }
                _ = &mut sleep => {
                    sleep.as_mut().reset(Instant::now() + report_interval);
                    Self::new_heartbeat_request(
                        &heartbeat_request,
                        None,
                        self.resource_stat.get_cpu_usage_millicores(),
                        self.resource_stat.get_memory_usage_bytes(),
                    )
                }
            };

            if let Some(mut request) = request {
                if !self.add_request_extensions(&mut request).await {
                    return ConnectionEnd::Shutdown;
                }
                let result = tokio::select! {
                    _ = self.cancellation.cancelled() => return ConnectionEnd::Shutdown,
                    result = sender.send(request.clone()) => result,
                };
                if let Err(error) = result {
                    error!(error; "Failed to send heartbeat to metasrv");
                    return ConnectionEnd::Reconnect;
                }
                HEARTBEAT_SENT_COUNT.inc();
                debug!(
                    "Send a heartbeat request to metasrv, content: {:?}",
                    request
                );
            }
        }
    }

    async fn add_request_extensions(&self, request: &mut HeartbeatRequest) -> bool {
        for extension in self.extensions.extensions() {
            let generated = tokio::select! {
                _ = self.cancellation.cancelled() => return false,
                generated = extension.request_extensions() => generated,
            };
            let generated = match generated {
                Ok(generated) => generated,
                Err(error) => {
                    error!(error; "Heartbeat extension '{}' failed to generate request extensions", extension.name());
                    continue;
                }
            };

            if let Some(key) = generated
                .keys()
                .find(|key| request.extensions.contains_key(*key))
            {
                warn!(
                    "Heartbeat extension '{}' produced conflicting key '{}'; discarding its output",
                    extension.name(),
                    key
                );
                continue;
            }
            request.extensions.extend(generated);
        }
        true
    }

    fn new_heartbeat_request(
        heartbeat_request: &HeartbeatRequest,
        message: Option<OutgoingMessage>,
        cpu_usage: i64,
        memory_usage: i64,
    ) -> Option<HeartbeatRequest> {
        let mailbox_message = match message.map(outgoing_message_to_mailbox_message) {
            Some(Ok(message)) => Some(message),
            Some(Err(error)) => {
                error!(error; "Failed to encode mailbox messages");
                return None;
            }
            None => None,
        };

        let mut heartbeat_request = HeartbeatRequest {
            mailbox_message,
            ..heartbeat_request.clone()
        };
        if let Some(info) = heartbeat_request.info.as_mut() {
            info.memory_usage_bytes = memory_usage;
            info.cpu_usage_millicores = cpu_usage;
        }
        Some(heartbeat_request)
    }

    #[allow(deprecated)]
    fn build_node_info(
        start_time_ms: u64,
        total_cpu_millicores: i64,
        total_memory_bytes: i64,
    ) -> Option<NodeInfo> {
        let build_info = common_version::build_info();
        Some(NodeInfo {
            version: build_info.version.to_string(),
            git_commit: build_info.commit_short.to_string(),
            start_time_ms,
            total_cpu_millicores,
            total_memory_bytes,
            cpu_usage_millicores: 0,
            memory_usage_bytes: 0,
            // TODO(zyy17): Remove these deprecated fields when the deprecated fields are removed from the proto.
            cpus: total_cpu_millicores as u32,
            memory_bytes: total_memory_bytes as u64,
            hostname: hostname::get()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
        })
    }

    async fn handle_response(&self, context: HeartbeatResponseHandlerContext) -> Result<()> {
        self.resp_handler_executor
            .handle(context)
            .await
            .context(error::HandleHeartbeatResponseSnafu)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ConnectionEnd {
    Reconnect,
    Shutdown,
}

/// The frontend task that sends [`HeartbeatRequest`] values to metasrv in the background.
#[derive(Clone)]
pub struct HeartbeatTask {
    runner: HeartbeatRunner,
    start_lock: Arc<Mutex<()>>,
    shutdown_lock: Arc<Mutex<()>>,
    supervisor: Arc<Mutex<Option<common_runtime::JoinHandle<()>>>>,
}

impl HeartbeatTask {
    pub fn new(
        peer_addr: String,
        opts: &FrontendOptions,
        meta_client: Arc<MetaClient>,
        resp_handler_executor: HeartbeatResponseHandlerExecutorRef,
        resource_stat: ResourceStatRef,
    ) -> Self {
        Self::new_with_connector(
            peer_addr,
            opts,
            Arc::new(MetaHeartbeatConnector {
                client: meta_client,
            }),
            resp_handler_executor,
            resource_stat,
        )
    }

    fn new_with_connector(
        peer_addr: String,
        opts: &FrontendOptions,
        connector: Arc<dyn HeartbeatConnector>,
        resp_handler_executor: HeartbeatResponseHandlerExecutorRef,
        resource_stat: ResourceStatRef,
    ) -> Self {
        Self {
            runner: HeartbeatRunner {
                peer_addr,
                connector,
                resp_handler_executor,
                start_time_ms: common_time::util::current_time_millis() as u64,
                resource_stat,
                env_vars: EnvVars::from_config(&opts.heartbeat_env_vars),
                extensions: HeartbeatExtensions::default(),
                cancellation: CancellationToken::new(),
                generation: Arc::new(AtomicU64::new(0)),
            },
            start_lock: Arc::new(Mutex::new(())),
            shutdown_lock: Arc::new(Mutex::new(())),
            supervisor: Arc::new(Mutex::new(None)),
        }
    }

    /// Installs the extensions registered before heartbeat startup.
    pub fn with_extensions(mut self, extensions: HeartbeatExtensions) -> Self {
        self.runner.extensions = extensions;
        self
    }

    /// Establishes the initial heartbeat connection and starts its background supervisor.
    pub async fn start(&self) -> Result<()> {
        let _start_guard = self.start_lock.lock().await;
        if self.runner.cancellation.is_cancelled() {
            return Ok(());
        }

        let finished = {
            let mut supervisor = self.supervisor.lock().await;
            match supervisor.as_ref() {
                Some(handle) if !handle.is_finished() => return Ok(()),
                Some(_) => supervisor.take(),
                None => None,
            }
        };
        if let Some(handle) = finished
            && let Err(error) = handle.await
            && !error.is_cancelled()
        {
            error!(error; "Heartbeat supervisor join failed");
        }

        let Some(connection) = self.runner.connect().await? else {
            return Ok(());
        };
        info!(
            "Heartbeat started with Metasrv config: {}",
            connection.config
        );

        let generation = self.runner.next_generation();
        if !self.runner.notify_connected(generation).await {
            return Ok(());
        }

        let runner = self.runner.clone();
        let handle = common_runtime::spawn_hb(async move {
            runner.run(connection).await;
        });
        *self.supervisor.lock().await = Some(handle);
        Ok(())
    }

    /// Cancels and joins heartbeat I/O and all registered extension lifecycles.
    pub async fn shutdown(&self) {
        let _shutdown_guard = self.shutdown_lock.lock().await;
        if self.runner.cancellation.is_cancelled() {
            return;
        }
        self.runner.cancellation.cancel();

        // Wait for a concurrently running handshake or connected callback to observe cancellation.
        let _start_guard = self.start_lock.lock().await;
        let handle = self.supervisor.lock().await.take();
        if let Some(handle) = handle
            && let Err(error) = handle.await
            && !error.is_cancelled()
        {
            error!(error; "Heartbeat supervisor join failed");
        }
        self.runner.shutdown_extensions().await;
    }

    #[cfg(test)]
    fn generation(&self) -> u64 {
        self.runner.generation.load(Ordering::Acquire)
    }

    #[cfg(test)]
    async fn has_supervisor(&self) -> bool {
        self.supervisor.lock().await.is_some()
    }

    #[cfg(test)]
    pub(crate) fn is_shutdown(&self) -> bool {
        self.runner.cancellation.is_cancelled()
    }
}

pub(crate) fn frontend_peer_addr(opts: &FrontendOptions) -> String {
    // if internal grpc is configured, use its address as the peer address
    // otherwise use the public grpc address, because peer address only promises to be reachable
    // by other components, it doesn't matter whether it's internal or external
    if let Some(internal) = &opts.internal_grpc {
        addrs::resolve_addr(&internal.bind_addr, Some(&internal.server_addr))
    } else {
        addrs::resolve_addr(&opts.grpc.bind_addr, Some(&opts.grpc.server_addr))
    }
}
