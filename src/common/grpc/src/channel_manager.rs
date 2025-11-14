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

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::channel;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use common_base::readable_size::ReadableSize;
use common_telemetry::{error, info};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use lazy_static::lazy_static;
use notify::{EventKind, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use tonic::transport::{
    Certificate, Channel as InnerChannel, ClientTlsConfig, Endpoint, Identity, Uri,
};
use tower::Service;

use crate::error::{CreateChannelSnafu, FileWatchSnafu, InvalidConfigFilePathSnafu, Result};

const RECYCLE_CHANNEL_INTERVAL_SECS: u64 = 60;
pub const DEFAULT_GRPC_REQUEST_TIMEOUT_SECS: u64 = 10;
pub const DEFAULT_GRPC_CONNECT_TIMEOUT_SECS: u64 = 1;
pub const DEFAULT_MAX_GRPC_RECV_MESSAGE_SIZE: ReadableSize = ReadableSize::mb(512);
pub const DEFAULT_MAX_GRPC_SEND_MESSAGE_SIZE: ReadableSize = ReadableSize::mb(512);

lazy_static! {
    static ref ID: AtomicU64 = AtomicU64::new(0);
}

#[derive(Clone, Debug, Default)]
pub struct ChannelManager {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    id: u64,
    config: ChannelConfig,
    reloadable_client_tls_config: Option<Arc<ReloadableClientTlsConfig>>,
    pool: Arc<Pool>,
    channel_recycle_started: AtomicBool,
    cancel: CancellationToken,
}

impl Default for Inner {
    fn default() -> Self {
        Self::with_config(ChannelConfig::default())
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Cancel the channel recycle task.
        self.cancel.cancel();
    }
}

impl Inner {
    fn with_config(config: ChannelConfig) -> Self {
        let id = ID.fetch_add(1, Ordering::Relaxed);
        let pool = Arc::new(Pool::default());
        let cancel = CancellationToken::new();

        Self {
            id,
            config,
            reloadable_client_tls_config: None,
            pool,
            channel_recycle_started: AtomicBool::new(false),
            cancel,
        }
    }
}

impl ChannelManager {
    pub fn new() -> Self {
        Default::default()
    }

    /// Create a ChannelManager with configuration and optional TLS config
    ///
    /// Use [`load_reloadable_client_tls_config`] to create TLS configuration from `ClientTlsOption`.
    /// The TLS config supports both static (watch disabled) and dynamic reloading (watch enabled).
    pub fn with_config(
        config: ChannelConfig,
        reloadable_tls_config: Option<Arc<ReloadableClientTlsConfig>>,
    ) -> Self {
        let mut inner = Inner::with_config(config.clone());
        inner.reloadable_client_tls_config = reloadable_tls_config;
        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn config(&self) -> &ChannelConfig {
        &self.inner.config
    }

    fn pool(&self) -> &Arc<Pool> {
        &self.inner.pool
    }

    pub fn get(&self, addr: impl AsRef<str>) -> Result<InnerChannel> {
        self.trigger_channel_recycling();

        let addr = addr.as_ref();
        // It will acquire the read lock.
        if let Some(inner_ch) = self.pool().get(addr) {
            return Ok(inner_ch);
        }

        // It will acquire the write lock.
        let entry = match self.pool().entry(addr.to_string()) {
            Entry::Occupied(entry) => {
                entry.get().increase_access();
                entry.into_ref()
            }
            Entry::Vacant(entry) => {
                let endpoint = self.build_endpoint(addr)?;
                let inner_channel = endpoint.connect_lazy();

                let channel = Channel {
                    channel: inner_channel,
                    access: AtomicUsize::new(1),
                    use_default_connector: true,
                };
                entry.insert(channel)
            }
        };
        Ok(entry.channel.clone())
    }

    pub fn reset_with_connector<C>(
        &self,
        addr: impl AsRef<str>,
        connector: C,
    ) -> Result<InnerChannel>
    where
        C: Service<Uri> + Send + 'static,
        C::Response: hyper::rt::Read + hyper::rt::Write + Send + Unpin,
        C::Future: Send + 'static,
        Box<dyn std::error::Error + Send + Sync>: From<C::Error> + Send + 'static,
    {
        let addr = addr.as_ref();
        let endpoint = self.build_endpoint(addr)?;
        let inner_channel = endpoint.connect_with_connector_lazy(connector);
        let channel = Channel {
            channel: inner_channel.clone(),
            access: AtomicUsize::new(1),
            use_default_connector: false,
        };
        self.pool().put(addr, channel);

        Ok(inner_channel)
    }

    pub fn retain_channel<F>(&self, f: F)
    where
        F: FnMut(&String, &mut Channel) -> bool,
    {
        self.pool().retain_channel(f);
    }

    /// Clear all channels to force reconnection.
    /// This should be called when TLS configuration changes to ensure new connections use updated certificates.
    pub fn clear_all_channels(&self) {
        self.pool().retain_channel(|_, _| false);
    }

    fn build_endpoint(&self, addr: &str) -> Result<Endpoint> {
        // Get the latest TLS config from reloadable config (which handles both static and dynamic cases)
        let tls_config = self
            .inner
            .reloadable_client_tls_config
            .as_ref()
            .and_then(|c| c.get_client_config());

        let http_prefix = if tls_config.is_some() {
            "https"
        } else {
            "http"
        };

        let mut endpoint = Endpoint::new(format!("{http_prefix}://{addr}"))
            .context(CreateChannelSnafu { addr })?;

        if let Some(dur) = self.config().timeout {
            endpoint = endpoint.timeout(dur);
        }
        if let Some(dur) = self.config().connect_timeout {
            endpoint = endpoint.connect_timeout(dur);
        }
        if let Some(limit) = self.config().concurrency_limit {
            endpoint = endpoint.concurrency_limit(limit);
        }
        if let Some((limit, dur)) = self.config().rate_limit {
            endpoint = endpoint.rate_limit(limit, dur);
        }
        if let Some(size) = self.config().initial_stream_window_size {
            endpoint = endpoint.initial_stream_window_size(size);
        }
        if let Some(size) = self.config().initial_connection_window_size {
            endpoint = endpoint.initial_connection_window_size(size);
        }
        if let Some(dur) = self.config().http2_keep_alive_interval {
            endpoint = endpoint.http2_keep_alive_interval(dur);
        }
        if let Some(dur) = self.config().http2_keep_alive_timeout {
            endpoint = endpoint.keep_alive_timeout(dur);
        }
        if let Some(enabled) = self.config().http2_keep_alive_while_idle {
            endpoint = endpoint.keep_alive_while_idle(enabled);
        }
        if let Some(enabled) = self.config().http2_adaptive_window {
            endpoint = endpoint.http2_adaptive_window(enabled);
        }
        if let Some(tls_config) = tls_config {
            endpoint = endpoint
                .tls_config(tls_config)
                .context(CreateChannelSnafu { addr })?;
        }

        endpoint = endpoint
            .tcp_keepalive(self.config().tcp_keepalive)
            .tcp_nodelay(self.config().tcp_nodelay);

        Ok(endpoint)
    }

    fn trigger_channel_recycling(&self) {
        if self
            .inner
            .channel_recycle_started
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let pool = self.pool().clone();
        let cancel = self.inner.cancel.clone();
        let id = self.inner.id;
        let _handle = common_runtime::spawn_global(async move {
            recycle_channel_in_loop(pool, id, cancel, RECYCLE_CHANNEL_INTERVAL_SECS).await;
        });
        info!(
            "ChannelManager: {}, channel recycle is started, running in the background!",
            self.inner.id
        );
    }
}

fn load_tls_config(tls_option: Option<&ClientTlsOption>) -> Result<Option<ClientTlsConfig>> {
    let path_config = match tls_option {
        Some(path_config) if path_config.enabled => path_config,
        _ => return Ok(None),
    };

    let mut tls_config = ClientTlsConfig::new();

    if let Some(server_ca) = &path_config.server_ca_cert_path {
        let server_root_ca_cert =
            std::fs::read_to_string(server_ca).context(InvalidConfigFilePathSnafu)?;
        let server_root_ca_cert = Certificate::from_pem(server_root_ca_cert);
        tls_config = tls_config.ca_certificate(server_root_ca_cert);
    }

    if let (Some(client_cert_path), Some(client_key_path)) =
        (&path_config.client_cert_path, &path_config.client_key_path)
    {
        let client_cert =
            std::fs::read_to_string(client_cert_path).context(InvalidConfigFilePathSnafu)?;
        let client_key =
            std::fs::read_to_string(client_key_path).context(InvalidConfigFilePathSnafu)?;
        let client_identity = Identity::from_pem(client_cert, client_key);
        tls_config = tls_config.identity(client_identity);
    }
    Ok(Some(tls_config))
}

/// Load client TLS configuration from `ClientTlsOption` and return a `ReloadableClientTlsConfig`.
/// This is the primary way to create TLS configuration for the ChannelManager.
pub fn load_client_tls_config(
    tls_option: Option<ClientTlsOption>,
) -> Result<Option<Arc<ReloadableClientTlsConfig>>> {
    match tls_option {
        Some(option) if option.enabled => {
            let reloadable = ReloadableClientTlsConfig::try_new(option)?;
            Ok(Some(Arc::new(reloadable)))
        }
        _ => Ok(None),
    }
}

/// A mutable container for TLS client config
///
/// This struct allows dynamic reloading of client certificates and keys
#[derive(Debug)]
pub struct ReloadableClientTlsConfig {
    tls_option: ClientTlsOption,
    config: RwLock<Option<ClientTlsConfig>>,
    version: AtomicUsize,
}

impl ReloadableClientTlsConfig {
    /// Create client config by loading configuration from `ClientTlsOption`
    pub fn try_new(tls_option: ClientTlsOption) -> Result<ReloadableClientTlsConfig> {
        let client_config = load_tls_config(Some(&tls_option))?;
        Ok(Self {
            tls_option,
            config: RwLock::new(client_config),
            version: AtomicUsize::new(0),
        })
    }

    /// Reread client certificates and keys from file system.
    pub fn reload(&self) -> Result<()> {
        let client_config = load_tls_config(Some(&self.tls_option))?;
        *self.config.write().unwrap() = client_config;
        self.version.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Get the client config held by this container
    pub fn get_client_config(&self) -> Option<ClientTlsConfig> {
        self.config.read().unwrap().clone()
    }

    /// Get associated `ClientTlsOption`
    pub fn get_tls_option(&self) -> &ClientTlsOption {
        &self.tls_option
    }

    /// Get version of current config
    ///
    /// this version will auto increase when client config get reloaded.
    pub fn get_version(&self) -> usize {
        self.version.load(Ordering::Relaxed)
    }

    fn cert_path(&self) -> Option<&Path> {
        self.tls_option
            .client_cert_path
            .as_ref()
            .map(|p| Path::new(p.as_str()))
    }

    fn key_path(&self) -> Option<&Path> {
        self.tls_option
            .client_key_path
            .as_ref()
            .map(|p| Path::new(p.as_str()))
    }

    fn server_ca_cert_path(&self) -> Option<&Path> {
        self.tls_option
            .server_ca_cert_path
            .as_ref()
            .map(|p| Path::new(p.as_str()))
    }

    fn watch_enabled(&self) -> bool {
        self.tls_option.enabled && self.tls_option.watch
    }
}

pub fn maybe_watch_client_tls_config(
    client_tls_config: Arc<ReloadableClientTlsConfig>,
    channel_manager: &ChannelManager,
) -> Result<()> {
    if !client_tls_config.watch_enabled() {
        return Ok(());
    }

    let client_tls_config_for_watcher = client_tls_config.clone();
    let channel_manager_for_watcher = channel_manager.clone();

    let (tx, rx) = channel::<notify::Result<notify::Event>>();
    let mut watcher = notify::recommended_watcher(tx).context(FileWatchSnafu { path: "<none>" })?;

    // Watch client cert if present
    if let Some(cert_path) = client_tls_config.cert_path() {
        watcher
            .watch(cert_path, RecursiveMode::NonRecursive)
            .with_context(|_| FileWatchSnafu {
                path: cert_path.display().to_string(),
            })?;
    }

    // Watch client key if present
    if let Some(key_path) = client_tls_config.key_path() {
        watcher
            .watch(key_path, RecursiveMode::NonRecursive)
            .with_context(|_| FileWatchSnafu {
                path: key_path.display().to_string(),
            })?;
    }

    // Watch server CA cert if present
    if let Some(ca_path) = client_tls_config.server_ca_cert_path() {
        watcher
            .watch(ca_path, RecursiveMode::NonRecursive)
            .with_context(|_| FileWatchSnafu {
                path: ca_path.display().to_string(),
            })?;
    }

    std::thread::spawn(move || {
        let _watcher = watcher;
        while let Ok(res) = rx.recv() {
            if let Ok(event) = res {
                match event.kind {
                    EventKind::Modify(_) | EventKind::Create(_) => {
                        info!("Detected TLS cert/key file change: {:?}", event);
                        if let Err(err) = client_tls_config_for_watcher.reload() {
                            error!(err; "Failed to reload TLS client config");
                        } else {
                            info!("Reloaded TLS cert/key file successfully.");
                            // Clear all existing channels to force reconnection with new certificates
                            channel_manager_for_watcher.clear_all_channels();
                            info!("Cleared all existing channels to use new TLS certificates.");
                        }
                    }
                    _ => {}
                }
            }
        }
    });

    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientTlsOption {
    /// Whether to enable TLS for client.
    pub enabled: bool,
    pub server_ca_cert_path: Option<String>,
    pub client_cert_path: Option<String>,
    pub client_key_path: Option<String>,
    pub watch: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelConfig {
    pub timeout: Option<Duration>,
    pub connect_timeout: Option<Duration>,
    pub concurrency_limit: Option<usize>,
    pub rate_limit: Option<(u64, Duration)>,
    pub initial_stream_window_size: Option<u32>,
    pub initial_connection_window_size: Option<u32>,
    pub http2_keep_alive_interval: Option<Duration>,
    pub http2_keep_alive_timeout: Option<Duration>,
    pub http2_keep_alive_while_idle: Option<bool>,
    pub http2_adaptive_window: Option<bool>,
    pub tcp_keepalive: Option<Duration>,
    pub tcp_nodelay: bool,
    pub client_tls: Option<ClientTlsOption>,
    // Max gRPC receiving(decoding) message size
    pub max_recv_message_size: ReadableSize,
    // Max gRPC sending(encoding) message size
    pub max_send_message_size: ReadableSize,
    pub send_compression: bool,
    pub accept_compression: bool,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            timeout: Some(Duration::from_secs(DEFAULT_GRPC_REQUEST_TIMEOUT_SECS)),
            connect_timeout: Some(Duration::from_secs(DEFAULT_GRPC_CONNECT_TIMEOUT_SECS)),
            concurrency_limit: None,
            rate_limit: None,
            initial_stream_window_size: None,
            initial_connection_window_size: None,
            http2_keep_alive_interval: Some(Duration::from_secs(30)),
            http2_keep_alive_timeout: None,
            http2_keep_alive_while_idle: Some(true),
            http2_adaptive_window: None,
            tcp_keepalive: None,
            tcp_nodelay: true,
            client_tls: None,
            max_recv_message_size: DEFAULT_MAX_GRPC_RECV_MESSAGE_SIZE,
            max_send_message_size: DEFAULT_MAX_GRPC_SEND_MESSAGE_SIZE,
            send_compression: false,
            accept_compression: false,
        }
    }
}

impl ChannelConfig {
    pub fn new() -> Self {
        Default::default()
    }

    /// A timeout to each request.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// A timeout to connecting to the uri.
    ///
    /// Defaults to no timeout.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// A concurrency limit to each request.
    pub fn concurrency_limit(mut self, limit: usize) -> Self {
        self.concurrency_limit = Some(limit);
        self
    }

    /// A rate limit to each request.
    pub fn rate_limit(mut self, limit: u64, duration: Duration) -> Self {
        self.rate_limit = Some((limit, duration));
        self
    }

    /// Sets the SETTINGS_INITIAL_WINDOW_SIZE option for HTTP2 stream-level flow control.
    /// Default is 65,535
    pub fn initial_stream_window_size(mut self, size: u32) -> Self {
        self.initial_stream_window_size = Some(size);
        self
    }

    /// Sets the max connection-level flow control for HTTP2
    ///
    /// Default is 65,535
    pub fn initial_connection_window_size(mut self, size: u32) -> Self {
        self.initial_connection_window_size = Some(size);
        self
    }

    /// Set http2 KEEP_ALIVE_INTERVAL. Uses hyper’s default otherwise.
    pub fn http2_keep_alive_interval(mut self, duration: Duration) -> Self {
        self.http2_keep_alive_interval = Some(duration);
        self
    }

    /// Set http2 KEEP_ALIVE_TIMEOUT. Uses hyper’s default otherwise.
    pub fn http2_keep_alive_timeout(mut self, duration: Duration) -> Self {
        self.http2_keep_alive_timeout = Some(duration);
        self
    }

    /// Set http2 KEEP_ALIVE_WHILE_IDLE. Uses hyper’s default otherwise.
    pub fn http2_keep_alive_while_idle(mut self, enabled: bool) -> Self {
        self.http2_keep_alive_while_idle = Some(enabled);
        self
    }

    /// Sets whether to use an adaptive flow control. Uses hyper’s default otherwise.
    pub fn http2_adaptive_window(mut self, enabled: bool) -> Self {
        self.http2_adaptive_window = Some(enabled);
        self
    }

    /// Set whether TCP keepalive messages are enabled on accepted connections.
    ///
    /// If None is specified, keepalive is disabled, otherwise the duration specified
    /// will be the time to remain idle before sending TCP keepalive probes.
    ///
    /// Default is no keepalive (None)
    pub fn tcp_keepalive(mut self, duration: Duration) -> Self {
        self.tcp_keepalive = Some(duration);
        self
    }

    /// Set the value of TCP_NODELAY option for accepted connections.
    ///
    /// Enabled by default.
    pub fn tcp_nodelay(mut self, enabled: bool) -> Self {
        self.tcp_nodelay = enabled;
        self
    }

    /// Set the value of tls client auth.
    ///
    /// Disabled by default.
    pub fn client_tls_config(mut self, client_tls_option: ClientTlsOption) -> Self {
        self.client_tls = Some(client_tls_option);
        self
    }
}

#[derive(Debug)]
pub struct Channel {
    channel: InnerChannel,
    access: AtomicUsize,
    use_default_connector: bool,
}

impl Channel {
    #[inline]
    pub fn access(&self) -> usize {
        self.access.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn use_default_connector(&self) -> bool {
        self.use_default_connector
    }

    #[inline]
    pub fn increase_access(&self) {
        let _ = self.access.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Default)]
struct Pool {
    channels: DashMap<String, Channel>,
}

impl Pool {
    fn get(&self, addr: &str) -> Option<InnerChannel> {
        let channel = self.channels.get(addr);
        channel.map(|ch| {
            ch.increase_access();
            ch.channel.clone()
        })
    }

    fn entry(&self, addr: String) -> Entry<'_, String, Channel> {
        self.channels.entry(addr)
    }

    #[cfg(test)]
    fn get_access(&self, addr: &str) -> Option<usize> {
        let channel = self.channels.get(addr);
        channel.map(|ch| ch.access())
    }

    fn put(&self, addr: &str, channel: Channel) {
        let _ = self.channels.insert(addr.to_string(), channel);
    }

    fn retain_channel<F>(&self, f: F)
    where
        F: FnMut(&String, &mut Channel) -> bool,
    {
        self.channels.retain(f);
    }
}

async fn recycle_channel_in_loop(
    pool: Arc<Pool>,
    id: u64,
    cancel: CancellationToken,
    interval_secs: u64,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("Stop channel recycle, ChannelManager id: {}", id);
                break;
            },
            _ = interval.tick() => {}
        }

        pool.retain_channel(|_, c| c.access.swap(0, Ordering::Relaxed) != 0)
    }
}

#[cfg(test)]
mod tests {
    use tower::service_fn;

    use super::*;

    #[should_panic]
    #[test]
    fn test_invalid_addr() {
        let mgr = ChannelManager::default();
        let addr = "http://test";

        let _ = mgr.get(addr).unwrap();
    }

    #[tokio::test]
    async fn test_access_count() {
        let mgr = ChannelManager::new();
        // Do not start recycle
        mgr.inner
            .channel_recycle_started
            .store(true, Ordering::Relaxed);
        let mgr = Arc::new(mgr);
        let addr = "test_uri";

        let mut joins = Vec::with_capacity(10);
        for _ in 0..10 {
            let mgr_clone = mgr.clone();
            let join = tokio::spawn(async move {
                for _ in 0..100 {
                    let _ = mgr_clone.get(addr);
                }
            });
            joins.push(join);
        }
        for join in joins {
            join.await.unwrap();
        }

        assert_eq!(1000, mgr.pool().get_access(addr).unwrap());

        mgr.pool()
            .retain_channel(|_, c| c.access.swap(0, Ordering::Relaxed) != 0);

        assert_eq!(0, mgr.pool().get_access(addr).unwrap());
    }

    #[test]
    fn test_config() {
        let default_cfg = ChannelConfig::new();
        assert_eq!(
            ChannelConfig {
                timeout: Some(Duration::from_secs(DEFAULT_GRPC_REQUEST_TIMEOUT_SECS)),
                connect_timeout: Some(Duration::from_secs(DEFAULT_GRPC_CONNECT_TIMEOUT_SECS)),
                concurrency_limit: None,
                rate_limit: None,
                initial_stream_window_size: None,
                initial_connection_window_size: None,
                http2_keep_alive_interval: Some(Duration::from_secs(30)),
                http2_keep_alive_timeout: None,
                http2_keep_alive_while_idle: Some(true),
                http2_adaptive_window: None,
                tcp_keepalive: None,
                tcp_nodelay: true,
                client_tls: None,
                max_recv_message_size: DEFAULT_MAX_GRPC_RECV_MESSAGE_SIZE,
                max_send_message_size: DEFAULT_MAX_GRPC_SEND_MESSAGE_SIZE,
                send_compression: false,
                accept_compression: false,
            },
            default_cfg
        );

        let cfg = default_cfg
            .timeout(Duration::from_secs(3))
            .connect_timeout(Duration::from_secs(5))
            .concurrency_limit(6)
            .rate_limit(5, Duration::from_secs(1))
            .initial_stream_window_size(10)
            .initial_connection_window_size(20)
            .http2_keep_alive_interval(Duration::from_secs(1))
            .http2_keep_alive_timeout(Duration::from_secs(3))
            .http2_keep_alive_while_idle(true)
            .http2_adaptive_window(true)
            .tcp_keepalive(Duration::from_secs(2))
            .tcp_nodelay(false)
            .client_tls_config(ClientTlsOption {
                enabled: true,
                server_ca_cert_path: Some("some_server_path".to_string()),
                client_cert_path: Some("some_cert_path".to_string()),
                client_key_path: Some("some_key_path".to_string()),
                watch: false,
            });

        assert_eq!(
            ChannelConfig {
                timeout: Some(Duration::from_secs(3)),
                connect_timeout: Some(Duration::from_secs(5)),
                concurrency_limit: Some(6),
                rate_limit: Some((5, Duration::from_secs(1))),
                initial_stream_window_size: Some(10),
                initial_connection_window_size: Some(20),
                http2_keep_alive_interval: Some(Duration::from_secs(1)),
                http2_keep_alive_timeout: Some(Duration::from_secs(3)),
                http2_keep_alive_while_idle: Some(true),
                http2_adaptive_window: Some(true),
                tcp_keepalive: Some(Duration::from_secs(2)),
                tcp_nodelay: false,
                client_tls: Some(ClientTlsOption {
                    enabled: true,
                    server_ca_cert_path: Some("some_server_path".to_string()),
                    client_cert_path: Some("some_cert_path".to_string()),
                    client_key_path: Some("some_key_path".to_string()),
                    watch: false,
                }),
                max_recv_message_size: DEFAULT_MAX_GRPC_RECV_MESSAGE_SIZE,
                max_send_message_size: DEFAULT_MAX_GRPC_SEND_MESSAGE_SIZE,
                send_compression: false,
                accept_compression: false,
            },
            cfg
        );
    }

    #[test]
    fn test_build_endpoint() {
        let config = ChannelConfig::new()
            .timeout(Duration::from_secs(3))
            .connect_timeout(Duration::from_secs(5))
            .concurrency_limit(6)
            .rate_limit(5, Duration::from_secs(1))
            .initial_stream_window_size(10)
            .initial_connection_window_size(20)
            .http2_keep_alive_interval(Duration::from_secs(1))
            .http2_keep_alive_timeout(Duration::from_secs(3))
            .http2_keep_alive_while_idle(true)
            .http2_adaptive_window(true)
            .tcp_keepalive(Duration::from_secs(2))
            .tcp_nodelay(true);
        let mgr = ChannelManager::with_config(config, None);

        let res = mgr.build_endpoint("test_addr");

        let _ = res.unwrap();
    }

    #[tokio::test]
    async fn test_channel_with_connector() {
        let mgr = ChannelManager::new();

        let addr = "test_addr";
        let res = mgr.get(addr);
        let _ = res.unwrap();

        mgr.retain_channel(|addr, channel| {
            assert_eq!("test_addr", addr);
            assert!(channel.use_default_connector());
            true
        });

        let (client, _) = tokio::io::duplex(1024);
        let mut client = Some(hyper_util::rt::TokioIo::new(client));
        let res = mgr.reset_with_connector(
            addr,
            service_fn(move |_| {
                let client = client.take().unwrap();
                async move { Ok::<_, std::io::Error>(client) }
            }),
        );

        let _ = res.unwrap();

        mgr.retain_channel(|addr, channel| {
            assert_eq!("test_addr", addr);
            assert!(!channel.use_default_connector());
            true
        });
    }

    #[tokio::test]
    async fn test_pool_release_with_channel_recycle() {
        let mgr = ChannelManager::new();

        let pool_holder = mgr.pool().clone();

        // start channel recycle task
        let addr = "test_addr";
        let _ = mgr.get(addr);

        let mgr_clone_1 = mgr.clone();
        let mgr_clone_2 = mgr.clone();
        assert_eq!(3, Arc::strong_count(mgr.pool()));

        drop(mgr_clone_1);
        drop(mgr_clone_2);
        assert_eq!(3, Arc::strong_count(mgr.pool()));

        drop(mgr);

        // wait for the channel recycle task to finish
        tokio::time::sleep(Duration::from_millis(10)).await;

        assert_eq!(1, Arc::strong_count(&pool_holder));
    }

    #[tokio::test]
    async fn test_pool_release_without_channel_recycle() {
        let mgr = ChannelManager::new();

        let pool_holder = mgr.pool().clone();

        let mgr_clone_1 = mgr.clone();
        let mgr_clone_2 = mgr.clone();
        assert_eq!(2, Arc::strong_count(mgr.pool()));

        drop(mgr_clone_1);
        drop(mgr_clone_2);
        assert_eq!(2, Arc::strong_count(mgr.pool()));

        drop(mgr);

        assert_eq!(1, Arc::strong_count(&pool_holder));
    }
}
