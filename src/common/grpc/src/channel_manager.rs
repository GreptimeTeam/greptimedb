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

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common_base::readable_size::ReadableSize;
use common_telemetry::info;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use lazy_static::lazy_static;
use snafu::{OptionExt, ResultExt};
use tonic::transport::{
    Certificate, Channel as InnerChannel, ClientTlsConfig, Endpoint, Identity, Uri,
};
use tower::make::MakeConnection;

use crate::error::{CreateChannelSnafu, InvalidConfigFilePathSnafu, InvalidTlsConfigSnafu, Result};

const RECYCLE_CHANNEL_INTERVAL_SECS: u64 = 60;
pub const DEFAULT_GRPC_REQUEST_TIMEOUT_SECS: u64 = 10;
pub const DEFAULT_GRPC_CONNECT_TIMEOUT_SECS: u64 = 1;
pub const DEFAULT_MAX_GRPC_RECV_MESSAGE_SIZE: ReadableSize = ReadableSize::mb(512);
pub const DEFAULT_MAX_GRPC_SEND_MESSAGE_SIZE: ReadableSize = ReadableSize::mb(512);

lazy_static! {
    static ref ID: AtomicU64 = AtomicU64::new(0);
}

#[derive(Clone, Debug)]
pub struct ChannelManager {
    id: u64,
    config: ChannelConfig,
    client_tls_config: Option<ClientTlsConfig>,
    pool: Arc<Pool>,
    channel_recycle_started: Arc<AtomicBool>,
}

impl Default for ChannelManager {
    fn default() -> Self {
        ChannelManager::with_config(ChannelConfig::default())
    }
}

impl ChannelManager {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_config(config: ChannelConfig) -> Self {
        let id = ID.fetch_add(1, Ordering::Relaxed);
        let pool = Arc::new(Pool::default());
        Self {
            id,
            config,
            client_tls_config: None,
            pool,
            channel_recycle_started: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn with_tls_config(config: ChannelConfig) -> Result<Self> {
        let mut cm = Self::with_config(config.clone());

        // setup tls
        let path_config = config.client_tls.context(InvalidTlsConfigSnafu {
            msg: "no config input",
        })?;

        let server_root_ca_cert = std::fs::read_to_string(path_config.server_ca_cert_path)
            .context(InvalidConfigFilePathSnafu)?;
        let server_root_ca_cert = Certificate::from_pem(server_root_ca_cert);
        let client_cert = std::fs::read_to_string(path_config.client_cert_path)
            .context(InvalidConfigFilePathSnafu)?;
        let client_key = std::fs::read_to_string(path_config.client_key_path)
            .context(InvalidConfigFilePathSnafu)?;
        let client_identity = Identity::from_pem(client_cert, client_key);

        cm.client_tls_config = Some(
            ClientTlsConfig::new()
                .ca_certificate(server_root_ca_cert)
                .identity(client_identity),
        );

        Ok(cm)
    }

    pub fn config(&self) -> &ChannelConfig {
        &self.config
    }

    pub fn get(&self, addr: impl AsRef<str>) -> Result<InnerChannel> {
        self.trigger_channel_recycling();

        let addr = addr.as_ref();
        // It will acquire the read lock.
        if let Some(inner_ch) = self.pool.get(addr) {
            return Ok(inner_ch);
        }

        // It will acquire the write lock.
        let entry = match self.pool.entry(addr.to_string()) {
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
        C: MakeConnection<Uri> + Send + 'static,
        C::Connection: Unpin + Send + 'static,
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
        self.pool.put(addr, channel);

        Ok(inner_channel)
    }

    pub fn retain_channel<F>(&self, f: F)
    where
        F: FnMut(&String, &mut Channel) -> bool,
    {
        self.pool.retain_channel(f);
    }

    fn build_endpoint(&self, addr: &str) -> Result<Endpoint> {
        let http_prefix = if self.client_tls_config.is_some() {
            "https"
        } else {
            "http"
        };

        let mut endpoint =
            Endpoint::new(format!("{http_prefix}://{addr}")).context(CreateChannelSnafu)?;

        if let Some(dur) = self.config.timeout {
            endpoint = endpoint.timeout(dur);
        }
        if let Some(dur) = self.config.connect_timeout {
            endpoint = endpoint.connect_timeout(dur);
        }
        if let Some(limit) = self.config.concurrency_limit {
            endpoint = endpoint.concurrency_limit(limit);
        }
        if let Some((limit, dur)) = self.config.rate_limit {
            endpoint = endpoint.rate_limit(limit, dur);
        }
        if let Some(size) = self.config.initial_stream_window_size {
            endpoint = endpoint.initial_stream_window_size(size);
        }
        if let Some(size) = self.config.initial_connection_window_size {
            endpoint = endpoint.initial_connection_window_size(size);
        }
        if let Some(dur) = self.config.http2_keep_alive_interval {
            endpoint = endpoint.http2_keep_alive_interval(dur);
        }
        if let Some(dur) = self.config.http2_keep_alive_timeout {
            endpoint = endpoint.keep_alive_timeout(dur);
        }
        if let Some(enabled) = self.config.http2_keep_alive_while_idle {
            endpoint = endpoint.keep_alive_while_idle(enabled);
        }
        if let Some(enabled) = self.config.http2_adaptive_window {
            endpoint = endpoint.http2_adaptive_window(enabled);
        }
        if let Some(tls_config) = &self.client_tls_config {
            endpoint = endpoint
                .tls_config(tls_config.clone())
                .context(CreateChannelSnafu)?;
        }

        endpoint = endpoint
            .tcp_keepalive(self.config.tcp_keepalive)
            .tcp_nodelay(self.config.tcp_nodelay);

        Ok(endpoint)
    }

    fn trigger_channel_recycling(&self) {
        if self
            .channel_recycle_started
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let pool = self.pool.clone();
        let _handle = common_runtime::spawn_global(async {
            recycle_channel_in_loop(pool, RECYCLE_CHANNEL_INTERVAL_SECS).await;
        });
        info!(
            "ChannelManager: {}, channel recycle is started, running in the background!",
            self.id
        );
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientTlsOption {
    pub server_ca_cert_path: String,
    pub client_cert_path: String,
    pub client_key_path: String,
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

    fn entry(&self, addr: String) -> Entry<String, Channel> {
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

async fn recycle_channel_in_loop(pool: Arc<Pool>, interval_secs: u64) {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));

    loop {
        let _ = interval.tick().await;
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
        let pool = Arc::new(Pool::default());
        let mgr = ChannelManager {
            pool,
            ..Default::default()
        };
        let addr = "http://test";

        let _ = mgr.get(addr).unwrap();
    }

    #[tokio::test]
    async fn test_access_count() {
        let mgr = ChannelManager::new();
        // Do not start recycle
        mgr.channel_recycle_started.store(true, Ordering::Relaxed);
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

        assert_eq!(1000, mgr.pool.get_access(addr).unwrap());

        mgr.pool
            .retain_channel(|_, c| c.access.swap(0, Ordering::Relaxed) != 0);

        assert_eq!(0, mgr.pool.get_access(addr).unwrap());
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
                server_ca_cert_path: "some_server_path".to_string(),
                client_cert_path: "some_cert_path".to_string(),
                client_key_path: "some_key_path".to_string(),
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
                    server_ca_cert_path: "some_server_path".to_string(),
                    client_cert_path: "some_cert_path".to_string(),
                    client_key_path: "some_key_path".to_string(),
                }),
                max_recv_message_size: DEFAULT_MAX_GRPC_RECV_MESSAGE_SIZE,
                max_send_message_size: DEFAULT_MAX_GRPC_SEND_MESSAGE_SIZE,
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
        let mgr = ChannelManager::with_config(config);

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
        let mut client = Some(client);
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
}
