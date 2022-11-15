// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use snafu::ResultExt;
use tonic::transport::{Channel as InnerChannel, Endpoint, Uri};
use tower::make::MakeConnection;

use crate::error;
use crate::error::Result;

const RECYCLE_CHANNEL_INTERVAL_SECS: u64 = 60;

#[derive(Clone, Debug)]
pub struct ChannelManager {
    config: ChannelConfig,
    pool: Arc<Pool>,
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
        let pool = Arc::new(Pool::default());
        let cloned_pool = pool.clone();

        common_runtime::spawn_bg(async {
            recycle_channel_in_loop(cloned_pool, RECYCLE_CHANNEL_INTERVAL_SECS).await;
        });

        Self { config, pool }
    }

    pub fn config(&self) -> &ChannelConfig {
        &self.config
    }

    pub fn get(&self, addr: impl AsRef<str>) -> Result<InnerChannel> {
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
        let mut endpoint =
            Endpoint::new(format!("http://{}", addr)).context(error::CreateChannelSnafu)?;

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
        endpoint = endpoint
            .tcp_keepalive(self.config.tcp_keepalive)
            .tcp_nodelay(self.config.tcp_nodelay);

        Ok(endpoint)
    }
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
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            timeout: None,
            connect_timeout: None,
            concurrency_limit: None,
            rate_limit: None,
            initial_stream_window_size: None,
            initial_connection_window_size: None,
            http2_keep_alive_interval: None,
            http2_keep_alive_timeout: None,
            http2_keep_alive_while_idle: None,
            http2_adaptive_window: None,
            tcp_keepalive: None,
            tcp_nodelay: true,
        }
    }
}

impl ChannelConfig {
    pub fn new() -> Self {
        Default::default()
    }

    /// A timeout to each request.
    pub fn timeout(self, timeout: Duration) -> Self {
        Self {
            timeout: Some(timeout),
            ..self
        }
    }

    /// A timeout to connecting to the uri.
    ///
    /// Defaults to no timeout.
    pub fn connect_timeout(self, timeout: Duration) -> Self {
        Self {
            connect_timeout: Some(timeout),
            ..self
        }
    }

    /// A concurrency limit to each request.
    pub fn concurrency_limit(self, limit: usize) -> Self {
        Self {
            concurrency_limit: Some(limit),
            ..self
        }
    }

    /// A rate limit to each request.
    pub fn rate_limit(self, limit: u64, duration: Duration) -> Self {
        Self {
            rate_limit: Some((limit, duration)),
            ..self
        }
    }

    /// Sets the SETTINGS_INITIAL_WINDOW_SIZE option for HTTP2 stream-level flow control.
    /// Default is 65,535
    pub fn initial_stream_window_size(self, size: u32) -> Self {
        Self {
            initial_stream_window_size: Some(size),
            ..self
        }
    }

    /// Sets the max connection-level flow control for HTTP2
    ///
    /// Default is 65,535
    pub fn initial_connection_window_size(self, size: u32) -> Self {
        Self {
            initial_connection_window_size: Some(size),
            ..self
        }
    }

    /// Set http2 KEEP_ALIVE_INTERVAL. Uses hyper’s default otherwise.
    pub fn http2_keep_alive_interval(self, duration: Duration) -> Self {
        Self {
            http2_keep_alive_interval: Some(duration),
            ..self
        }
    }

    /// Set http2 KEEP_ALIVE_TIMEOUT. Uses hyper’s default otherwise.
    pub fn http2_keep_alive_timeout(self, duration: Duration) -> Self {
        Self {
            http2_keep_alive_timeout: Some(duration),
            ..self
        }
    }

    /// Set http2 KEEP_ALIVE_WHILE_IDLE. Uses hyper’s default otherwise.
    pub fn http2_keep_alive_while_idle(self, enabled: bool) -> Self {
        Self {
            http2_keep_alive_while_idle: Some(enabled),
            ..self
        }
    }

    /// Sets whether to use an adaptive flow control. Uses hyper’s default otherwise.
    pub fn http2_adaptive_window(self, enabled: bool) -> Self {
        Self {
            http2_adaptive_window: Some(enabled),
            ..self
        }
    }

    /// Set whether TCP keepalive messages are enabled on accepted connections.
    ///
    /// If None is specified, keepalive is disabled, otherwise the duration specified
    /// will be the time to remain idle before sending TCP keepalive probes.
    ///
    /// Default is no keepalive (None)
    pub fn tcp_keepalive(self, duration: Duration) -> Self {
        Self {
            tcp_keepalive: Some(duration),
            ..self
        }
    }

    /// Set the value of TCP_NODELAY option for accepted connections.
    ///
    /// Enabled by default.
    pub fn tcp_nodelay(self, enabled: bool) -> Self {
        Self {
            tcp_nodelay: enabled,
            ..self
        }
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
        self.access.fetch_add(1, Ordering::Relaxed);
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
        self.channels.insert(addr.to_string(), channel);
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
        interval.tick().await;
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
        let pool = Arc::new(Pool::default());
        let config = ChannelConfig::new();
        let mgr = Arc::new(ChannelManager { pool, config });
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
                timeout: None,
                connect_timeout: None,
                concurrency_limit: None,
                rate_limit: None,
                initial_stream_window_size: None,
                initial_connection_window_size: None,
                http2_keep_alive_interval: None,
                http2_keep_alive_timeout: None,
                http2_keep_alive_while_idle: None,
                http2_adaptive_window: None,
                tcp_keepalive: None,
                tcp_nodelay: true,
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
            .tcp_nodelay(false);

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
            },
            cfg
        );
    }

    #[test]
    fn test_build_endpoint() {
        let pool = Arc::new(Pool::default());
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
        let mgr = ChannelManager { pool, config };

        let res = mgr.build_endpoint("test_addr");

        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_channel_with_connector() {
        let pool = Pool {
            channels: DashMap::default(),
        };

        let pool = Arc::new(pool);

        let config = ChannelConfig::new();
        let mgr = ChannelManager { pool, config };

        let addr = "test_addr";
        let res = mgr.get(addr);
        assert!(res.is_ok());

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

        assert!(res.is_ok());

        mgr.retain_channel(|addr, channel| {
            assert_eq!("test_addr", addr);
            assert!(!channel.use_default_connector());
            true
        });
    }
}
