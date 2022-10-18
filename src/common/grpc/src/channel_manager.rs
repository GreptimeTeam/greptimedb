use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use snafu::ResultExt;
use tonic::transport::Channel as InnerChannel;
use tonic::transport::Endpoint;

use crate::error;
use crate::error::Result;

const RECYCLE_CHANNEL_INTERVAL_SECS: u64 = 60;

#[derive(Clone, Debug)]
pub struct ChannelManager {
    config: Option<ChannelConfig>,
    pool: Arc<Mutex<Pool>>,
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
struct Pool {
    channels: HashMap<String, Channel>,
}

#[derive(Debug)]
struct Channel {
    channel: InnerChannel,
    access: usize,
}

impl ChannelManager {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_config(config: ChannelConfig) -> Self {
        let mut manager = ChannelManager::new();
        manager.config = Some(config);
        manager
    }

    pub fn config(&self) -> Option<ChannelConfig> {
        self.config.clone()
    }

    pub fn get(&self, addr: impl AsRef<str>) -> Result<InnerChannel> {
        let addr = addr.as_ref();
        let mut pool = self.pool.lock().unwrap();
        if let Some(ch) = pool.get_mut(addr) {
            ch.access += 1;
            return Ok(ch.channel.clone());
        }

        let mut endpoint =
            Endpoint::new(format!("http://{}", addr)).context(error::CreateChannelSnafu)?;

        if let Some(cfg) = &self.config {
            if let Some(dur) = cfg.timeout {
                endpoint = endpoint.timeout(dur);
            }
            if let Some(dur) = cfg.connect_timeout {
                endpoint = endpoint.connect_timeout(dur);
            }
            if let Some(limit) = cfg.concurrency_limit {
                endpoint = endpoint.concurrency_limit(limit);
            }
            if let Some((limit, dur)) = cfg.rate_limit {
                endpoint = endpoint.rate_limit(limit, dur);
            }
            if let Some(size) = cfg.initial_stream_window_size {
                endpoint = endpoint.initial_stream_window_size(size);
            }
            if let Some(size) = cfg.initial_connection_window_size {
                endpoint = endpoint.initial_connection_window_size(size);
            }
            if let Some(dur) = cfg.http2_keep_alive_interval {
                endpoint = endpoint.http2_keep_alive_interval(dur);
            }
            if let Some(dur) = cfg.http2_keep_alive_timeout {
                endpoint = endpoint.keep_alive_timeout(dur);
            }
            if let Some(enabled) = cfg.http2_keep_alive_while_idle {
                endpoint = endpoint.keep_alive_while_idle(enabled);
            }
            if let Some(enabled) = cfg.http2_adaptive_window {
                endpoint = endpoint.http2_adaptive_window(enabled);
            }
            endpoint = endpoint
                .tcp_keepalive(cfg.tcp_keepalive)
                .tcp_nodelay(cfg.tcp_nodelay);
        }

        let inner_channel = endpoint.connect_lazy();
        let channel = Channel {
            channel: inner_channel.clone(),
            access: 1,
        };
        pool.put(addr, channel);

        Ok(inner_channel)
    }
}

impl Pool {
    #[inline]
    fn get_mut(&mut self, addr: &str) -> Option<&mut Channel> {
        self.channels.get_mut(addr)
    }

    #[inline]
    fn put(&mut self, addr: &str, channel: Channel) {
        self.channels.insert(addr.to_string(), channel);
    }

    #[inline]
    fn retain_channel<F>(&mut self, f: F)
    where
        F: FnMut(&String, &mut Channel) -> bool,
    {
        self.channels.retain(f);
    }
}

impl Default for ChannelManager {
    fn default() -> Self {
        let pool = Pool {
            channels: HashMap::default(),
        };
        let pool = Arc::new(Mutex::new(pool));
        let cloned_pool = pool.clone();

        common_runtime::spawn_bg(async move {
            recycle_channel_in_loop(cloned_pool, RECYCLE_CHANNEL_INTERVAL_SECS).await;
        });

        Self { pool, config: None }
    }
}

async fn recycle_channel_in_loop(pool: Arc<Mutex<Pool>>, interval_secs: u64) {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));

    loop {
        interval.tick().await;
        let mut pool = pool.lock().unwrap();
        pool.retain_channel(|_, c| {
            if c.access == 0 {
                false
            } else {
                c.access = 0;
                true
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[should_panic]
    #[test]
    fn test_invalid_addr() {
        let pool = Pool {
            channels: HashMap::default(),
        };
        let pool = Arc::new(Mutex::new(pool));
        let mgr = ChannelManager { pool, config: None };
        let addr = "http://test";

        let _ = mgr.get(addr).unwrap();
    }

    #[tokio::test]
    async fn test_access_count() {
        let pool = Pool {
            channels: HashMap::default(),
        };
        let pool = Arc::new(Mutex::new(pool));
        let config = ChannelConfig::new()
            .timeout(Duration::from_secs(1))
            .connect_timeout(Duration::from_secs(1))
            .concurrency_limit(1)
            .rate_limit(1, Duration::from_secs(1))
            .initial_stream_window_size(1)
            .initial_connection_window_size(1)
            .http2_keep_alive_interval(Duration::from_secs(1))
            .http2_keep_alive_timeout(Duration::from_secs(1))
            .http2_keep_alive_while_idle(true)
            .http2_adaptive_window(true)
            .tcp_keepalive(Duration::from_secs(1))
            .tcp_nodelay(true);
        let mgr = ChannelManager {
            pool,
            config: Some(config),
        };
        let addr = "test_uri";

        for i in 0..10 {
            {
                let _ = mgr.get(addr).unwrap();
                let mut pool = mgr.pool.lock().unwrap();
                assert_eq!(i + 1, pool.get_mut(addr).unwrap().access);
            }
        }

        let mut pool = mgr.pool.lock().unwrap();

        assert_eq!(10, pool.get_mut(addr).unwrap().access);

        pool.retain_channel(|_, c| {
            if c.access == 0 {
                false
            } else {
                c.access = 0;
                true
            }
        });

        assert_eq!(0, pool.get_mut(addr).unwrap().access);
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
}
