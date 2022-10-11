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
    config: Option<Config>,
    pool: Arc<Mutex<Pool>>,
}

#[derive(Clone, Debug, Default)]
pub struct Config {
    /// A timeout to each request.
    pub timeout: Option<Duration>,
    /// A timeout to connecting to the uri.
    ///
    /// Defaults to no timeout.
    pub connect_timeout: Option<Duration>,
    /// A concurrency limit to each request.
    pub concurrency_limit: Option<usize>,
    /// A rate limit to each request.
    pub rate_limit: Option<(u64, Duration)>,
    /// Sets the SETTINGS_INITIAL_WINDOW_SIZE option for HTTP2 stream-level flow control.
    /// Default is 65,535
    pub initial_stream_window_size: Option<u32>,
    /// Sets the max connection-level flow control for HTTP2
    ///
    /// Default is 65,535
    pub initial_connection_window_size: Option<u32>,
    /// Set http2 KEEP_ALIVE_INTERVAL. Uses hyper’s default otherwise.
    pub http2_keep_alive_interval: Option<Duration>,
    /// Set http2 KEEP_ALIVE_TIMEOUT. Uses hyper’s default otherwise.
    pub http2_keep_alive_timeout: Option<Duration>,
    /// Set http2 KEEP_ALIVE_WHILE_IDLE. Uses hyper’s default otherwise.
    pub http2_keep_alive_while_idle: Option<bool>,
    /// Sets whether to use an adaptive flow control. Uses hyper’s default otherwise.
    pub http2_adaptive_window: Option<bool>,
    /// Set whether TCP keepalive messages are enabled on accepted connections.
    ///
    /// If None is specified, keepalive is disabled, otherwise the duration specified
    /// will be the time to remain idle before sending TCP keepalive probes.
    ///
    /// Default is no keepalive (None)
    pub tcp_keepalive: Option<Duration>,
    /// Set the value of TCP_NODELAY option for accepted connections.
    ///
    /// Enabled by default.
    pub tcp_nodelay: bool,
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

    pub fn with_config(config: Config) -> Self {
        let mut manager = ChannelManager::new();
        manager.config = Some(config);
        manager
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
            if let Some(sz) = cfg.initial_stream_window_size {
                endpoint = endpoint.initial_stream_window_size(sz);
            }
            if let Some(sz) = cfg.initial_connection_window_size {
                endpoint = endpoint.initial_connection_window_size(sz);
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
        let mgr = ChannelManager { pool, config: None };
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
}
