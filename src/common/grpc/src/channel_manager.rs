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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use snafu::{OptionExt, ResultExt};
use tonic::transport::{
    Certificate, Channel as InnerChannel, ClientTlsConfig, Endpoint, Identity, Uri,
};
use tower::make::MakeConnection;

use crate::error::{CreateChannelSnafu, InvalidConfigFilePathSnafu, InvalidTlsConfigSnafu, Result};

const RECYCLE_CHANNEL_INTERVAL_SECS: u64 = 60;

#[derive(Clone, Debug)]
pub struct ChannelManager {
    config: ChannelConfig,
    client_tls_config: Option<ClientTlsConfig>,
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

        Self {
            config,
            client_tls_config: None,
            pool,
        }
    }

    pub fn with_tls_config(config: ChannelConfig) -> Result<Self> {
        let pool = Arc::new(Pool::default());
        let cloned_pool = pool.clone();

        common_runtime::spawn_bg(async {
            recycle_channel_in_loop(cloned_pool, RECYCLE_CHANNEL_INTERVAL_SECS).await;
        });

        // setup tls
        let path_config = config.client_tls.clone().context(InvalidTlsConfigSnafu {
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

        Ok(Self {
            config,
            client_tls_config: Some(
                ClientTlsConfig::new()
                    .ca_certificate(server_root_ca_cert)
                    .identity(client_identity),
            ),
            pool,
        })
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
        let mut endpoint = Endpoint::new(format!("http://{addr}")).context(CreateChannelSnafu)?;

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
            client_tls: None,
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

    /// Set the value of tls client auth.
    ///
    /// Disabled by default.
    pub fn client_tls_config(self, client_tls_option: ClientTlsOption) -> Self {
        Self {
            client_tls: Some(client_tls_option),
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
    use std::fs::File;
    use std::io::Write;

    use tempdir::TempDir;
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
        let mgr = Arc::new(ChannelManager {
            pool,
            config,
            client_tls_config: None,
        });
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
                client_tls: None,
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
        let mgr = ChannelManager {
            pool,
            config,
            client_tls_config: None,
        };

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
        let mgr = ChannelManager {
            pool,
            config,
            client_tls_config: None,
        };

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

    #[tokio::test]
    async fn test_mtls_config() {
        // test no config
        let config = ChannelConfig::new();
        let re = ChannelManager::with_tls_config(config);
        assert!(re.is_err());

        let dir = create_tmp_mtls_file();
        let dir_path = dir.path().to_str().unwrap();

        // test wrong file
        let config = ChannelConfig::new().client_tls_config(ClientTlsOption {
            server_ca_cert_path: format!("{dir_path}/wrong_server_ca_cert"),
            client_cert_path: format!("{dir_path}/wrong_client_cert"),
            client_key_path: format!("{dir_path}/wrong_client_key"),
        });

        let re = ChannelManager::with_tls_config(config);
        assert!(re.is_err());

        // test corrupted file content
        let config = ChannelConfig::new().client_tls_config(ClientTlsOption {
            server_ca_cert_path: format!("{dir_path}/server_ca_cert"),
            client_cert_path: format!("{dir_path}/client_cert"),
            client_key_path: format!("{dir_path}/corrupted"),
        });

        let re = ChannelManager::with_tls_config(config);
        assert!(re.is_ok());
        let re = re.unwrap().get("127.0.0.1:0");
        assert!(re.is_err());

        // success
        let config = ChannelConfig::new().client_tls_config(ClientTlsOption {
            server_ca_cert_path: format!("{dir_path}/server_ca_cert"),
            client_cert_path: format!("{dir_path}/client_cert"),
            client_key_path: format!("{dir_path}/client_key"),
        });

        let re = ChannelManager::with_tls_config(config);
        assert!(re.is_ok());
        let re = re.unwrap().get("127.0.0.1:0");
        assert!(re.is_ok());
    }

    fn create_tmp_mtls_file() -> TempDir {
        // mtls file create by this function
        // is strictly for test purpose only

        let dir = TempDir::new("grpc_client_mtls_test").unwrap();
        let dir_path = dir.path().to_str().unwrap();

        let mut server_ca = File::create(format!("{dir_path}/server_ca_cert")).unwrap();
        server_ca.write_all(SERVER_CERT_STR.as_bytes()).unwrap();

        let mut client_cert = File::create(format!("{dir_path}/client_cert")).unwrap();
        client_cert.write_all(CLIENT_CERT_STR.as_bytes()).unwrap();

        let mut client_key = File::create(format!("{dir_path}/client_key")).unwrap();
        client_key.write_all(CLIENT_KEY_STR.as_bytes()).unwrap();

        let mut corrupted = File::create(format!("{dir_path}/corrupted")).unwrap();
        corrupted.write_all(CORRUPTED_STR.as_bytes()).unwrap();

        dir
    }

    const SERVER_CERT_STR: &str = r#"
    -----BEGIN CERTIFICATE-----
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
95I025URIlhJGLGBUlA=
-----END CERTIFICATE-----
    "#;

    const CLIENT_CERT_STR: &str = r#"
-----BEGIN CERTIFICATE-----
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
dBCSsexiXj/S5KwKtz/c
-----END CERTIFICATE-----
    "#;

    const CLIENT_KEY_STR: &str = r#"
-----BEGIN RSA PRIVATE KEY-----
MIIJKQIBAAKCAgEAzT4l2RStXAznr3VOetvMagGYx59nvOoQ/YodCB70Cm3AJmvH
4RTEPhpIvqdvhmtX8Rw4tfmlJ9NFI8iwMUjEFbpaWVmB7EY95KMg/S8k76Q0JEsC
YRTbS991palDLY4FV6M54xAn4XP4cCifQUtG+Y9tV86mJEikyTXMWZqDSyVby3X2
7Foy8sEgB23BdYLNsNyMa7j0MMjFy9VEY3MBkU7Svs5OkBsYMl7CBCQ1uBGakcV4
JhM+48e73wy2mHPBqLo05ItnZMk1kJHQ6yePze1Vhjhoy+tBCQznEMgqMA7gKrGw
eQvTroQefaNv8M3knmmO6kuZyzrn8ImJr3NUupIIvXpoq8ahS5q3Gib/EDwxF+rn
yPMEpAkvwl/yFDVz5Eq4qVN80HI6K4pwMN8PaRmNVTZJ8+QIA16xAFte2mSrKUMi
0spfO7xvXYY8m3ZHO0MgYIU8cHoIGE9SDWOnZo/YoyMlPvVeuMTk2/XZW4C1v6gL
2cnqth/WeWgcJBt9Z9W9q19CRCvvzQvvD0bNrVqutxDgfIc4eJhfUSi3GQt80vXi
WtQdLoMsuX8PBhlgOPR96xaFNru/hms83Z+bg/bKi7lZ81tRURA4TBxneFXUcb7o
VKfY8/btFg0a9iMeze1Xee5sIQT4r1MsIO0MGUk6pSKxlaEBTy+AY1sEkrMCAwEA
AQKCAgEAw2jBZj5+k96hk/dPIkA1DlS43o7RmRcN2CdwXrQBzBAUW0BRDObVtP8X
dZY647M+BozFHdUzPoizEk/YGQRb1QgZT2qd/ZQfB5mdJhGFzDf9gPR9rmrKJCH8
hB50nGHUik0ZJyvRnKDqz/aNMgB28dJx26Efo/oaEoyLJGCtUpWeIUgOMZfrXB8t
3ITOJZDFP/esJj/xFqWBVQGXXEw6GNwAYLRSLnftgL+hX4oOL1NrZBCrxSybuwkG
wWX8T4gewQOQqmxjo5zCyANc8xc2nmyx+dmpRUWWJQTI1ryNFjaDjYKiL41oHIcj
9KDwSkftvDlqXX5fThSmkeiRU5+t8UMj4+Bt7opCzIlwHtQe+95BqiXQ7bHfCjn7
GvShZgHo45rDkfwWDz/pYhHQ2Wb9DkhEtwa0cu3mDMGc6BY+4yo+Vz6Rk1TypxQw
LIa43WgVCRm66Mq65sObx7wkdxvolUE8j1Io3AHwgeBjV+gISV9srj2m/HnOmFFb
16SKQEDEVoaci+v6DT8A7UOZH4sgYSbknHdjMy6c6UlYgd8UNqbY3h/ohZ2JOcPd
8DqGUDGKbpS7OxWogxb9K++6SPSn86sPmUjzRPMgijVjU5pyK42DpZj1/RIe8Tml
JXVqHuZvURK4Qi3ECQ09m9vQ9nS88HMRVJ7sFSca6HOFYSFyAfkCggEBAPN35hva
OhbgQlFJrpo5YDYS5v7l7YjLbry6DaCR1CpYaKlTPkc4tiznCUHe1N41mR4qu2Tc
4+m7GN9BZfLU8w/Jvrp7mAO7fZXZtIzTQrZQDbAZppUGBbGBoAOlLVxR4NrN2TSk
49Ljj87UynhxeCv6RWx0F1p1/VIZertLELbSdb3C43pAsNSXzbkb7LtT9RXemyUL
LBK4ugcXMSZrzHJK1Ct31LoGd9m+TEp/VW2aGMeWliuIticJx44OW4tlJ70qKrd0
KezBZVMHPa3FqW7kdYwdlISoqZsE9OVPgLCQNVLhDO1YMaTl3WEHKTRBxTF70pvv
zMkSRQGoU4ff7AUCggEBANfOkCsx2mRvJV+UYxW8R6510w2H1bNbNRbfTJAo8kld
/7dXU4H3QrhUrCsSyc5ijm09q7I4+rc+uMxfT/R1mO5tq9AueCWhg85WV+NBR1FE
Yg7MX+zblpHqUDQoTj9vvgwLyqvZ7k9NON42Zz+Tj2worICnVlDvahm/3NaItT9B
oGhsEoJjYFK4Hq7RwosU+KPXkQBxWzrNLipo8jx0XFpPZVHSLIFs9eW25bnj/qxc
toMgx4IsvEDlzS/oqfycCrDdKwqiW74w0Djb5TiJv+dYzl9GnN6istqbUTNZkJjn
lkbmegrtfz3Yd1ORvjkNqHuANyuR+YnUSIsb0PV5eVcCggEAck5bgb4eQbk+SY3P
ZOcFLb4IJ6ppsCzaq86qMTXmJ49kbAMCHUwZ89DwvrVQuZbucYRcgMlYU9ccoUzC
AZVLHKF6Y3E9eJshJiaVJvzUuGWzV3djh1nReHpEVxHIzyw95lx42seDkvJ2BQRQ
nuWfJv6Uc4u5nyYALfh6b86ZZUxALTx/slkG7HjtBDiBF54eVgsySd0J7yw9YrDX
yZMY5JwPKu1SuZfp0xgOF3fa8t9DPQmNLZk88+0afK5u+m4ejyhp78GhIV/XI3kl
0x0XJFIsggEtRm8tWfOkyrhd0geSkXvJpvEeNa4aFsDW7ormewoIYl/ehJSIQ3P0
67kMxQKCAQA12iP7w2r+GQY4fazkJaG1lU1fWQAoy5/J31sZtj4PtNc1ByOdkPgj
S23TKdMWH13vQK5xwOo/g/VVeotXM2lARjnTr2Tn7xAXE1DHMuj7DJdznehqELnY
G6J8AXrVNas1ElQ24iEnxNtmCClnogjuMpApYpiVhcjyOACBwIeKC3Rd2mocA3Rr
7+ooMcvcLRWGvSo/9AmR+NWGW73m/Bp3psxfyJS2j1wlQKi+5HgOxuv8eNeQUl1/
zFiRlfulP8MjM22kL7O5GDE9nxHqM+Whc3W8LMDEhdEf4BY5PCZrIY9MjgLyayWP
Z08PmZTgY9ohR3N8+eZNUJ3xqLVSLEftAoIBAQDF1K8lPXAs8e4V0oc9hq4GFLvi
E0KC+8X1ShzvkVGV/3Kz1FJ0bwix/M3C5XSSNguxHI6CG2GprJlExp1qqwlvmGr2
hHdfemvq6tF4qjXLgPXvgoWocBGNUvBXxFVuc0hOHgT/X3+GsPYtNvZb3fp+4Bm6
ugUu05drqrHSOY5kUbU3jf/5KctnDFmOsSeOgGiI/JJWVcKJALpDkhazRL0nxfuW
6xU6pZazhCAby2Qn+wn0xyi4bEZSNobiQTgOXOC0DA1uGD3XHctCMnSBtYtocQjq
IFT2l3u4pEKpVQwuc4+yObWUT47oBxV6vFneXsnV89vd2SSUPuR8GIYYeA+/
-----END RSA PRIVATE KEY-----
    "#;

    const CORRUPTED_STR: &str = r#"
rWtZ7U3SoVAl6yMhfJsB
LcEGbuCfgFxk2ADw0N1G
byTKlrUgoRZeSc0cYHTf
0XjbRCBtMV9yYaVJKPwi
rGofQgFoc1lW0U5x2bnN
O9nn9aDe5t5LAlGS81uX
aBMvuzVjHbZKOlabXl4W
ZJc06qngAcQWQUu8nAnR
FLsjhoaTyuaDMY3OWJAx
5Dt7YglND5uFAqYwRG9L
agLGOCH8suwnXGYaPxjM
Ysb5RANkpgcbSulLZiic
4sLmpJomjokwZbctODVW
pCLiQT3wWDJ7YjIePR6g
P3Jlg0LDhbgSwXxgjjUR
6qGRfcb8LFlVlT7O1ze2
lFBNWzijkPeKyKmwpOSa
oGCR2OUg71n0Tzt2a3ir
WLijq0bL1Cetz24fv738
L3MEAwezFBW38U4QilNz
uza1bC3PgToermGSgKLx
WMdgjZIszK4t6Rehelx8
YpCJWVXTob3Gn4bMwWJO
xpJ9qhvMBdD8iamheF4b
bUm1YmHW4gPT1ujiqCmN
I7hOFurjJ6zvXGETyfCn
w23W8PNFWbqpHUKN59Bz
HpbsIRDVVpEGxnoWmdjq
58BUOxDdbTZxCKt0UqLD
uUPOlW8bRhuC1tK1NL5u
wq9ybcfwZ4jIHyYlHZ5M
4t4zKLRG2DN6icHmctOW
TzYp3np0OFsTlzCwkogM
Os6SOvjU0Irq2Xo5wLvn
1nN6FQwUxcw0H5rfQEZo
NioHP0JdBv3HmIaQZs1n
8lJWLVof1TBWtRUKmWmO
79DcTURdzt28Vdn6F0K0
UiG15bda4Pb81I9IE9ug
iZkC7CE98aE6WQK9Ghlu
dNXJTkUD3uVg6Tqi3957
Hfa9xMclyrxsOvkGcudI
QbcvG5Apom6nBWIGHRMQ
68rn9eZEcq5mJLaiNmHr
5AOtHddC5NVgQLgdmmKb
gQlrcSXzxT6V6jzbxZ79
xmulvmkeqG4kj6TAuJEg
u9dCkExxv5tLSpF8hC08
HHU4QE56UC97djO5EpmK
g3rElyboRHlAYPWviWbm
    "#;
}
