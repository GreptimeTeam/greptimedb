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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use api::v1::HealthCheckRequest;
use api::v1::flow::flow_client::FlowClient as PbFlowClient;
use api::v1::health_check_client::HealthCheckClient;
use api::v1::prometheus_gateway_client::PrometheusGatewayClient;
use api::v1::region::region_client::RegionClient as PbRegionClient;
use arrow_flight::flight_service_client::FlightServiceClient;
use common_grpc::channel_manager::{
    ChannelConfig, ChannelManager, ClientTlsOption, load_client_tls_config,
};
use parking_lot::RwLock;
use snafu::{OptionExt, ResultExt};
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;

use crate::load_balance::{LoadBalance, Loadbalancer};
use crate::{Result, error};

const DEFAULT_HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Options for a gRPC client.
#[derive(Clone, Debug)]
pub struct ClientOptions {
    /// Interval for refreshing peer health. `Duration::ZERO` disables background health checks.
    pub health_check_interval: Duration,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            health_check_interval: DEFAULT_HEALTH_CHECK_INTERVAL,
        }
    }
}

pub struct FlightClient {
    addr: String,
    client: FlightServiceClient<Channel>,
}

impl FlightClient {
    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn mut_inner(&mut self) -> &mut FlightServiceClient<Channel> {
        &mut self.client
    }
}

#[derive(Clone, Debug, Default)]
pub struct Client {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    channel_manager: ChannelManager,
    peers: Arc<[String]>,
    peer_states: Arc<RwLock<PeerStates>>,
    load_balance: Loadbalancer,
    health_check_interval: Duration,
    health_check_started: AtomicBool,
}

impl Default for Inner {
    fn default() -> Self {
        Self::with_manager_and_peers(ChannelManager::new(), Vec::new(), ClientOptions::default())
    }
}

#[derive(Debug, Default)]
struct PeerStates {
    active: Vec<usize>,
    inactive: Vec<usize>,
}

impl Inner {
    fn with_manager_and_peers(
        channel_manager: ChannelManager,
        peers: Vec<String>,
        options: ClientOptions,
    ) -> Self {
        let peer_count = peers.len();
        Self {
            channel_manager,
            peers: peers.into(),
            peer_states: Arc::new(RwLock::new(PeerStates {
                active: Vec::new(),
                inactive: (0..peer_count).collect(),
            })),
            load_balance: Loadbalancer::default(),
            health_check_interval: options.health_check_interval,
            health_check_started: AtomicBool::new(false),
        }
    }

    fn get_peer(&self) -> Option<String> {
        let peer_states = self.peer_states.read();
        let index = self
            .load_balance
            .get_index(&peer_states.active)
            .or_else(|| self.load_balance.get_index(&peer_states.inactive))?;
        Some(self.peers[*index].clone())
    }

    async fn refresh_peer_states(&self) {
        let peers = {
            let peer_states = self.peer_states.read();
            peer_states
                .active
                .iter()
                .chain(&peer_states.inactive)
                .copied()
                .collect::<Vec<_>>()
        };
        let health_checks = peers.into_iter().map(|index| async move {
            let is_active = self.check_peer_health(&self.peers[index]).await;
            (index, is_active)
        });
        let results = futures::future::join_all(health_checks).await;

        let (active, inactive) = results.into_iter().fold(
            (Vec::new(), Vec::new()),
            |(mut active, mut inactive), (index, is_active)| {
                if is_active {
                    active.push(index);
                } else {
                    inactive.push(index);
                }
                (active, inactive)
            },
        );
        *self.peer_states.write() = PeerStates { active, inactive };
    }

    async fn check_peer_health(&self, addr: &str) -> bool {
        let Ok(channel) = self.channel_manager.get(addr) else {
            return false;
        };
        let mut client = HealthCheckClient::new(channel);
        client.health_check(HealthCheckRequest {}).await.is_ok()
    }
}

impl Client {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_urls<U, A>(urls: A) -> Self
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        Self::with_urls_and_options(urls, ClientOptions::default())
    }

    /// Creates a client with URLs and custom options.
    pub fn with_urls_and_options<U, A>(urls: A, options: ClientOptions) -> Self
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        Self::with_manager_and_urls_and_options(ChannelManager::new(), urls, options)
    }

    pub fn with_tls_and_urls<U, A>(urls: A, client_tls: ClientTlsOption) -> Result<Self>
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        Self::with_tls_and_urls_and_options(urls, client_tls, ClientOptions::default())
    }

    /// Creates a client with TLS URLs and custom options.
    pub fn with_tls_and_urls_and_options<U, A>(
        urls: A,
        client_tls: ClientTlsOption,
        options: ClientOptions,
    ) -> Result<Self>
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        let channel_config = ChannelConfig::default().client_tls_config(client_tls.clone());
        let tls_config =
            load_client_tls_config(Some(client_tls)).context(error::CreateTlsChannelSnafu)?;
        let channel_manager = ChannelManager::with_config(channel_config, tls_config);
        Ok(Self::with_manager_and_urls_and_options(
            channel_manager,
            urls,
            options,
        ))
    }

    pub fn with_manager_and_urls<U, A>(channel_manager: ChannelManager, urls: A) -> Self
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        Self::with_manager_and_urls_and_options(channel_manager, urls, ClientOptions::default())
    }

    /// Creates a client with a channel manager, URLs, and custom options.
    pub fn with_manager_and_urls_and_options<U, A>(
        channel_manager: ChannelManager,
        urls: A,
        options: ClientOptions,
    ) -> Self
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        let urls: Vec<String> = urls
            .as_ref()
            .iter()
            .map(|peer| peer.as_ref().to_string())
            .collect();
        let client = Self {
            inner: Arc::new(Inner::with_manager_and_peers(
                channel_manager,
                urls,
                options,
            )),
        };
        client.start_health_check();
        client
    }

    fn start_health_check(&self) {
        if self.inner.health_check_interval.is_zero() {
            return;
        }

        if self
            .inner
            .health_check_started
            .swap(true, Ordering::Relaxed)
        {
            return;
        }

        let inner = Arc::downgrade(&self.inner);
        let health_check_interval = self.inner.health_check_interval;
        common_runtime::spawn_global(async move {
            let mut interval = tokio::time::interval(health_check_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;
                let Some(inner) = inner.upgrade() else {
                    return;
                };
                inner.refresh_peer_states().await;
            }
        });
    }

    pub fn find_channel(&self) -> Result<(String, Channel)> {
        let addr = self
            .inner
            .get_peer()
            .context(error::IllegalGrpcClientStateSnafu {
                err_msg: "No available peer found",
            })?;

        let channel = self
            .inner
            .channel_manager
            .get(&addr)
            .context(error::CreateChannelSnafu { addr: &addr })?;
        Ok((addr, channel))
    }

    pub fn max_grpc_recv_message_size(&self) -> usize {
        self.inner
            .channel_manager
            .config()
            .max_recv_message_size
            .as_bytes() as usize
    }

    pub fn max_grpc_send_message_size(&self) -> usize {
        self.inner
            .channel_manager
            .config()
            .max_send_message_size
            .as_bytes() as usize
    }

    pub fn make_flight_client(
        &self,
        send_compression: bool,
        accept_compression: bool,
    ) -> Result<FlightClient> {
        let (addr, channel) = self.find_channel()?;

        let mut client = FlightServiceClient::new(channel)
            .max_decoding_message_size(self.max_grpc_recv_message_size())
            .max_encoding_message_size(self.max_grpc_send_message_size());
        // todo(hl): support compression methods.
        if send_compression {
            client = client.send_compressed(CompressionEncoding::Zstd);
        }
        if accept_compression {
            client = client.accept_compressed(CompressionEncoding::Zstd);
        }

        Ok(FlightClient { addr, client })
    }

    pub(crate) fn raw_region_client(&self) -> Result<(String, PbRegionClient<Channel>)> {
        let (addr, channel) = self.find_channel()?;
        let client = PbRegionClient::new(channel)
            .max_decoding_message_size(self.max_grpc_recv_message_size())
            .max_encoding_message_size(self.max_grpc_send_message_size());
        Ok((addr, client))
    }

    pub(crate) fn raw_flow_client(&self) -> Result<(String, PbFlowClient<Channel>)> {
        let (addr, channel) = self.find_channel()?;
        let client = PbFlowClient::new(channel)
            .max_decoding_message_size(self.max_grpc_recv_message_size())
            .max_encoding_message_size(self.max_grpc_send_message_size())
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Zstd);
        Ok((addr, client))
    }

    pub fn make_prometheus_gateway_client(&self) -> Result<PrometheusGatewayClient<Channel>> {
        let (_, channel) = self.find_channel()?;
        let client = PrometheusGatewayClient::new(channel)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd);
        Ok(client)
    }

    pub async fn health_check(&self) -> Result<()> {
        let (_, channel) = self.find_channel()?;
        let mut client = HealthCheckClient::new(channel);
        let _ = client.health_check(HealthCheckRequest {}).await?;
        Ok(())
    }

    /// Returns peer addresses grouped by active and inactive state for tests.
    #[cfg(feature = "testing")]
    pub fn peer_addresses_by_state(&self) -> (Vec<String>, Vec<String>) {
        let peer_states = self.inner.peer_states.read();
        let addresses = |indices: &[usize]| {
            indices
                .iter()
                .map(|&index| self.inner.peers[index].clone())
                .collect()
        };
        (
            addresses(&peer_states.active),
            addresses(&peer_states.inactive),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use api::v1::health_check_server::{HealthCheck, HealthCheckServer};
    use api::v1::{HealthCheckRequest, HealthCheckResponse};
    use common_grpc::channel_manager::ChannelManager;
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
    use tokio::time::{interval, timeout};
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status};

    use super::{Client, ClientOptions, Inner, PeerStates};
    use crate::load_balance::Loadbalancer;

    const HEALTH_REFRESH_INTERVAL: Duration = Duration::from_millis(10);
    const STATE_REFRESH_TIMEOUT: Duration = Duration::from_secs(1);

    struct HealthyHealthCheck;

    #[tonic::async_trait]
    impl HealthCheck for HealthyHealthCheck {
        async fn health_check(
            &self,
            _request: Request<HealthCheckRequest>,
        ) -> Result<Response<HealthCheckResponse>, Status> {
            Ok(Response::new(HealthCheckResponse {}))
        }
    }

    struct UnhealthyHealthCheck;

    #[tonic::async_trait]
    impl HealthCheck for UnhealthyHealthCheck {
        async fn health_check(
            &self,
            _request: Request<HealthCheckRequest>,
        ) -> Result<Response<HealthCheckResponse>, Status> {
            Err(Status::unavailable("peer is unavailable"))
        }
    }

    async fn start_health_check_server<T>(handler: T) -> (String, JoinHandle<()>)
    where
        T: HealthCheck + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind health check server");
        let addr = listener
            .local_addr()
            .expect("read health check server address")
            .to_string();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(HealthCheckServer::new(handler))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("serve health check server");
        });

        (addr, server)
    }

    async fn wait_for_peer_states(client: &Client, expected: PeerStates) {
        let mut poll = interval(HEALTH_REFRESH_INTERVAL);
        timeout(STATE_REFRESH_TIMEOUT, async {
            loop {
                poll.tick().await;
                let states = client.inner.peer_states.read();
                if states.active == expected.active && states.inactive == expected.inactive {
                    return;
                }
            }
        })
        .await
        .expect("health refresh did not reach expected peer states");
    }

    fn mock_peers() -> Vec<String> {
        vec![
            "127.0.0.1:3001".to_string(),
            "127.0.0.1:3002".to_string(),
            "127.0.0.1:3003".to_string(),
        ]
    }

    #[test]
    fn test_inner() {
        let inner = Inner::default();

        assert!(matches!(
            inner.load_balance,
            Loadbalancer::Random(crate::load_balance::Random)
        ));
        assert!(inner.get_peer().is_none());

        let peers = mock_peers();
        let all: HashSet<String> = peers.iter().cloned().collect();
        let inner =
            Inner::with_manager_and_peers(ChannelManager::new(), peers, ClientOptions::default());

        for _ in 0..20 {
            assert!(all.contains(&inner.get_peer().unwrap()));
        }
    }

    #[test]
    fn test_inner_prefers_active_peer() {
        let peers = mock_peers();
        let inner = Inner::with_manager_and_peers(
            ChannelManager::new(),
            peers.clone(),
            ClientOptions::default(),
        );
        *inner.peer_states.write() = PeerStates {
            active: vec![0],
            inactive: vec![1, 2],
        };

        assert_eq!(Some(peers[0].clone()), inner.get_peer());
    }

    #[test]
    fn test_zero_health_check_interval_disables_background_task() {
        let client = Client::with_urls_and_options(
            mock_peers(),
            ClientOptions {
                health_check_interval: Duration::ZERO,
            },
        );

        assert!(!client.inner.health_check_started.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_health_refresh_marks_unhealthy_peer_inactive_and_selects_healthy_peer() {
        // Arrange: one peer responds to health checks and the other rejects them.
        let (healthy_addr, healthy_server) = start_health_check_server(HealthyHealthCheck).await;
        let (unhealthy_addr, unhealthy_server) =
            start_health_check_server(UnhealthyHealthCheck).await;
        let client = Client::with_urls_and_options(
            [healthy_addr.clone(), unhealthy_addr],
            ClientOptions {
                health_check_interval: HEALTH_REFRESH_INTERVAL,
            },
        );

        // Act: poll with a bounded timeout until the background health refresh completes.
        wait_for_peer_states(
            &client,
            PeerStates {
                active: vec![0],
                inactive: vec![1],
            },
        )
        .await;

        // Assert: an inactive peer does not prevent selection of its active peer.
        assert_eq!(Some(healthy_addr), client.inner.get_peer());

        healthy_server.abort();
        unhealthy_server.abort();
    }
}
