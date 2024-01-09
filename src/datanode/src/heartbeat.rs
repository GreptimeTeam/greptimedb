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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use api::v1::meta::{HeartbeatRequest, Peer, RegionRole, RegionStat, Role};
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_meta::distributed_time_constants::META_KEEP_ALIVE_INTERVAL_SECS;
use common_meta::heartbeat::handler::parse_mailbox_message::ParseMailboxMessageHandler;
use common_meta::heartbeat::handler::{
    HandlerGroupExecutor, HeartbeatResponseHandlerContext, HeartbeatResponseHandlerExecutorRef,
};
use common_meta::heartbeat::mailbox::{HeartbeatMailbox, MailboxRef};
use common_meta::heartbeat::utils::outgoing_message_to_mailbox_message;
use common_telemetry::{debug, error, info, trace, warn};
use meta_client::client::{HeartbeatSender, MetaClient, MetaClientBuilder};
use meta_client::MetaClientOptions;
use snafu::ResultExt;
use tokio::sync::{mpsc, Notify};
use tokio::time::Instant;

use self::handler::RegionHeartbeatResponseHandler;
use crate::alive_keeper::RegionAliveKeeper;
use crate::config::DatanodeOptions;
use crate::error::{self, MetaClientInitSnafu, Result};
use crate::event_listener::RegionServerEventReceiver;
use crate::metrics;
use crate::region_server::RegionServer;

pub(crate) mod handler;
pub(crate) mod task_tracker;

pub struct HeartbeatTask {
    node_id: u64,
    node_epoch: u64,
    server_addr: String,
    server_hostname: Option<String>,
    running: Arc<AtomicBool>,
    meta_client: Arc<MetaClient>,
    region_server: RegionServer,
    interval: u64,
    resp_handler_executor: HeartbeatResponseHandlerExecutorRef,
    region_alive_keeper: Arc<RegionAliveKeeper>,
}

impl Drop for HeartbeatTask {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
    }
}

impl HeartbeatTask {
    /// Create a new heartbeat task instance.
    pub async fn try_new(
        opts: &DatanodeOptions,
        region_server: RegionServer,
        meta_client: MetaClient,
    ) -> Result<Self> {
        let region_alive_keeper = Arc::new(RegionAliveKeeper::new(
            region_server.clone(),
            opts.heartbeat.interval.as_millis() as u64,
        ));
        let resp_handler_executor = Arc::new(HandlerGroupExecutor::new(vec![
            region_alive_keeper.clone(),
            Arc::new(ParseMailboxMessageHandler),
            Arc::new(RegionHeartbeatResponseHandler::new(region_server.clone())),
        ]));

        Ok(Self {
            node_id: opts.node_id.unwrap_or(0),
            // We use datanode's start time millis as the node's epoch.
            node_epoch: common_time::util::current_time_millis() as u64,
            server_addr: opts.rpc_addr.clone(),
            server_hostname: opts.rpc_hostname.clone(),
            running: Arc::new(AtomicBool::new(false)),
            meta_client: Arc::new(meta_client),
            region_server,
            interval: opts.heartbeat.interval.as_millis() as u64,
            resp_handler_executor,
            region_alive_keeper,
        })
    }

    pub async fn create_streams(
        meta_client: &MetaClient,
        running: Arc<AtomicBool>,
        handler_executor: HeartbeatResponseHandlerExecutorRef,
        mailbox: MailboxRef,
        mut notify: Option<Arc<Notify>>,
        quit_signal: Arc<Notify>,
    ) -> Result<HeartbeatSender> {
        let client_id = meta_client.id();
        let (tx, mut rx) = meta_client.heartbeat().await.context(MetaClientInitSnafu)?;

        let mut last_received_lease = Instant::now();

        let _handle = common_runtime::spawn_bg(async move {
            while let Some(res) = match rx.message().await {
                Ok(m) => m,
                Err(e) => {
                    error!(e; "Error while reading heartbeat response");
                    None
                }
            } {
                if let Some(msg) = res.mailbox_message.as_ref() {
                    info!("Received mailbox message: {msg:?}, meta_client id: {client_id:?}");
                }
                if let Some(lease) = res.region_lease.as_ref() {
                    metrics::LAST_RECEIVED_HEARTBEAT_ELAPSED
                        .set(last_received_lease.elapsed().as_millis() as i64);
                    // Resets the timer.
                    last_received_lease = Instant::now();

                    let mut leader_region_lease_count = 0;
                    let mut follower_region_lease_count = 0;
                    for lease in &lease.regions {
                        match lease.role() {
                            RegionRole::Leader => leader_region_lease_count += 1,
                            RegionRole::Follower => follower_region_lease_count += 1,
                        }
                    }

                    metrics::HEARTBEAT_REGION_LEASES
                        .with_label_values(&["leader"])
                        .set(leader_region_lease_count);
                    metrics::HEARTBEAT_REGION_LEASES
                        .with_label_values(&["follower"])
                        .set(follower_region_lease_count);
                }
                let ctx = HeartbeatResponseHandlerContext::new(mailbox.clone(), res);
                if let Err(e) = Self::handle_response(ctx, handler_executor.clone()).await {
                    error!(e; "Error while handling heartbeat response");
                }
                if let Some(notify) = notify.take() {
                    notify.notify_one();
                }
                if !running.load(Ordering::Acquire) {
                    info!("Heartbeat task shutdown");
                }
            }
            quit_signal.notify_one();
            info!("Heartbeat handling loop exit.");
        });
        Ok(tx)
    }

    async fn handle_response(
        ctx: HeartbeatResponseHandlerContext,
        handler_executor: HeartbeatResponseHandlerExecutorRef,
    ) -> Result<()> {
        trace!("heartbeat response: {:?}", ctx.response);
        handler_executor
            .handle(ctx)
            .await
            .context(error::HandleHeartbeatResponseSnafu)
    }

    /// Start heartbeat task, spawn background task.
    pub async fn start(
        &self,
        event_receiver: RegionServerEventReceiver,
        notify: Option<Arc<Notify>>,
    ) -> Result<()> {
        let running = self.running.clone();
        if running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            warn!("Heartbeat task started multiple times");
            return Ok(());
        }
        let interval = self.interval;
        let node_id = self.node_id;
        let node_epoch = self.node_epoch;
        let addr = resolve_addr(&self.server_addr, &self.server_hostname);
        info!("Starting heartbeat to Metasrv with interval {interval}. My node id is {node_id}, address is {addr}.");

        let meta_client = self.meta_client.clone();
        let region_server_clone = self.region_server.clone();

        let handler_executor = self.resp_handler_executor.clone();

        let (outgoing_tx, mut outgoing_rx) = mpsc::channel(16);
        let mailbox = Arc::new(HeartbeatMailbox::new(outgoing_tx));

        let quit_signal = Arc::new(tokio::sync::Notify::new());

        let mut tx = Self::create_streams(
            &meta_client,
            running.clone(),
            handler_executor.clone(),
            mailbox.clone(),
            notify,
            quit_signal.clone(),
        )
        .await?;

        let self_peer = Some(Peer {
            id: node_id,
            addr: addr.clone(),
        });
        let epoch = self.region_alive_keeper.epoch();

        self.region_alive_keeper.start(Some(event_receiver)).await?;

        common_runtime::spawn_bg(async move {
            let sleep = tokio::time::sleep(Duration::from_millis(0));
            tokio::pin!(sleep);
            loop {
                if !running.load(Ordering::Relaxed) {
                    info!("shutdown heartbeat task");
                    break;
                }
                let req = tokio::select! {
                    message = outgoing_rx.recv() => {
                        if let Some(message) = message {
                            match outgoing_message_to_mailbox_message(message) {
                                Ok(message) => {
                                    let req = HeartbeatRequest {
                                        peer: self_peer.clone(),
                                        mailbox_message: Some(message),
                                        ..Default::default()
                                    };
                                    Some(req)
                                }
                                Err(e) => {
                                    error!(e;"Failed to encode mailbox messages!");
                                    None
                                }
                            }
                        } else {
                            None
                        }
                    }
                    _ = &mut sleep => {
                        let region_stats = Self::load_region_stats(&region_server_clone).await;
                        let now = Instant::now();
                        let duration_since_epoch = (now - epoch).as_millis() as u64;
                        let req = HeartbeatRequest {
                            peer: self_peer.clone(),
                            region_stats,
                            duration_since_epoch,
                            node_epoch,
                            ..Default::default()
                        };
                        sleep.as_mut().reset(now + Duration::from_millis(interval));
                        Some(req)
                    }
                    // If the heartbeat stream is broken, send a dummy heartbeat request to re-create the heartbeat stream.
                    _ = quit_signal.notified() => {
                        let req = HeartbeatRequest::default();
                        Some(req)
                    }
                };
                if let Some(req) = req {
                    debug!("Sending heartbeat request: {:?}", req);
                    if let Err(e) = tx.send(req).await {
                        error!("Failed to send heartbeat to metasrv, error: {:?}", e);
                        match Self::create_streams(
                            &meta_client,
                            running.clone(),
                            handler_executor.clone(),
                            mailbox.clone(),
                            None,
                            quit_signal.clone(),
                        )
                        .await
                        {
                            Ok(new_tx) => {
                                info!("Reconnected to metasrv");
                                tx = new_tx;
                                // Triggers to send heartbeat immediately.
                                sleep.as_mut().reset(Instant::now());
                            }
                            Err(e) => {
                                // Before the META_LEASE_SECS expires,
                                // any retries are meaningless, it always reads the old meta leader address.
                                // Triggers to retry after META_KEEP_ALIVE_INTERVAL_SECS.
                                sleep.as_mut().reset(
                                    Instant::now()
                                        + Duration::from_secs(META_KEEP_ALIVE_INTERVAL_SECS),
                                );
                                error!(e;"Failed to reconnect to metasrv!");
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    async fn load_region_stats(region_server: &RegionServer) -> Vec<RegionStat> {
        let regions = region_server.reportable_regions();

        let mut region_stats = Vec::new();
        for stat in regions {
            let approximate_bytes = region_server
                .region_disk_usage(stat.region_id)
                .await
                .unwrap_or(0);
            let region_stat = RegionStat {
                region_id: stat.region_id.as_u64(),
                engine: stat.engine,
                role: RegionRole::from(stat.role).into(),
                approximate_bytes,
                // TODO(ruihang): scratch more info
                rcus: 0,
                wcus: 0,
                approximate_rows: 0,
            };
            region_stats.push(region_stat);
        }
        region_stats
    }

    pub async fn close(&self) -> Result<()> {
        let running = self.running.clone();
        if running
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            warn!("Call close heartbeat task multiple times");
        }

        Ok(())
    }
}

/// Resolves hostname:port address for meta registration
///
fn resolve_addr(bind_addr: &str, hostname_addr: &Option<String>) -> String {
    match hostname_addr {
        Some(hostname_addr) => {
            // it has port configured
            if hostname_addr.contains(':') {
                hostname_addr.clone()
            } else {
                // otherwise, resolve port from bind_addr
                // should be safe to unwrap here because bind_addr is already validated
                let port = bind_addr.split(':').nth(1).unwrap();
                format!("{hostname_addr}:{port}")
            }
        }
        None => bind_addr.to_owned(),
    }
}

/// Create metasrv client instance and spawn heartbeat loop.
pub async fn new_metasrv_client(
    node_id: u64,
    meta_config: &MetaClientOptions,
) -> Result<MetaClient> {
    let cluster_id = 0; // TODO(hl): read from config
    let member_id = node_id;

    let config = ChannelConfig::new()
        .timeout(meta_config.timeout)
        .connect_timeout(meta_config.connect_timeout)
        .tcp_nodelay(meta_config.tcp_nodelay);
    let channel_manager = ChannelManager::with_config(config.clone());
    let heartbeat_channel_manager = ChannelManager::with_config(
        config
            .timeout(meta_config.timeout)
            .connect_timeout(meta_config.connect_timeout),
    );

    let mut meta_client = MetaClientBuilder::new(cluster_id, member_id, Role::Datanode)
        .enable_heartbeat()
        .enable_router()
        .enable_store()
        .channel_manager(channel_manager)
        .heartbeat_channel_manager(heartbeat_channel_manager)
        .build();
    meta_client
        .start(&meta_config.metasrv_addrs)
        .await
        .context(MetaClientInitSnafu)?;

    // required only when the heartbeat_client is enabled
    meta_client
        .ask_leader()
        .await
        .context(MetaClientInitSnafu)?;
    Ok(meta_client)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_resolve_addr() {
        assert_eq!(
            "tomcat:3001",
            super::resolve_addr("127.0.0.1:3001", &Some("tomcat".to_owned()))
        );

        assert_eq!(
            "tomcat:3002",
            super::resolve_addr("127.0.0.1:3001", &Some("tomcat:3002".to_owned()))
        );

        assert_eq!(
            "127.0.0.1:3001",
            super::resolve_addr("127.0.0.1:3001", &None)
        );
    }
}
