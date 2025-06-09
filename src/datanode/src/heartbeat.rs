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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use api::v1::meta::heartbeat_request::NodeWorkloads;
use api::v1::meta::{DatanodeWorkloads, HeartbeatRequest, NodeInfo, Peer, RegionRole, RegionStat};
use common_base::Plugins;
use common_meta::cache_invalidator::CacheInvalidatorRef;
use common_meta::datanode::REGION_STATISTIC_KEY;
use common_meta::distributed_time_constants::META_KEEP_ALIVE_INTERVAL_SECS;
use common_meta::heartbeat::handler::invalidate_table_cache::InvalidateCacheHandler;
use common_meta::heartbeat::handler::parse_mailbox_message::ParseMailboxMessageHandler;
use common_meta::heartbeat::handler::{
    HandlerGroupExecutor, HeartbeatResponseHandlerContext, HeartbeatResponseHandlerExecutorRef,
};
use common_meta::heartbeat::mailbox::{HeartbeatMailbox, MailboxRef};
use common_meta::heartbeat::utils::outgoing_message_to_mailbox_message;
use common_telemetry::{debug, error, info, trace, warn};
use common_workload::DatanodeWorkloadType;
use meta_client::client::{HeartbeatSender, MetaClient};
use meta_client::MetaClientRef;
use servers::addrs;
use snafu::ResultExt;
use tokio::sync::{mpsc, Notify};
use tokio::time::Instant;

use self::handler::RegionHeartbeatResponseHandler;
use crate::alive_keeper::{CountdownTaskHandlerExtRef, RegionAliveKeeper};
use crate::config::DatanodeOptions;
use crate::error::{self, MetaClientInitSnafu, Result};
use crate::event_listener::RegionServerEventReceiver;
use crate::metrics::{self, HEARTBEAT_RECV_COUNT, HEARTBEAT_SENT_COUNT};
use crate::region_server::RegionServer;

pub(crate) mod handler;
pub(crate) mod task_tracker;

/// The datanode heartbeat task which sending `[HeartbeatRequest]` to Metasrv periodically in background.
pub struct HeartbeatTask {
    node_id: u64,
    workload_types: Vec<DatanodeWorkloadType>,
    node_epoch: u64,
    peer_addr: String,
    running: Arc<AtomicBool>,
    meta_client: MetaClientRef,
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
        meta_client: MetaClientRef,
        cache_invalidator: CacheInvalidatorRef,
        plugins: Plugins,
    ) -> Result<Self> {
        let countdown_task_handler_ext = plugins.get::<CountdownTaskHandlerExtRef>();
        let region_alive_keeper = Arc::new(RegionAliveKeeper::new(
            region_server.clone(),
            countdown_task_handler_ext,
            opts.heartbeat.interval.as_millis() as u64,
        ));
        let resp_handler_executor = Arc::new(HandlerGroupExecutor::new(vec![
            region_alive_keeper.clone(),
            Arc::new(ParseMailboxMessageHandler),
            Arc::new(RegionHeartbeatResponseHandler::new(region_server.clone())),
            Arc::new(InvalidateCacheHandler::new(cache_invalidator)),
        ]));

        Ok(Self {
            node_id: opts.node_id.unwrap_or(0),
            workload_types: opts.workload_types.clone(),
            // We use datanode's start time millis as the node's epoch.
            node_epoch: common_time::util::current_time_millis() as u64,
            peer_addr: addrs::resolve_addr(&opts.grpc.bind_addr, Some(&opts.grpc.server_addr)),
            running: Arc::new(AtomicBool::new(false)),
            meta_client,
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

        let _handle = common_runtime::spawn_hb(async move {
            while let Some(res) = rx.message().await.unwrap_or_else(|e| {
                error!(e; "Error while reading heartbeat response");
                None
            }) {
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
                            RegionRole::Leader | RegionRole::DowngradingLeader => {
                                leader_region_lease_count += 1
                            }
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
        trace!("Heartbeat response: {:?}", ctx.response);
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
        let addr = &self.peer_addr;
        info!("Starting heartbeat to Metasrv with interval {interval}. My node id is {node_id}, address is {addr}.");

        let meta_client = self.meta_client.clone();
        let region_server_clone = self.region_server.clone();

        let handler_executor = self.resp_handler_executor.clone();

        let (outgoing_tx, mut outgoing_rx) = mpsc::channel(16);
        let mailbox = Arc::new(HeartbeatMailbox::new(outgoing_tx));

        let quit_signal = Arc::new(Notify::new());

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
        let workload_types = self.workload_types.clone();

        self.region_alive_keeper.start(Some(event_receiver)).await?;
        let mut last_sent = Instant::now();

        common_runtime::spawn_hb(async move {
            let sleep = tokio::time::sleep(Duration::from_millis(0));
            tokio::pin!(sleep);

            let build_info = common_version::build_info();
            let heartbeat_request = HeartbeatRequest {
                peer: self_peer,
                node_epoch,
                info: Some(NodeInfo {
                    version: build_info.version.to_string(),
                    git_commit: build_info.commit_short.to_string(),
                    start_time_ms: node_epoch,
                    cpus: num_cpus::get() as u32,
                }),
                node_workloads: Some(NodeWorkloads::Datanode(DatanodeWorkloads {
                    types: workload_types.iter().map(|w| w.to_i32()).collect(),
                })),
                ..Default::default()
            };

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
                                        mailbox_message: Some(message),
                                        ..heartbeat_request.clone()
                                    };
                                    HEARTBEAT_RECV_COUNT.with_label_values(&["success"]).inc();
                                    Some(req)
                                }
                                Err(e) => {
                                    error!(e; "Failed to encode mailbox messages!");
                                    HEARTBEAT_RECV_COUNT.with_label_values(&["error"]).inc();
                                    None
                                }
                            }
                        } else {
                            None
                        }
                    }
                    _ = &mut sleep => {
                        let region_stats = Self::load_region_stats(&region_server_clone);
                        let now = Instant::now();
                        let duration_since_epoch = (now - epoch).as_millis() as u64;
                        let req = HeartbeatRequest {
                            region_stats,
                            duration_since_epoch,
                            ..heartbeat_request.clone()
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
                    metrics::LAST_SENT_HEARTBEAT_ELAPSED
                        .set(last_sent.elapsed().as_millis() as i64);
                    // Resets the timer.
                    last_sent = Instant::now();
                    debug!("Sending heartbeat request: {:?}", req);
                    if let Err(e) = tx.send(req).await {
                        error!(e; "Failed to send heartbeat to metasrv");
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
                                error!(e; "Failed to reconnect to metasrv!");
                            }
                        }
                    } else {
                        HEARTBEAT_SENT_COUNT.inc();
                    }
                }
            }
        });

        Ok(())
    }

    fn load_region_stats(region_server: &RegionServer) -> Vec<RegionStat> {
        region_server
            .reportable_regions()
            .into_iter()
            .map(|stat| {
                let region_stat = region_server
                    .region_statistic(stat.region_id)
                    .unwrap_or_default();
                let mut extensions = HashMap::new();
                if let Some(serialized) = region_stat.serialize_to_vec() {
                    extensions.insert(REGION_STATISTIC_KEY.to_string(), serialized);
                }

                RegionStat {
                    region_id: stat.region_id.as_u64(),
                    engine: stat.engine,
                    role: RegionRole::from(stat.role).into(),
                    // TODO(weny): w/rcus
                    rcus: 0,
                    wcus: 0,
                    approximate_bytes: region_stat.estimated_disk_size() as i64,
                    extensions,
                }
            })
            .collect()
    }

    pub fn close(&self) -> Result<()> {
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
