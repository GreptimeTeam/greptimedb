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

//! Send heartbeat from flownode to metasrv

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use api::v1::meta::{HeartbeatRequest, Peer};
use common_error::ext::BoxedError;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_meta::heartbeat::handler::parse_mailbox_message::ParseMailboxMessageHandler;
use common_meta::heartbeat::handler::{
    HandlerGroupExecutor, HeartbeatResponseHandlerContext, HeartbeatResponseHandlerExecutorRef,
};
use common_meta::heartbeat::mailbox::{HeartbeatMailbox, MailboxRef, OutgoingMessage};
use common_meta::heartbeat::utils::outgoing_message_to_mailbox_message;
use common_telemetry::{debug, error, info, warn};
use greptime_proto::v1::meta::NodeInfo;
use meta_client::client::{HeartbeatSender, HeartbeatStream, MetaClient, MetaClientBuilder};
use meta_client::{MetaClientOptions, MetaClientType};
use servers::addrs;
use servers::heartbeat_options::HeartbeatOptions;
use snafu::ResultExt;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};

use crate::error::{ExternalSnafu, MetaClientInitSnafu};
use crate::{Error, FlownodeOptions};

/// The flownode heartbeat task which sending `[HeartbeatRequest]` to Metasrv periodically in background.
#[derive(Clone)]
pub struct HeartbeatTask {
    node_id: u64,
    peer_addr: String,
    meta_client: Arc<MetaClient>,
    report_interval: Duration,
    retry_interval: Duration,
    resp_handler_executor: HeartbeatResponseHandlerExecutorRef,
    start_time_ms: u64,
    running: Arc<AtomicBool>,
}

impl HeartbeatTask {
    pub fn new(
        opts: &FlownodeOptions,
        meta_client: Arc<MetaClient>,
        heartbeat_opts: HeartbeatOptions,
        resp_handler_executor: HeartbeatResponseHandlerExecutorRef,
    ) -> Self {
        Self {
            node_id: opts.node_id.unwrap_or(0),
            peer_addr: addrs::resolve_addr(&opts.grpc.addr, Some(&opts.grpc.hostname)),
            meta_client,
            report_interval: heartbeat_opts.interval,
            retry_interval: heartbeat_opts.retry_interval,
            resp_handler_executor,
            start_time_ms: common_time::util::current_time_millis() as u64,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn start(&self) -> Result<(), Error> {
        if self
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            warn!("Heartbeat task started multiple times");
            return Ok(());
        }
        info!("Start to establish the heartbeat connection to metasrv.");
        let (req_sender, resp_stream) = self
            .meta_client
            .heartbeat()
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

        info!("Flownode's heartbeat connection has been established with metasrv");

        let (outgoing_tx, outgoing_rx) = mpsc::channel(16);
        let mailbox = Arc::new(HeartbeatMailbox::new(outgoing_tx));

        self.start_handle_resp_stream(resp_stream, mailbox);

        self.start_heartbeat_report(req_sender, outgoing_rx);

        Ok(())
    }

    pub fn shutdown(&self) {
        info!("Close heartbeat task for flownode");
        if self
            .running
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            warn!("Call close heartbeat task multiple times");
        }
    }

    fn create_heartbeat_request(
        message: Option<OutgoingMessage>,
        peer: Option<Peer>,
        start_time_ms: u64,
    ) -> Option<HeartbeatRequest> {
        let mailbox_message = match message.map(outgoing_message_to_mailbox_message) {
            Some(Ok(message)) => Some(message),
            Some(Err(e)) => {
                error!(e; "Failed to encode mailbox messages");
                return None;
            }
            None => None,
        };

        Some(HeartbeatRequest {
            mailbox_message,
            peer,
            info: Self::build_node_info(start_time_ms),
            ..Default::default()
        })
    }

    fn build_node_info(start_time_ms: u64) -> Option<NodeInfo> {
        let build_info = common_version::build_info();
        Some(NodeInfo {
            version: build_info.version.to_string(),
            git_commit: build_info.commit_short.to_string(),
            start_time_ms,
        })
    }

    fn start_heartbeat_report(
        &self,
        req_sender: HeartbeatSender,
        mut outgoing_rx: mpsc::Receiver<OutgoingMessage>,
    ) {
        let report_interval = self.report_interval;
        let start_time_ms = self.start_time_ms;
        let self_peer = Some(Peer {
            id: self.node_id,
            addr: self.peer_addr.clone(),
        });

        common_runtime::spawn_hb(async move {
            // note that using interval will cause it to first immediately send
            // a heartbeat
            let mut interval = tokio::time::interval(report_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                let req = tokio::select! {
                    message = outgoing_rx.recv() => {
                        if let Some(message) = message {
                            Self::create_heartbeat_request(Some(message), self_peer.clone(), start_time_ms)
                        } else {
                            // Receives None that means Sender was dropped, we need to break the current loop
                            break
                        }
                    }
                    _ = interval.tick() => {
                        Self::create_heartbeat_request(None, self_peer.clone(), start_time_ms)
                    }
                };

                if let Some(req) = req {
                    if let Err(e) = req_sender.send(req.clone()).await {
                        error!(e; "Failed to send heartbeat to metasrv");
                        break;
                    } else {
                        debug!("Send a heartbeat request to metasrv, content: {:?}", req);
                    }
                }
            }
        });
    }

    fn start_handle_resp_stream(&self, mut resp_stream: HeartbeatStream, mailbox: MailboxRef) {
        let capture_self = self.clone();
        let retry_interval = self.retry_interval;

        let _handle = common_runtime::spawn_hb(async move {
            loop {
                match resp_stream.message().await {
                    Ok(Some(resp)) => {
                        debug!("Receiving heartbeat response: {:?}", resp);
                        let ctx = HeartbeatResponseHandlerContext::new(mailbox.clone(), resp);
                        if let Err(e) = capture_self.handle_response(ctx).await {
                            error!(e; "Error while handling heartbeat response");
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        error!(e; "Occur error while reading heartbeat response");
                        capture_self.start_with_retry(retry_interval).await;

                        break;
                    }
                }
            }
        });
    }

    async fn handle_response(&self, ctx: HeartbeatResponseHandlerContext) -> Result<(), Error> {
        self.resp_handler_executor
            .handle(ctx)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }

    async fn start_with_retry(&self, retry_interval: Duration) {
        loop {
            tokio::time::sleep(retry_interval).await;

            info!("Try to re-establish the heartbeat connection to metasrv.");

            if self.start().await.is_ok() {
                break;
            }
        }
    }
}
