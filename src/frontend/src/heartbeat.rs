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

#[cfg(test)]
mod tests;

use std::sync::Arc;

use api::v1::meta::{HeartbeatRequest, NodeInfo, Peer};
use common_meta::heartbeat::handler::{
    HeartbeatResponseHandlerContext, HeartbeatResponseHandlerExecutorRef,
};
use common_meta::heartbeat::mailbox::{HeartbeatMailbox, MailboxRef, OutgoingMessage};
use common_meta::heartbeat::utils::outgoing_message_to_mailbox_message;
use common_stat::ResourceStatRef;
use common_telemetry::{debug, error, info, warn};
use meta_client::client::{HeartbeatSender, HeartbeatStream, MetaClient};
use servers::addrs;
use servers::heartbeat_options::HeartbeatOptions;
use snafu::ResultExt;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::time::{Duration, Instant};

use crate::error;
use crate::error::Result;
use crate::frontend::FrontendOptions;
use crate::metrics::{HEARTBEAT_RECV_COUNT, HEARTBEAT_SENT_COUNT};

/// The frontend heartbeat task which sending `[HeartbeatRequest]` to Metasrv periodically in background.
#[derive(Clone)]
pub struct HeartbeatTask {
    peer_addr: String,
    meta_client: Arc<MetaClient>,
    report_interval: Duration,
    retry_interval: Duration,
    resp_handler_executor: HeartbeatResponseHandlerExecutorRef,
    start_time_ms: u64,
    resource_stat: ResourceStatRef,
}

impl HeartbeatTask {
    pub fn new(
        opts: &FrontendOptions,
        meta_client: Arc<MetaClient>,
        heartbeat_opts: HeartbeatOptions,
        resp_handler_executor: HeartbeatResponseHandlerExecutorRef,
        resource_stat: ResourceStatRef,
    ) -> Self {
        HeartbeatTask {
            // if internal grpc is configured, use its address as the peer address
            // otherwise use the public grpc address, because peer address only promises to be reachable
            // by other components, it doesn't matter whether it's internal or external
            peer_addr: if let Some(internal) = &opts.internal_grpc {
                addrs::resolve_addr(&internal.bind_addr, Some(&internal.server_addr))
            } else {
                addrs::resolve_addr(&opts.grpc.bind_addr, Some(&opts.grpc.server_addr))
            },
            meta_client,
            report_interval: heartbeat_opts.interval,
            retry_interval: heartbeat_opts.retry_interval,
            resp_handler_executor,
            start_time_ms: common_time::util::current_time_millis() as u64,
            resource_stat,
        }
    }

    pub async fn start(&self) -> Result<()> {
        let (req_sender, resp_stream) = self
            .meta_client
            .heartbeat()
            .await
            .context(error::CreateMetaHeartbeatStreamSnafu)?;

        info!("A heartbeat connection has been established with metasrv");

        let (outgoing_tx, outgoing_rx) = mpsc::channel(16);
        let mailbox = Arc::new(HeartbeatMailbox::new(outgoing_tx));

        self.start_handle_resp_stream(resp_stream, mailbox);

        self.start_heartbeat_report(req_sender, outgoing_rx);

        Ok(())
    }

    fn start_handle_resp_stream(&self, mut resp_stream: HeartbeatStream, mailbox: MailboxRef) {
        let capture_self = self.clone();
        let retry_interval = self.retry_interval;

        let _handle = common_runtime::spawn_hb(async move {
            loop {
                match resp_stream.message().await {
                    Ok(Some(resp)) => {
                        debug!("Receiving heartbeat response: {:?}", resp);
                        if let Some(message) = &resp.mailbox_message {
                            info!("Received mailbox message: {message:?}");
                        }
                        let ctx = HeartbeatResponseHandlerContext::new(mailbox.clone(), resp);
                        if let Err(e) = capture_self.handle_response(ctx).await {
                            error!(e; "Error while handling heartbeat response");
                            HEARTBEAT_RECV_COUNT
                                .with_label_values(&["processing_error"])
                                .inc();
                        } else {
                            HEARTBEAT_RECV_COUNT.with_label_values(&["success"]).inc();
                        }
                    }
                    Ok(None) => {
                        warn!("Heartbeat response stream closed");
                        capture_self.start_with_retry(retry_interval).await;
                        break;
                    }
                    Err(e) => {
                        HEARTBEAT_RECV_COUNT.with_label_values(&["error"]).inc();
                        error!(e; "Occur error while reading heartbeat response");
                        capture_self.start_with_retry(retry_interval).await;

                        break;
                    }
                }
            }
        });
    }

    fn new_heartbeat_request(
        heartbeat_request: &HeartbeatRequest,
        message: Option<OutgoingMessage>,
        cpu_usage: i64,
        memory_usage: i64,
    ) -> Option<HeartbeatRequest> {
        let mailbox_message = match message.map(outgoing_message_to_mailbox_message) {
            Some(Ok(message)) => Some(message),
            Some(Err(e)) => {
                error!(e; "Failed to encode mailbox messages");
                return None;
            }
            None => None,
        };

        let mut heartbeat_request = HeartbeatRequest {
            mailbox_message,
            ..heartbeat_request.clone()
        };

        if let Some(info) = heartbeat_request.info.as_mut() {
            info.memory_usage_bytes = memory_usage;
            info.cpu_usage_millicores = cpu_usage;
        }

        Some(heartbeat_request)
    }

    #[allow(deprecated)]
    fn build_node_info(
        start_time_ms: u64,
        total_cpu_millicores: i64,
        total_memory_bytes: i64,
    ) -> Option<NodeInfo> {
        let build_info = common_version::build_info();

        Some(NodeInfo {
            version: build_info.version.to_string(),
            git_commit: build_info.commit_short.to_string(),
            start_time_ms,
            total_cpu_millicores,
            total_memory_bytes,
            cpu_usage_millicores: 0,
            memory_usage_bytes: 0,
            // TODO(zyy17): Remove these deprecated fields when the deprecated fields are removed from the proto.
            cpus: total_cpu_millicores as u32,
            memory_bytes: total_memory_bytes as u64,
            hostname: hostname::get()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
        })
    }

    fn start_heartbeat_report(
        &self,
        req_sender: HeartbeatSender,
        mut outgoing_rx: Receiver<OutgoingMessage>,
    ) {
        let report_interval = self.report_interval;
        let start_time_ms = self.start_time_ms;
        let self_peer = Some(Peer {
            // The peer id doesn't make sense for frontend, so we just set it 0.
            id: 0,
            addr: self.peer_addr.clone(),
        });
        let total_cpu_millicores = self.resource_stat.get_total_cpu_millicores();
        let total_memory_bytes = self.resource_stat.get_total_memory_bytes();
        let resource_stat = self.resource_stat.clone();
        common_runtime::spawn_hb(async move {
            let sleep = tokio::time::sleep(Duration::from_millis(0));
            tokio::pin!(sleep);

            let heartbeat_request = HeartbeatRequest {
                peer: self_peer,
                info: Self::build_node_info(
                    start_time_ms,
                    total_cpu_millicores,
                    total_memory_bytes,
                ),
                ..Default::default()
            };

            loop {
                let req = tokio::select! {
                    message = outgoing_rx.recv() => {
                        if let Some(message) = message {
                            Self::new_heartbeat_request(&heartbeat_request, Some(message), 0, 0)
                        } else {
                            warn!("Sender has been dropped, exiting the heartbeat loop");
                            // Receives None that means Sender was dropped, we need to break the current loop
                            break
                        }
                    }
                    _ = &mut sleep => {
                       sleep.as_mut().reset(Instant::now() + report_interval);
                       Self::new_heartbeat_request(&heartbeat_request, None, resource_stat.get_cpu_usage_millicores(), resource_stat.get_memory_usage_bytes())
                    }
                };

                if let Some(req) = req {
                    if let Err(e) = req_sender.send(req.clone()).await {
                        error!(e; "Failed to send heartbeat to metasrv");
                        break;
                    } else {
                        HEARTBEAT_SENT_COUNT.inc();
                        debug!("Send a heartbeat request to metasrv, content: {:?}", req);
                    }
                }
            }
        });
    }

    async fn handle_response(&self, ctx: HeartbeatResponseHandlerContext) -> Result<()> {
        self.resp_handler_executor
            .handle(ctx)
            .await
            .context(error::HandleHeartbeatResponseSnafu)
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
