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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use api::v1::meta::{HeartbeatRequest, Peer};
use common_error::ext::BoxedError;
use common_meta::heartbeat::handler::{
    HeartbeatResponseHandlerContext, HeartbeatResponseHandlerExecutorRef,
};
use common_meta::heartbeat::mailbox::{HeartbeatMailbox, MailboxRef, OutgoingMessage};
use common_meta::heartbeat::utils::outgoing_message_to_mailbox_message;
use common_meta::key::flow::flow_state::FlowStat;
use common_stat::ResourceStatRef;
use common_telemetry::{debug, error, info, warn};
use greptime_proto::v1::meta::NodeInfo;
use meta_client::client::{HeartbeatSender, HeartbeatStream, MetaClient};
use servers::addrs;
use servers::heartbeat_options::HeartbeatOptions;
use snafu::ResultExt;
use tokio::sync::mpsc;
use tokio::time::Duration;

use crate::error::ExternalSnafu;
use crate::utils::SizeReportSender;
use crate::{Error, FlownodeOptions};

async fn query_flow_state(
    query_stat_size: &Option<SizeReportSender>,
    timeout: Duration,
) -> Option<FlowStat> {
    if let Some(report_requester) = query_stat_size.as_ref() {
        let ret = report_requester.query(timeout).await;
        match ret {
            Ok(latest) => Some(latest),
            Err(err) => {
                error!(err; "Failed to get query stat size");
                None
            }
        }
    } else {
        None
    }
}

/// The flownode heartbeat task which sending `[HeartbeatRequest]` to Metasrv periodically in background.
#[derive(Clone)]
pub struct HeartbeatTask {
    node_id: u64,
    node_epoch: u64,
    peer_addr: String,
    meta_client: Arc<MetaClient>,
    report_interval: Duration,
    retry_interval: Duration,
    resp_handler_executor: HeartbeatResponseHandlerExecutorRef,
    running: Arc<AtomicBool>,
    query_stat_size: Option<SizeReportSender>,
    resource_stat: ResourceStatRef,
}

impl HeartbeatTask {
    pub fn with_query_stat_size(mut self, query_stat_size: SizeReportSender) -> Self {
        self.query_stat_size = Some(query_stat_size);
        self
    }

    pub fn new(
        opts: &FlownodeOptions,
        meta_client: Arc<MetaClient>,
        heartbeat_opts: HeartbeatOptions,
        resp_handler_executor: HeartbeatResponseHandlerExecutorRef,
        resource_stat: ResourceStatRef,
    ) -> Self {
        Self {
            node_id: opts.node_id.unwrap_or(0),
            node_epoch: common_time::util::current_time_millis() as u64,
            peer_addr: addrs::resolve_addr(&opts.grpc.bind_addr, Some(&opts.grpc.server_addr)),
            meta_client,
            report_interval: heartbeat_opts.interval,
            retry_interval: heartbeat_opts.retry_interval,
            resp_handler_executor,
            running: Arc::new(AtomicBool::new(false)),
            query_stat_size: None,
            resource_stat,
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

        self.create_streams().await
    }

    async fn create_streams(&self) -> Result<(), Error> {
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

    fn new_heartbeat_request(
        heartbeat_request: &HeartbeatRequest,
        message: Option<OutgoingMessage>,
        latest_report: &Option<FlowStat>,
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
        let flow_stat = latest_report
            .as_ref()
            .map(|report| api::v1::meta::FlowStat {
                flow_stat_size: report
                    .state_size
                    .iter()
                    .map(|(k, v)| (*k, *v as u64))
                    .collect(),
                flow_last_exec_time_map: report
                    .last_exec_time_map
                    .iter()
                    .map(|(k, v)| (*k, *v))
                    .collect(),
            });

        let mut heartbeat_request = HeartbeatRequest {
            mailbox_message,
            flow_stat,
            ..heartbeat_request.clone()
        };

        if let Some(info) = heartbeat_request.info.as_mut() {
            info.cpu_usage_millicores = cpu_usage;
            info.memory_usage_bytes = memory_usage;
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
        mut outgoing_rx: mpsc::Receiver<OutgoingMessage>,
    ) {
        let report_interval = self.report_interval;
        let node_epoch = self.node_epoch;
        let self_peer = Some(Peer {
            id: self.node_id,
            addr: self.peer_addr.clone(),
        });
        let total_cpu_millicores = self.resource_stat.get_total_cpu_millicores();
        let total_memory_bytes = self.resource_stat.get_total_memory_bytes();
        let resource_stat = self.resource_stat.clone();
        let query_stat_size = self.query_stat_size.clone();

        common_runtime::spawn_hb(async move {
            // note that using interval will cause it to first immediately send
            // a heartbeat
            let mut interval = tokio::time::interval(report_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut latest_report = None;

            let heartbeat_request = HeartbeatRequest {
                peer: self_peer,
                node_epoch,
                info: Self::build_node_info(node_epoch, total_cpu_millicores, total_memory_bytes),
                ..Default::default()
            };

            loop {
                let req = tokio::select! {
                    message = outgoing_rx.recv() => {
                        if let Some(message) = message {
                            Self::new_heartbeat_request(&heartbeat_request, Some(message), &latest_report, 0, 0)
                        } else {
                            warn!("Sender has been dropped, exiting the heartbeat loop");
                            // Receives None that means Sender was dropped, we need to break the current loop
                            break
                        }
                    }
                    _ = interval.tick() => {
                        Self::new_heartbeat_request(&heartbeat_request, None, &latest_report, resource_stat.get_cpu_usage_millicores(), resource_stat.get_memory_usage_bytes())
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
                // after sending heartbeat, try to get the latest report
                // TODO(discord9): consider a better place to update the size report
                // set the timeout to half of the report interval so that it wouldn't delay heartbeat if something went horribly wrong
                latest_report = query_flow_state(&query_stat_size, report_interval / 2).await;
            }

            info!("flownode heartbeat task stopped.");
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
                    Ok(None) => {
                        warn!("Heartbeat response stream closed");
                        capture_self.start_with_retry(retry_interval).await;
                        break;
                    }
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

            if self.create_streams().await.is_ok() {
                break;
            }
        }
    }
}
