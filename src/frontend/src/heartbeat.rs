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

use api::v1::meta::HeartbeatRequest;
use common_meta::heartbeat::handler::{
    HeartbeatResponseHandlerContext, HeartbeatResponseHandlerExecutorRef,
};
use common_meta::heartbeat::mailbox::{HeartbeatMailbox, MailboxRef, OutgoingMessage};
use common_meta::heartbeat::utils::outgoing_message_to_mailbox_message;
use common_telemetry::{debug, error, info};
use meta_client::client::{HeartbeatSender, HeartbeatStream, MetaClient};
use servers::heartbeat_options::HeartbeatOptions;
use snafu::ResultExt;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::time::{Duration, Instant};

use crate::error;
use crate::error::Result;

pub mod handler;

#[derive(Clone)]
pub struct HeartbeatTask {
    meta_client: Arc<MetaClient>,
    report_interval: u64,
    retry_interval: u64,
    resp_handler_executor: HeartbeatResponseHandlerExecutorRef,
}

impl HeartbeatTask {
    pub fn new(
        meta_client: Arc<MetaClient>,
        heartbeat_opts: HeartbeatOptions,
        resp_handler_executor: HeartbeatResponseHandlerExecutorRef,
    ) -> Self {
        HeartbeatTask {
            meta_client,
            report_interval: heartbeat_opts.interval.as_millis() as u64,
            retry_interval: heartbeat_opts.retry_interval.as_millis() as u64,
            resp_handler_executor,
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

        let _handle = common_runtime::spawn_bg(async move {
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
                        capture_self
                            .start_with_retry(Duration::from_millis(retry_interval))
                            .await;

                        break;
                    }
                }
            }
        });
    }

    fn start_heartbeat_report(
        &self,
        req_sender: HeartbeatSender,
        mut outgoing_rx: Receiver<OutgoingMessage>,
    ) {
        let report_interval = self.report_interval;

        common_runtime::spawn_bg(async move {
            let sleep = tokio::time::sleep(Duration::from_millis(0));
            tokio::pin!(sleep);

            loop {
                let req = tokio::select! {
                    message = outgoing_rx.recv() => {
                        if let Some(message) = message {
                            match outgoing_message_to_mailbox_message(message) {
                                Ok(message) => {
                                    let req = HeartbeatRequest {
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
                            // Receives None that means Sender was dropped, we need to break the current loop
                            break
                        }
                    }
                    _ = &mut sleep => {
                        sleep.as_mut().reset(Instant::now() + Duration::from_millis(report_interval));
                        let req = HeartbeatRequest {
                            ..Default::default()
                        };
                        Some(req)
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
