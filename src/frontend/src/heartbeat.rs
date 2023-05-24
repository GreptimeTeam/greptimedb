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
use std::time::Duration;

use api::v1::meta::{HeartbeatRequest, HeartbeatResponse};
use common_telemetry::tracing::trace;
use common_telemetry::{error, info};
use meta_client::client::{HeartbeatSender, HeartbeatStream, MetaClient};
use snafu::ResultExt;

use crate::error;
use crate::error::Result;

pub mod handler;

#[derive(Clone)]
pub struct HeartbeatTask {
    meta_client: Arc<MetaClient>,
    report_interval: u64,
    retry_interval: u64,
}

impl HeartbeatTask {
    pub fn new(meta_client: Arc<MetaClient>, report_interval: u64, retry_interval: u64) -> Self {
        HeartbeatTask {
            meta_client,
            report_interval,
            retry_interval,
        }
    }

    pub async fn start(&self) -> Result<()> {
        let (req_sender, resp_stream) = self
            .meta_client
            .heartbeat()
            .await
            .context(error::CreateMetaHeartbeatStreamSnafu)?;

        info!("A heartbeat connection has been established with metasrv");

        self.start_handle_resp_stream(resp_stream);

        self.start_heartbeat_report(req_sender);

        Ok(())
    }

    fn start_handle_resp_stream(&self, mut resp_stream: HeartbeatStream) {
        let capture_self = self.clone();
        let retry_interval = self.retry_interval;

        common_runtime::spawn_bg(async move {
            loop {
                match resp_stream.message().await {
                    Ok(Some(resp)) => capture_self.handle_response(resp).await,
                    Ok(None) => break,
                    Err(e) => {
                        error!(e; "Occur error while reading heartbeat response");

                        capture_self
                            .start_with_retry(Duration::from_secs(retry_interval))
                            .await;

                        break;
                    }
                }
            }
        });
    }

    fn start_heartbeat_report(&self, req_sender: HeartbeatSender) {
        let report_interval = self.report_interval;

        common_runtime::spawn_bg(async move {
            loop {
                let req = HeartbeatRequest::default();

                if let Err(e) = req_sender.send(req.clone()).await {
                    error!(e; "Failed to send heartbeat to metasrv");
                    break;
                } else {
                    trace!("Send a heartbeat request to metasrv, content: {:?}", req);
                }

                tokio::time::sleep(Duration::from_secs(report_interval)).await;
            }
        });
    }

    async fn handle_response(&self, resp: HeartbeatResponse) {
        trace!("Received a heartbeat response: {:?}", resp);
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
