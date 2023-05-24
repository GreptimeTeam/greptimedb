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

use api::v1::meta::{HeartbeatRequest, NodeStat, Peer};
use catalog::{datanode_stat, CatalogManagerRef};
use common_meta::heartbeat::handler::{
    HeartbeatResponseHandlerContext, HeartbeatResponseHandlerExecutorRef,
};
use common_meta::heartbeat::mailbox::{HeartbeatMailbox, MailboxRef};
use common_meta::heartbeat::utils::outgoing_message_to_mailbox_message;
use common_telemetry::{error, info, trace, warn};
use meta_client::client::{HeartbeatSender, MetaClient};
use snafu::ResultExt;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::error::{self, MetaClientInitSnafu, Result};

pub(crate) mod handler;

pub struct HeartbeatTask {
    node_id: u64,
    server_addr: String,
    server_hostname: Option<String>,
    running: Arc<AtomicBool>,
    meta_client: Arc<MetaClient>,
    catalog_manager: CatalogManagerRef,
    interval: u64,
    heartbeat_response_handler_exector: HeartbeatResponseHandlerExecutorRef,
}

impl Drop for HeartbeatTask {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
    }
}

impl HeartbeatTask {
    /// Create a new heartbeat task instance.
    pub fn new(
        node_id: u64,
        server_addr: String,
        server_hostname: Option<String>,
        meta_client: Arc<MetaClient>,
        catalog_manager: CatalogManagerRef,
        heartbeat_response_handler_exector: HeartbeatResponseHandlerExecutorRef,
    ) -> Self {
        Self {
            node_id,
            server_addr,
            server_hostname,
            running: Arc::new(AtomicBool::new(false)),
            meta_client,
            catalog_manager,
            interval: 5_000, // default interval is set to 5 secs
            heartbeat_response_handler_exector,
        }
    }

    pub async fn create_streams(
        meta_client: &MetaClient,
        running: Arc<AtomicBool>,
        handler_executor: HeartbeatResponseHandlerExecutorRef,
        mailbox: MailboxRef,
    ) -> Result<HeartbeatSender> {
        let client_id = meta_client.id();

        let (tx, mut rx) = meta_client.heartbeat().await.context(MetaClientInitSnafu)?;
        common_runtime::spawn_bg(async move {
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

                let ctx = HeartbeatResponseHandlerContext::new(mailbox.clone(), res);
                if let Err(e) = Self::handle_response(ctx, handler_executor.clone()) {
                    error!(e;"Error while handling heartbeat response");
                }
                if !running.load(Ordering::Acquire) {
                    info!("Heartbeat task shutdown");
                }
            }
            info!("Heartbeat handling loop exit.")
        });
        Ok(tx)
    }

    fn handle_response(
        ctx: HeartbeatResponseHandlerContext,
        handler_executor: HeartbeatResponseHandlerExecutorRef,
    ) -> Result<()> {
        trace!("heartbeat response: {:?}", ctx.response);
        handler_executor
            .handle(ctx)
            .context(error::HandleHeartbeatResponseSnafu)
    }

    /// Start heartbeat task, spawn background task.
    pub async fn start(&self) -> Result<()> {
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
        let addr = resolve_addr(&self.server_addr, &self.server_hostname);
        info!("Starting heartbeat to Metasrv with interval {interval}. My node id is {node_id}, address is {addr}.");

        let meta_client = self.meta_client.clone();
        let catalog_manager_clone = self.catalog_manager.clone();

        let handler_executor = self.heartbeat_response_handler_exector.clone();

        let (outgoing_tx, mut outgoing_rx) = mpsc::channel(16);
        let mailbox = Arc::new(HeartbeatMailbox::new(outgoing_tx));

        let mut tx = Self::create_streams(
            &meta_client,
            running.clone(),
            handler_executor.clone(),
            mailbox.clone(),
        )
        .await?;

        common_runtime::spawn_bg(async move {
            let sleep = tokio::time::sleep(Duration::from_millis(0));
            tokio::pin!(sleep);

            loop {
                if !running.load(Ordering::Acquire) {
                    info!("shutdown heartbeat task");
                    break;
                }
                let req = tokio::select! {
                    message = outgoing_rx.recv() => {
                        if let Some(message) = message {
                            match outgoing_message_to_mailbox_message(message) {
                                Ok(message) => {
                                    let req = HeartbeatRequest {
                                        peer: Some(Peer {
                                            id: node_id,
                                            addr: addr.clone(),
                                        }),
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
                        let (region_num, region_stats) = datanode_stat(&catalog_manager_clone).await;
                        let req = HeartbeatRequest {
                            peer: Some(Peer {
                                id: node_id,
                                addr: addr.clone(),
                            }),
                            node_stat: Some(NodeStat {
                                region_num: region_num as _,
                                ..Default::default()
                            }),
                            region_stats,
                            ..Default::default()
                        };
                        sleep.as_mut().reset(Instant::now() + Duration::from_millis(interval));
                        Some(req)
                    }
                };
                if let Some(req) = req {
                    if let Err(e) = tx.send(req).await {
                        error!("Failed to send heartbeat to metasrv, error: {:?}", e);
                        match Self::create_streams(
                            &meta_client,
                            running.clone(),
                            handler_executor.clone(),
                            mailbox.clone(),
                        )
                        .await
                        {
                            Ok(new_tx) => {
                                info!("Reconnected to metasrv");
                                tx = new_tx;
                            }
                            Err(e) => {
                                error!(e;"Failed to reconnect to metasrv!");
                            }
                        }
                    }
                }
            }
        });

        Ok(())
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
