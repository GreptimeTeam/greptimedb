use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use api::v1::meta::{HeartbeatRequest, HeartbeatResponse, Peer};
use common_telemetry::{error, info};
use meta_client::client::{HeartbeatSender, MetaClient};
use snafu::ResultExt;

use crate::error::{MetaClientInitSnafu, Result};

#[derive(Debug, Clone, Default)]
pub struct HeartbeatTask {
    node_id: u64,
    server_addr: String,
    started: Arc<AtomicBool>,
    metasrv_client: MetaClient,
    interval: u64,
}

impl HeartbeatTask {
    /// Create a new heartbeat task instance.
    pub fn new(node_id: u64, server_addr: String, metasrv_client: MetaClient) -> Self {
        Self {
            node_id,
            server_addr,
            started: Arc::new(AtomicBool::new(false)),
            metasrv_client,
            interval: 5_000, // default interval is set to 5 secs
        }
    }

    pub async fn create_streams(meta_client: &MetaClient) -> Result<HeartbeatSender> {
        let (tx, mut rx) = meta_client.heartbeat().await.context(MetaClientInitSnafu)?;
        common_runtime::spawn_bg(async move {
            while let Some(res) = match rx.message().await {
                Ok(m) => m,
                Err(e) => {
                    error!(e; "Error while reading heartbeat response");
                    None
                }
            } {
                Self::handle_response(res).await;
            }
            info!("Heartbeat handling loop exit.")
        });
        Ok(tx)
    }

    async fn handle_response(resp: HeartbeatResponse) {
        info!("heartbeat response: {:?}", resp);
    }

    /// Start heartbeat task, spawn background task.
    pub async fn start(&self) -> Result<()> {
        let started = self.started.clone();
        started.store(true, Ordering::Release);
        let interval = self.interval;
        let node_id = self.node_id;
        let server_addr = self.server_addr.clone();
        let meta_client = self.metasrv_client.clone();

        let mut tx = Self::create_streams(&meta_client).await?;
        common_runtime::spawn_bg(async move {
            while started.load(Ordering::Acquire) {
                let req = HeartbeatRequest {
                    peer: Some(Peer {
                        id: node_id,
                        addr: server_addr.clone(),
                    }),
                    ..Default::default()
                };
                if let Err(e) = tx.send(req).await {
                    error!("Failed to send heartbeat to metasrv, error: {:?}", e);
                    match Self::create_streams(&meta_client).await {
                        Ok(new_tx) => {
                            info!("Reconnected to metasrv");
                            tx = new_tx;
                        }
                        Err(e) => {
                            error!(e;"Failed to reconnect to metasrv!");
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(interval)).await;
            }
        });

        Ok(())
    }
}
