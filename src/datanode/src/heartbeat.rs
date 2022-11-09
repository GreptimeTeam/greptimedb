use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use api::v1::meta::{HeartbeatRequest, HeartbeatResponse, Peer};
use common_telemetry::{error, info, warn};
use meta_client::client::{HeartbeatSender, MetaClient};
use snafu::ResultExt;

use crate::error::{MetaClientInitSnafu, Result};

#[derive(Debug, Clone, Default)]
pub struct HeartbeatTask {
    node_id: u64,
    server_addr: String,
    running: Arc<AtomicBool>,
    meta_client: MetaClient,
    interval: u64,
}

impl Drop for HeartbeatTask {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
    }
}

impl HeartbeatTask {
    /// Create a new heartbeat task instance.
    pub fn new(node_id: u64, server_addr: String, meta_client: MetaClient) -> Self {
        Self {
            node_id,
            server_addr,
            running: Arc::new(AtomicBool::new(false)),
            meta_client,
            interval: 5_000, // default interval is set to 5 secs
        }
    }

    pub async fn create_streams(
        meta_client: &MetaClient,
        running: Arc<AtomicBool>,
    ) -> Result<HeartbeatSender> {
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
                if !running.load(Ordering::Acquire) {
                    info!("Heartbeat task shutdown");
                }
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
        let server_addr = self.server_addr.clone();
        let meta_client = self.meta_client.clone();

        let mut tx = Self::create_streams(&meta_client, running.clone()).await?;
        common_runtime::spawn_bg(async move {
            while running.load(Ordering::Acquire) {
                let req = HeartbeatRequest {
                    peer: Some(Peer {
                        id: node_id,
                        addr: server_addr.clone(),
                    }),
                    ..Default::default()
                };
                if let Err(e) = tx.send(req).await {
                    error!("Failed to send heartbeat to metasrv, error: {:?}", e);
                    match Self::create_streams(&meta_client, running.clone()).await {
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
