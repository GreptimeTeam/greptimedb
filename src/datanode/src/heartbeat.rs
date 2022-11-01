use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use api::v1::meta::{HeartbeatRequest, Peer};
use common_telemetry::{error, info};
use meta_client::client::MetaClient;
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

    /// Start heartbeat task, spawn background task.
    pub async fn start(&self) -> Result<()> {
        let (tx, mut rx) = self
            .metasrv_client
            .heartbeat()
            .await
            .context(MetaClientInitSnafu)?;
        let started = self.started.clone();
        started.store(true, Ordering::Release);
        let interval = self.interval;

        let node_id = self.node_id;
        let server_addr = self.server_addr.clone();

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
                }
                tokio::time::sleep(Duration::from_millis(interval)).await;
            }
        });

        common_runtime::spawn_bg(async move {
            while let Some(res) = rx.message().await.unwrap() {
                info!("heartbeat response: {:#?}", res);
            }
        });

        Ok(())
    }
}
