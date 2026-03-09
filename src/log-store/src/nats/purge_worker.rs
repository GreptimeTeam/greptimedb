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

//! Background WAL purge worker for the NATS JetStream log store.
//!
//! [`PurgeWorker`] drains the `pending_purge` map at a configurable interval
//! and calls `stream.purge().filter(subject).sequence(n)` for each subject
//! that has an outstanding truncation watermark.

use std::sync::Arc;
use std::time::Duration;

use common_telemetry::{info, warn};
use dashmap::DashMap;
use store_api::logstore::provider::NatsProvider;
use tokio::time;

use crate::nats::client::NatsClientRef;

/// Background task that periodically purges obsolete WAL entries.
pub(crate) struct PurgeWorker {
    client: NatsClientRef,
    /// Map from NATS subject to the highest sequence number that may be purged.
    pending_purge: Arc<DashMap<Arc<NatsProvider>, u64>>,
    interval: Duration,
}

impl PurgeWorker {
    pub(crate) fn new(
        client: NatsClientRef,
        pending_purge: Arc<DashMap<Arc<NatsProvider>, u64>>,
        interval: Duration,
    ) -> Self {
        Self {
            client,
            pending_purge,
            interval,
        }
    }

    /// Spawns the purge worker as a background Tokio task.
    pub(crate) fn run(self) {
        tokio::spawn(async move {
            let mut ticker = time::interval(self.interval);
            // First tick fires immediately; skip it to avoid spurious work on startup.
            ticker.tick().await;

            loop {
                ticker.tick().await;
                self.purge_pending().await;
            }
        });
    }

    async fn purge_pending(&self) {
        // Drain the map into a local vec so we hold the DashMap lock as briefly as possible.
        let to_purge: Vec<(Arc<NatsProvider>, u64)> = self
            .pending_purge
            .iter()
            .map(|e| (e.key().clone(), *e.value()))
            .collect();

        if to_purge.is_empty() {
            return;
        }

        let stream = match self.client.stream().await {
            Ok(s) => s,
            Err(e) => {
                warn!("NATS purge worker: failed to get stream handle: {}", e);
                return;
            }
        };

        for (provider, up_to_seq) in to_purge {
            match stream
                .purge()
                .filter(provider.topic.clone())
                .sequence(up_to_seq)
                .await
            {
                Ok(info) => {
                    info!(
                        "NATS WAL purge: subject={} up_to_seq={} deleted={} messages",
                        provider.topic, up_to_seq, info.purged
                    );
                    // Remove from map only after a successful purge so the
                    // watermark is retried on next tick if there was a failure.
                    self.pending_purge.remove(&provider);
                }
                Err(e) => {
                    warn!(
                        "NATS purge worker: failed to purge subject={} up_to_seq={}: {}",
                        provider.topic, up_to_seq, e
                    );
                }
            }
        }
    }
}
