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

use common_telemetry::tracing::warn;
use tokio::sync::oneshot;

use crate::kafka::worker::{
    BackgroundProducerWorker, PendingRequest, ProduceRequest, ProduceResultReceiver,
};

impl BackgroundProducerWorker {
    /// Aggregates records into batches, ensuring that the size of each batch does not exceed a specified maximum (`max_batch_bytes`).
    ///
    /// ## Panic
    /// Panic if any [Record]'s `approximate_size` > `max_batch_bytes`.
    pub(crate) fn aggregate_records(
        &self,
        requests: &mut Vec<ProduceRequest>,
        max_batch_bytes: usize,
    ) -> Vec<PendingRequest> {
        let mut records_buffer = vec![];
        let mut region_ids = vec![];
        let mut batch_size = 0;
        let mut pending_requests = Vec::with_capacity(requests.len());

        for ProduceRequest {
            batch,
            sender,
            region_id,
        } in std::mem::take(requests)
        {
            let mut receiver = ProduceResultReceiver::default();
            for record in batch {
                assert!(record.approximate_size() <= max_batch_bytes);
                // Yields the `PendingRequest` if buffer is full.
                if batch_size + record.approximate_size() > max_batch_bytes {
                    let (tx, rx) = oneshot::channel();
                    pending_requests.push(PendingRequest {
                        batch: std::mem::take(&mut records_buffer),
                        region_ids: std::mem::take(&mut region_ids),
                        size: batch_size,
                        sender: tx,
                    });
                    batch_size = 0;
                    receiver.add_receiver(rx);
                }

                batch_size += record.approximate_size();
                records_buffer.push(record);
                region_ids.push(region_id);
            }
            // The remaining records.
            if batch_size > 0 {
                // Yields `PendingRequest`
                let (tx, rx) = oneshot::channel();
                pending_requests.push(PendingRequest {
                    batch: std::mem::take(&mut records_buffer),
                    region_ids: std::mem::take(&mut region_ids),
                    size: batch_size,
                    sender: tx,
                });
                batch_size = 0;
                receiver.add_receiver(rx);
            }

            if sender.send(receiver).is_err() {
                warn!("The Receiver of ProduceResultReceiver is dropped");
            }
        }
        pending_requests
    }
}
