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

use common_telemetry::warn;
use rskafka::client::partition::Compression;
use rskafka::client::producer::ProducerClient;
use snafu::ResultExt;

use crate::error;
use crate::kafka::collector::IndexCollector;
use crate::kafka::worker::{BackgroundProducerWorker, PendingRequest};

async fn do_flush(
    client: &Arc<dyn ProducerClient>,
    collector: &mut Box<dyn IndexCollector>,
    PendingRequest {
        batch,
        region_ids,
        sender,
        size: _size,
    }: PendingRequest,
    compression: Compression,
) {
    let result = client
        .produce(batch, compression)
        .await
        .context(error::BatchProduceSnafu);

    if let Ok(result) = &result {
        for (idx, region_id) in result.iter().zip(region_ids) {
            collector.append(region_id, *idx as u64);
        }
    }

    if let Err(err) = sender.send(result) {
        warn!(err; "BatchFlushState Receiver is dropped");
    }
}

impl BackgroundProducerWorker {
    pub(crate) async fn try_flush_pending_requests(&mut self) {
        // Processes pending requests first.
        if !self.pending_requests.is_empty() {
            // TODO(weny): Considering merge `PendingRequest`s.
            for req in self.pending_requests.drain(..) {
                do_flush(
                    &self.client,
                    &mut self.index_collector,
                    req,
                    self.compression,
                )
                .await
            }
        }
    }
}
