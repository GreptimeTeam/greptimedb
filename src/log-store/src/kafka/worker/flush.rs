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

use common_telemetry::warn;
use snafu::ResultExt;

use crate::error;
use crate::kafka::log_store::TopicStat;
use crate::kafka::worker::{BackgroundProducerWorker, PendingRequest};

impl BackgroundProducerWorker {
    async fn do_flush(
        &mut self,
        PendingRequest {
            batch,
            region_ids,
            sender,
            size: _size,
        }: PendingRequest,
    ) {
        let record_num = batch.len() as u64;
        let result = self
            .client
            .produce(batch, self.compression)
            .await
            .context(error::BatchProduceSnafu);

        if let Ok(result) = &result {
            let total_record_size = result.encoded_request_size as u64;
            for (idx, region_id) in result.offsets.iter().zip(region_ids) {
                self.index_collector.append(region_id, *idx as u64);
            }

            let max_offset = result.offsets.iter().max().cloned().unwrap_or_default() as u64;
            self.topic_stats
                .entry(self.provider.clone())
                .and_modify(|stat| {
                    stat.latest_offset = stat.latest_offset.max(max_offset);
                    stat.record_size += total_record_size;
                    stat.record_num += record_num;
                })
                .or_insert(TopicStat {
                    latest_offset: max_offset,
                    record_size: total_record_size,
                    record_num,
                });
        }

        if let Err(err) = sender.send(result.map(|r| r.offsets)) {
            warn!(err; "BatchFlushState Receiver is dropped");
        }
    }

    pub(crate) async fn try_flush_pending_requests(
        &mut self,
        pending_requests: Vec<PendingRequest>,
    ) {
        // TODO(weny): Considering merge `PendingRequest`s.
        for req in pending_requests {
            self.do_flush(req).await
        }
    }
}
