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

use common_telemetry::{debug, error};
use rskafka::client::partition::OffsetAt;
use snafu::ResultExt;

use crate::error;
use crate::kafka::log_store::TopicStat;
use crate::kafka::worker::BackgroundProducerWorker;

impl BackgroundProducerWorker {
    /// Fetches the latest offset for the topic.
    ///
    /// This function retrieves the topic's latest offset from Kafka and updates the latest offset
    /// in the shared map.
    pub async fn fetch_latest_offset(&mut self) {
        match self
            .client
            .get_offset(OffsetAt::Latest)
            .await
            .context(error::GetOffsetSnafu {
                topic: &self.provider.topic,
            }) {
            Ok(offset) => match self.topic_stats.entry(self.provider.clone()) {
                dashmap::Entry::Occupied(mut occupied_entry) => {
                    let offset = offset as u64;
                    let stat = occupied_entry.get_mut();
                    if stat.latest_offset < offset {
                        stat.latest_offset = offset;
                        debug!(
                            "Updated latest offset for topic {} to {}",
                            self.provider.topic, offset
                        );
                    }
                }
                dashmap::Entry::Vacant(vacant_entry) => {
                    vacant_entry.insert(TopicStat {
                        latest_offset: offset as u64,
                        record_size: 0,
                        record_num: 0,
                    });
                    debug!(
                        "Inserted latest offset for topic {} to {}",
                        self.provider.topic, offset
                    );
                }
            },
            Err(err) => {
                error!(err; "Failed to get latest offset for topic {}", self.provider.topic);
            }
        }
    }
}
