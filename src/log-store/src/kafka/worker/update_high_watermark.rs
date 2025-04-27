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
use crate::kafka::worker::BackgroundProducerWorker;

impl BackgroundProducerWorker {
    /// Updates the high watermark for the topic.
    ///
    /// This function retrieves the latest offset from Kafka and updates the high watermark
    /// in the shared map.
    pub async fn update_high_watermark(&mut self) {
        match self
            .client
            .get_offset(OffsetAt::Latest)
            .await
            .context(error::GetOffsetSnafu {
                topic: &self.provider.topic,
            }) {
            Ok(offset) => match self.high_watermark.entry(self.provider.clone()) {
                dashmap::Entry::Occupied(mut occupied_entry) => {
                    let offset = offset as u64;
                    if *occupied_entry.get() != offset {
                        occupied_entry.insert(offset);
                        debug!(
                            "Updated high watermark for topic {} to {}",
                            self.provider.topic, offset
                        );
                    }
                }
                dashmap::Entry::Vacant(vacant_entry) => {
                    vacant_entry.insert(offset as u64);
                    debug!(
                        "Inserted high watermark for topic {} to {}",
                        self.provider.topic, offset
                    );
                }
            },
            Err(err) => {
                error!(err; "Failed to get offset for topic {}", self.provider.topic);
            }
        }
    }
}
