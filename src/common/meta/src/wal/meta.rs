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

use snafu::OptionExt;

use crate::error::{MissingKafkaTopicManagerSnafu, Result};
use crate::kv_backend::KvBackendRef;
use crate::wal::kafka::{KafkaTopic as Topic, KafkaTopicManager as TopicManager};
use crate::wal::{WalOptions, WalProvider};

/// The allocator responsible for allocating wal metadata for a table.
#[derive(Default)]
pub struct WalMetaAllocator {
    wal_provider: WalProvider,
    topic_manager: Option<TopicManager>,
}

impl WalMetaAllocator {
    pub async fn try_new(wal_opts: &WalOptions, kv_backend: &KvBackendRef) -> Result<Self> {
        let mut this = Self {
            wal_provider: wal_opts.provider.clone(),
            ..Default::default()
        };

        match this.wal_provider {
            WalProvider::RaftEngine => {}
            WalProvider::Kafka => {
                let topic_manager =
                    TopicManager::try_new(wal_opts.kafka_opts.as_ref(), kv_backend).await?;
                this.topic_manager = Some(topic_manager);
            }
        }

        Ok(this)
    }

    pub fn wal_provider(&self) -> &WalProvider {
        &self.wal_provider
    }

    pub async fn try_alloc_topics(&self, num_topics: usize) -> Result<Vec<Topic>> {
        let topics = self
            .topic_manager
            .as_ref()
            .context(MissingKafkaTopicManagerSnafu)?
            .select_topics(num_topics);
        Ok(topics)
    }
}
