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

use std::collections::HashMap;
use std::sync::Arc;

use common_wal::config::kafka::DatanodeKafkaConfig;
use rskafka::client::partition::{PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::BackoffConfig;
use snafu::ResultExt;
use tokio::sync::RwLock;

use crate::error::{BuildClientSnafu, BuildPartitionClientSnafu, Result};

/// Each topic only has one partition and the default partition is the 0-th partition.
const DEFAULT_PARTITION: i32 = 0;

/// Manages accesses on partition clients. Each partition client associates with a specific topic.
#[derive(Debug)]
pub struct ClientManager {
    client_factory: Client,
    client_pool: RwLock<HashMap<String, Arc<PartitionClient>>>,
}

impl ClientManager {
    /// Tries to build a client manager.
    pub async fn try_new(config: &DatanodeKafkaConfig) -> Result<Self> {
        let client = ClientBuilder::new(config.broker_endpoints.clone())
            .backoff_config(BackoffConfig {
                init_backoff: config.backoff.init,
                max_backoff: config.backoff.max,
                base: config.backoff.base as f64,
                deadline: config.backoff.deadline,
            })
            .build()
            .await
            .with_context(|_| BuildClientSnafu {
                broker_endpoints: config.broker_endpoints.clone(),
            })?;
        Ok(Self {
            client_factory: client,
            client_pool: RwLock::new(HashMap::new()),
        })
    }

    /// Gets the partition client associated with the given topic. Constructs the client if it does not exist yet.
    pub async fn get_or_insert(&self, topic: &str) -> Result<Arc<PartitionClient>> {
        {
            let client_pool = self.client_pool.read().await;
            if let Some(client) = client_pool.get(topic) {
                return Ok(client.clone());
            }
        }

        let mut client_pool = self.client_pool.write().await;
        match client_pool.get(topic) {
            Some(client) => Ok(client.clone()),
            None => {
                let client = self
                    .client_factory
                    .partition_client(topic, DEFAULT_PARTITION, UnknownTopicHandling::Retry)
                    .await
                    .map(Arc::new)
                    .with_context(|_| BuildPartitionClientSnafu {
                        topic,
                        partition: DEFAULT_PARTITION,
                    })?;
                client_pool.insert(topic.to_string(), client.clone());
                Ok(client)
            }
        }
    }
}
