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
use std::fmt::Debug;
use std::sync::Arc;

use rskafka::client::partition::{Compression, UnknownTopicHandling};
use snafu::ResultExt;
use store_api::logstore::provider::KafkaProvider;
use tokio::sync::RwLock;

use crate::error;
use crate::error::Result;
use crate::kafka::producer::OrderedBatchProducer;

pub type OrderedBatchProducerRef = Arc<OrderedBatchProducer>;

// Each topic only has one partition for now.
// The `DEFAULT_PARTITION` refers to the index of the partition.
const DEFAULT_PARTITION: i32 = 0;

/// The minimum batch size.
pub(crate) const MIN_FLUSH_BATCH_SIZE: usize = 4 * 1024;

/// The registry or [OrderedBatchProducer].
pub struct ProducerRegistry {
    registry: RwLock<HashMap<Arc<KafkaProvider>, OrderedBatchProducerRef>>,
    client: rskafka::client::Client,
    producer_channel_size: usize,
    producer_request_batch_size: usize,
    flush_batch_size: usize,
    compression: Compression,
}

impl Debug for ProducerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ProducerRegistry: <ProducerRegistry>")
    }
}

impl ProducerRegistry {
    pub fn new(
        client: rskafka::client::Client,
        flush_batch_size: usize,
        producer_channel_size: usize,
        producer_request_batch_size: usize,
        compression: Compression,
    ) -> Self {
        let flush_batch_size = flush_batch_size.max(MIN_FLUSH_BATCH_SIZE);
        Self {
            registry: RwLock::new(HashMap::new()),
            client,
            producer_channel_size,
            producer_request_batch_size,
            flush_batch_size,
            compression,
        }
    }

    async fn create_producer(
        &self,
        provider: &Arc<KafkaProvider>,
    ) -> Result<OrderedBatchProducerRef> {
        let partition_client = self
            .client
            .partition_client(
                provider.topic.as_str(),
                DEFAULT_PARTITION,
                UnknownTopicHandling::Retry,
            )
            .await
            .context(error::BuildPartitionClientSnafu {
                topic: &provider.topic,
                partition: DEFAULT_PARTITION,
            })?;

        let producer = OrderedBatchProducer::new(
            Arc::new(partition_client),
            self.compression,
            self.producer_channel_size,
            self.producer_request_batch_size,
            self.flush_batch_size,
        );

        Ok(Arc::new(producer))
    }

    pub async fn get_or_register(
        &self,
        provider: &Arc<KafkaProvider>,
    ) -> Result<OrderedBatchProducerRef> {
        {
            let registry = self.registry.read().await;
            if let Some(client) = registry.get(provider) {
                return Ok(client.clone());
            }
        }

        let mut registry = self.registry.write().await;
        match registry.get(provider) {
            Some(client) => Ok(client.clone()),
            None => {
                // TODO(weny): Avoid to perform networking operations under the write lock
                let producer = self.create_producer(provider).await?;
                registry.insert(provider.clone(), producer.clone());
                Ok(producer)
            }
        }
    }
}
