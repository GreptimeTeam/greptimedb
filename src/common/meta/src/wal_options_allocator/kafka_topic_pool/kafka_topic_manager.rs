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

use common_telemetry::{error, info};
use common_wal::config::kafka::MetasrvKafkaConfig;
use rskafka::client::controller::ControllerClient;
use rskafka::client::error::Error as RsKafkaError;
use rskafka::client::error::ProtocolError::TopicAlreadyExists;
use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::record::Record;
use rskafka::BackoffConfig;
use snafu::ResultExt;

use crate::error::{
    BuildKafkaClientSnafu, BuildKafkaCtrlClientSnafu, BuildKafkaPartitionClientSnafu,
    CreateKafkaWalTopicSnafu, ProduceRecordSnafu, ResolveKafkaEndpointSnafu, Result,
    TlsConfigSnafu,
};

// Each topic only has one partition for now.
// The `DEFAULT_PARTITION` refers to the index of the partition.
const DEFAULT_PARTITION: i32 = 0;

/// Manages topics in kafka.
pub struct TopicKafkaManager {
    pub(super) config: MetasrvKafkaConfig,
}

impl TopicKafkaManager {
    pub fn new(config: MetasrvKafkaConfig) -> Self {
        Self { config }
    }

    async fn try_create_topic(&self, topic: &String, client: &ControllerClient) -> Result<()> {
        match client
            .create_topic(
                topic.clone(),
                self.config.kafka_topic.num_partitions,
                self.config.kafka_topic.replication_factor,
                self.config.kafka_topic.create_topic_timeout.as_millis() as i32,
            )
            .await
        {
            Ok(_) => {
                info!("Successfully created topic {}", topic);
                Ok(())
            }
            Err(e) => {
                if Self::is_topic_already_exist_err(&e) {
                    info!("The topic {} already exists", topic);
                    Ok(())
                } else {
                    error!("Failed to create a topic {}, error {:?}", topic, e);
                    Err(e).context(CreateKafkaWalTopicSnafu)
                }
            }
        }
    }

    async fn try_append_noop_record(&self, topic: &String, client: &Client) -> Result<()> {
        let partition_client = client
            .partition_client(topic, DEFAULT_PARTITION, UnknownTopicHandling::Retry)
            .await
            .context(BuildKafkaPartitionClientSnafu {
                topic,
                partition: DEFAULT_PARTITION,
            })?;

        partition_client
            .produce(
                vec![Record {
                    key: None,
                    value: None,
                    timestamp: chrono::Utc::now(),
                    headers: Default::default(),
                }],
                Compression::Lz4,
            )
            .await
            .context(ProduceRecordSnafu { topic })?;

        Ok(())
    }

    pub async fn try_create_topics(&self, topics: &[&String]) -> Result<()> {
        // Builds an kafka controller client for creating topics.
        let backoff_config = BackoffConfig {
            init_backoff: self.config.backoff.init,
            max_backoff: self.config.backoff.max,
            base: self.config.backoff.base as f64,
            deadline: self.config.backoff.deadline,
        };
        let broker_endpoints =
            common_wal::resolve_to_ipv4(&self.config.connection.broker_endpoints)
                .await
                .context(ResolveKafkaEndpointSnafu)?;
        let mut builder = ClientBuilder::new(broker_endpoints).backoff_config(backoff_config);
        if let Some(sasl) = &self.config.connection.sasl {
            builder = builder.sasl_config(sasl.config.clone().into_sasl_config());
        };
        if let Some(tls) = &self.config.connection.tls {
            builder = builder.tls_config(tls.to_tls_config().await.context(TlsConfigSnafu)?)
        };
        let client = builder
            .build()
            .await
            .with_context(|_| BuildKafkaClientSnafu {
                broker_endpoints: self.config.connection.broker_endpoints.clone(),
            })?;

        let control_client = client
            .controller_client()
            .context(BuildKafkaCtrlClientSnafu)?;

        // Try to create missing topics.
        let tasks = topics
            .iter()
            .map(|topic| async {
                self.try_create_topic(topic, &control_client).await?;
                self.try_append_noop_record(topic, &client).await?;
                Ok(())
            })
            .collect::<Vec<_>>();
        futures::future::try_join_all(tasks).await.map(|_| ())
    }

    fn is_topic_already_exist_err(e: &RsKafkaError) -> bool {
        matches!(
            e,
            &RsKafkaError::ServerError {
                protocol_error: TopicAlreadyExists,
                ..
            }
        )
    }
}
