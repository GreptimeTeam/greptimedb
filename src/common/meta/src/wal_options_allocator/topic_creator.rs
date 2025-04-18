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
use common_wal::config::kafka::common::DEFAULT_BACKOFF_CONFIG;
use common_wal::config::kafka::MetasrvKafkaConfig;
use rskafka::client::error::Error as RsKafkaError;
use rskafka::client::error::ProtocolError::TopicAlreadyExists;
use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::record::Record;
use snafu::ResultExt;

use crate::error::{
    BuildKafkaClientSnafu, BuildKafkaCtrlClientSnafu, BuildKafkaPartitionClientSnafu,
    CreateKafkaWalTopicSnafu, ProduceRecordSnafu, ResolveKafkaEndpointSnafu, Result,
    TlsConfigSnafu,
};

// Each topic only has one partition for now.
// The `DEFAULT_PARTITION` refers to the index of the partition.
const DEFAULT_PARTITION: i32 = 0;

/// Creates topics in kafka.
pub struct KafkaTopicCreator {
    client: Client,
    /// The number of partitions per topic.
    num_partitions: i32,
    /// The replication factor of each topic.
    replication_factor: i16,
    /// The timeout of topic creation in milliseconds.
    create_topic_timeout: i32,
}

impl KafkaTopicCreator {
    pub fn client(&self) -> &Client {
        &self.client
    }

    async fn create_topic(&self, topic: &String, client: &Client) -> Result<()> {
        let controller = client
            .controller_client()
            .context(BuildKafkaCtrlClientSnafu)?;
        match controller
            .create_topic(
                topic,
                self.num_partitions,
                self.replication_factor,
                self.create_topic_timeout,
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

    async fn append_noop_record(&self, topic: &String, client: &Client) -> Result<()> {
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

    /// Prepares topics in Kafka.
    /// 1. Creates missing topics.
    /// 2. Appends a noop record to each topic.
    pub async fn prepare_topics(&self, topics: &[&String]) -> Result<()> {
        // Try to create missing topics.
        let tasks = topics
            .iter()
            .map(|topic| async {
                self.create_topic(topic, &self.client).await?;
                self.append_noop_record(topic, &self.client).await?;
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

/// Builds a kafka [Client](rskafka::client::Client).
pub async fn build_kafka_client(config: &MetasrvKafkaConfig) -> Result<Client> {
    // Builds an kafka controller client for creating topics.
    let broker_endpoints = common_wal::resolve_to_ipv4(&config.connection.broker_endpoints)
        .await
        .context(ResolveKafkaEndpointSnafu)?;
    let mut builder = ClientBuilder::new(broker_endpoints).backoff_config(DEFAULT_BACKOFF_CONFIG);
    if let Some(sasl) = &config.connection.sasl {
        builder = builder.sasl_config(sasl.config.clone().into_sasl_config());
    };
    if let Some(tls) = &config.connection.tls {
        builder = builder.tls_config(tls.to_tls_config().await.context(TlsConfigSnafu)?)
    };
    builder
        .build()
        .await
        .with_context(|_| BuildKafkaClientSnafu {
            broker_endpoints: config.connection.broker_endpoints.clone(),
        })
}

/// Builds a [KafkaTopicCreator].
pub async fn build_kafka_topic_creator(config: &MetasrvKafkaConfig) -> Result<KafkaTopicCreator> {
    let client = build_kafka_client(config).await?;
    Ok(KafkaTopicCreator {
        client,
        num_partitions: config.kafka_topic.num_partitions,
        replication_factor: config.kafka_topic.replication_factor,
        create_topic_timeout: config.kafka_topic.create_topic_timeout.as_millis() as i32,
    })
}
