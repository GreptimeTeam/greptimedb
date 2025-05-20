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

use common_telemetry::{debug, error, info};
use common_wal::config::kafka::common::{
    KafkaConnectionConfig, KafkaTopicConfig, DEFAULT_BACKOFF_CONFIG,
};
use rskafka::client::error::Error as RsKafkaError;
use rskafka::client::error::ProtocolError::TopicAlreadyExists;
use rskafka::client::partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::record::Record;
use snafu::ResultExt;

use crate::error::{
    BuildKafkaClientSnafu, BuildKafkaCtrlClientSnafu, CreateKafkaWalTopicSnafu,
    KafkaGetOffsetSnafu, KafkaPartitionClientSnafu, ProduceRecordSnafu, ResolveKafkaEndpointSnafu,
    Result, TlsConfigSnafu,
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
                    error!(e; "Failed to create a topic {}", topic);
                    Err(e).context(CreateKafkaWalTopicSnafu)
                }
            }
        }
    }

    async fn prepare_topic(&self, topic: &String) -> Result<()> {
        let partition_client = self.partition_client(topic).await?;
        self.append_noop_record(topic, &partition_client).await?;
        Ok(())
    }

    /// Creates a [PartitionClient] for the given topic.
    async fn partition_client(&self, topic: &str) -> Result<PartitionClient> {
        self.client
            .partition_client(topic, DEFAULT_PARTITION, UnknownTopicHandling::Retry)
            .await
            .context(KafkaPartitionClientSnafu {
                topic,
                partition: DEFAULT_PARTITION,
            })
    }

    /// Appends a noop record to the topic.
    /// It only appends a noop record if the topic is empty.
    async fn append_noop_record(
        &self,
        topic: &String,
        partition_client: &PartitionClient,
    ) -> Result<()> {
        let end_offset = partition_client
            .get_offset(OffsetAt::Latest)
            .await
            .context(KafkaGetOffsetSnafu {
                topic: topic.to_string(),
                partition: DEFAULT_PARTITION,
            })?;
        if end_offset > 0 {
            return Ok(());
        }

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
        debug!("Appended a noop record to topic {}", topic);

        Ok(())
    }

    /// Creates topics in Kafka.
    pub async fn create_topics(&self, topics: &[String]) -> Result<()> {
        let tasks = topics
            .iter()
            .map(|topic| async { self.create_topic(topic, &self.client).await })
            .collect::<Vec<_>>();
        futures::future::try_join_all(tasks).await.map(|_| ())
    }

    /// Prepares topics in Kafka.
    ///
    /// It appends a noop record to each topic if the topic is empty.
    pub async fn prepare_topics(&self, topics: &[String]) -> Result<()> {
        // Try to create missing topics.
        let tasks = topics
            .iter()
            .map(|topic| async { self.prepare_topic(topic).await })
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

#[cfg(test)]
impl KafkaTopicCreator {
    pub async fn delete_topics(&self, topics: &[String]) -> Result<()> {
        let tasks = topics
            .iter()
            .map(|topic| async { self.delete_topic(topic, &self.client).await })
            .collect::<Vec<_>>();
        futures::future::try_join_all(tasks).await.map(|_| ())
    }

    async fn delete_topic(&self, topic: &String, client: &Client) -> Result<()> {
        let controller = client
            .controller_client()
            .context(BuildKafkaCtrlClientSnafu)?;
        match controller.delete_topic(topic, 10).await {
            Ok(_) => {
                info!("Successfully deleted topic {}", topic);
                Ok(())
            }
            Err(e) => {
                if Self::is_unknown_topic_err(&e) {
                    info!("The topic {} does not exist", topic);
                    Ok(())
                } else {
                    panic!("Failed to delete a topic {}, error: {}", topic, e);
                }
            }
        }
    }

    fn is_unknown_topic_err(e: &RsKafkaError) -> bool {
        matches!(
            e,
            &RsKafkaError::ServerError {
                protocol_error: rskafka::client::error::ProtocolError::UnknownTopicOrPartition,
                ..
            }
        )
    }

    pub async fn get_partition_client(&self, topic: &str) -> PartitionClient {
        self.partition_client(topic).await.unwrap()
    }
}
/// Builds a kafka [Client](rskafka::client::Client).
pub async fn build_kafka_client(connection: &KafkaConnectionConfig) -> Result<Client> {
    // Builds an kafka controller client for creating topics.
    let broker_endpoints = common_wal::resolve_to_ipv4(&connection.broker_endpoints)
        .await
        .context(ResolveKafkaEndpointSnafu)?;
    let mut builder = ClientBuilder::new(broker_endpoints).backoff_config(DEFAULT_BACKOFF_CONFIG);
    if let Some(sasl) = &connection.sasl {
        builder = builder.sasl_config(sasl.config.clone().into_sasl_config());
    };
    if let Some(tls) = &connection.tls {
        builder = builder.tls_config(tls.to_tls_config().await.context(TlsConfigSnafu)?)
    };
    builder
        .build()
        .await
        .with_context(|_| BuildKafkaClientSnafu {
            broker_endpoints: connection.broker_endpoints.clone(),
        })
}

/// Builds a [KafkaTopicCreator].
pub async fn build_kafka_topic_creator(
    connection: &KafkaConnectionConfig,
    kafka_topic: &KafkaTopicConfig,
) -> Result<KafkaTopicCreator> {
    let client = build_kafka_client(connection).await?;
    Ok(KafkaTopicCreator {
        client,
        num_partitions: kafka_topic.num_partitions,
        replication_factor: kafka_topic.replication_factor,
        create_topic_timeout: kafka_topic.create_topic_timeout.as_millis() as i32,
    })
}

#[cfg(test)]
mod tests {
    use common_wal::config::kafka::common::{KafkaConnectionConfig, KafkaTopicConfig};
    use common_wal::maybe_skip_kafka_integration_test;
    use common_wal::test_util::get_kafka_endpoints;

    use super::*;

    async fn test_topic_creator(broker_endpoints: Vec<String>) -> KafkaTopicCreator {
        let connection = KafkaConnectionConfig {
            broker_endpoints,
            ..Default::default()
        };
        let kafka_topic = KafkaTopicConfig::default();

        build_kafka_topic_creator(&connection, &kafka_topic)
            .await
            .unwrap()
    }

    async fn append_records(partition_client: &PartitionClient, num_records: usize) -> Result<()> {
        for i in 0..num_records {
            partition_client
                .produce(
                    vec![Record {
                        key: Some(b"test".to_vec()),
                        value: Some(format!("test {}", i).as_bytes().to_vec()),
                        timestamp: chrono::Utc::now(),
                        headers: Default::default(),
                    }],
                    Compression::Lz4,
                )
                .await
                .unwrap();
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_append_noop_record_to_empty_topic() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_kafka_integration_test!();
        let prefix = "append_noop_record_to_empty_topic";
        let creator = test_topic_creator(get_kafka_endpoints()).await;

        let topic = format!("{}{}", prefix, "0");
        // Clean up the topics before test
        creator.delete_topics(&[topic.to_string()]).await.unwrap();
        creator.create_topics(&[topic.to_string()]).await.unwrap();

        let partition_client = creator.partition_client(&topic).await.unwrap();
        let end_offset = partition_client.get_offset(OffsetAt::Latest).await.unwrap();
        assert_eq!(end_offset, 0);

        // The topic is not empty, so no noop record is appended.
        creator
            .append_noop_record(&topic, &partition_client)
            .await
            .unwrap();
        let end_offset = partition_client.get_offset(OffsetAt::Latest).await.unwrap();
        assert_eq!(end_offset, 1);
    }

    #[tokio::test]
    async fn test_append_noop_record_to_non_empty_topic() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_kafka_integration_test!();
        let prefix = "append_noop_record_to_non_empty_topic";
        let creator = test_topic_creator(get_kafka_endpoints()).await;

        let topic = format!("{}{}", prefix, "0");
        // Clean up the topics before test
        creator.delete_topics(&[topic.to_string()]).await.unwrap();

        creator.create_topics(&[topic.to_string()]).await.unwrap();
        let partition_client = creator.partition_client(&topic).await.unwrap();
        append_records(&partition_client, 2).await.unwrap();

        let end_offset = partition_client.get_offset(OffsetAt::Latest).await.unwrap();
        assert_eq!(end_offset, 2);

        // The topic is not empty, so no noop record is appended.
        creator
            .append_noop_record(&topic, &partition_client)
            .await
            .unwrap();
        let end_offset = partition_client.get_offset(OffsetAt::Latest).await.unwrap();
        assert_eq!(end_offset, 2);
    }

    #[tokio::test]
    async fn test_create_topic() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_kafka_integration_test!();
        let prefix = "create_topic";
        let creator = test_topic_creator(get_kafka_endpoints()).await;

        let topic = format!("{}{}", prefix, "0");
        // Clean up the topics before test
        creator.delete_topics(&[topic.to_string()]).await.unwrap();

        creator.create_topics(&[topic.to_string()]).await.unwrap();
        // Should be ok
        creator.create_topics(&[topic.to_string()]).await.unwrap();

        let partition_client = creator.partition_client(&topic).await.unwrap();
        let end_offset = partition_client.get_offset(OffsetAt::Latest).await.unwrap();
        assert_eq!(end_offset, 0);
    }

    #[tokio::test]
    async fn test_prepare_topic() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_kafka_integration_test!();
        let prefix = "prepare_topic";
        let creator = test_topic_creator(get_kafka_endpoints()).await;

        let topic = format!("{}{}", prefix, "0");
        // Clean up the topics before test
        creator.delete_topics(&[topic.to_string()]).await.unwrap();

        creator.create_topics(&[topic.to_string()]).await.unwrap();
        creator.prepare_topic(&topic).await.unwrap();

        let partition_client = creator.partition_client(&topic).await.unwrap();
        let start_offset = partition_client
            .get_offset(OffsetAt::Earliest)
            .await
            .unwrap();
        assert_eq!(start_offset, 0);

        let end_offset = partition_client.get_offset(OffsetAt::Latest).await.unwrap();
        assert_eq!(end_offset, 1);
    }

    #[tokio::test]
    async fn test_prepare_topic_with_stale_records_without_pruning() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_kafka_integration_test!();

        let prefix = "prepare_topic_with_stale_records_without_pruning";
        let creator = test_topic_creator(get_kafka_endpoints()).await;

        let topic = format!("{}{}", prefix, "0");
        // Clean up the topics before test
        creator.delete_topics(&[topic.to_string()]).await.unwrap();

        creator.create_topics(&[topic.to_string()]).await.unwrap();
        let partition_client = creator.partition_client(&topic).await.unwrap();
        append_records(&partition_client, 10).await.unwrap();

        creator.prepare_topic(&topic).await.unwrap();

        let end_offset = partition_client.get_offset(OffsetAt::Latest).await.unwrap();
        assert_eq!(end_offset, 10);
        let start_offset = partition_client
            .get_offset(OffsetAt::Earliest)
            .await
            .unwrap();
        assert_eq!(start_offset, 0);
    }
}
