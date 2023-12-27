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

use testcontainers::clients::Cli as DockerCli;
use testcontainers::Container;

use crate::wal_util::kafka::image::Image;

/// A runtime running a cluster consisting of a single Kafka node and a single ZooKeeper node.
#[derive(Default)]
pub struct Runtime {
    docker: DockerCli,
}

impl Runtime {
    /// Starts the runtime. The runtime terminates when the returned container is dropped.
    pub async fn start(&self) -> Container<Image> {
        self.docker.run(Image::default())
    }
}

#[cfg(test)]
mod tests {
    use rskafka::chrono::Utc;
    use rskafka::client::partition::UnknownTopicHandling;
    use rskafka::client::ClientBuilder;
    use rskafka::record::Record;

    use crate::start_kafka;

    #[tokio::test]
    async fn test_runtime() {
        start_kafka!();

        let bootstrap_brokers = vec![9092.to_string()];
        let client = ClientBuilder::new(bootstrap_brokers).build().await.unwrap();

        // Creates a topic.
        let topic = "test_topic";
        client
            .controller_client()
            .unwrap()
            .create_topic(topic, 1, 1, 500)
            .await
            .unwrap();

        // Produces a record.
        let partition_client = client
            .partition_client(topic, 0, UnknownTopicHandling::Error)
            .await
            .unwrap();
        let produced = vec![Record {
            key: Some(b"111".to_vec()),
            value: Some(b"222".to_vec()),
            timestamp: Utc::now(),
            headers: Default::default(),
        }];
        let offset = partition_client
            .produce(produced.clone(), Default::default())
            .await
            .unwrap()[0];

        // Consumes the record.
        let consumed = partition_client
            .fetch_records(offset, 1..4096, 500)
            .await
            .unwrap()
            .0;
        assert_eq!(produced[0], consumed[0].record);
    }
}
