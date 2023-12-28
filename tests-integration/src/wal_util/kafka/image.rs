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

use testcontainers::core::{ContainerState, ExecCommand, WaitFor};

use crate::wal_util::kafka::config::{
    Config, KAFKA_ADVERTISED_LISTENER_PORT, KAFKA_LISTENER_PORT, ZOOKEEPER_PORT,
};

#[derive(Debug, Clone, Default)]
pub struct ImageArgs;

impl testcontainers::ImageArgs for ImageArgs {
    fn into_iterator(self) -> Box<dyn Iterator<Item = String>> {
        Box::new(
            vec![
                "/bin/bash".to_string(),
                "-c".to_string(),
                format!(
                    r#"
                        echo 'clientPort={}' > zookeeper.properties;
                        echo 'dataDir=/var/lib/zookeeper/data' >> zookeeper.properties;
                        echo 'dataLogDir=/var/lib/zookeeper/log' >> zookeeper.properties;
                        zookeeper-server-start zookeeper.properties &
                        . /etc/confluent/docker/bash-config &&
                        /etc/confluent/docker/configure &&
                        /etc/confluent/docker/launch
                    "#,
                    ZOOKEEPER_PORT,
                ),
            ]
            .into_iter(),
        )
    }
}

#[derive(Default)]
pub struct Image {
    config: Config,
}

impl Image {
    #[allow(unused)]
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}

impl testcontainers::Image for Image {
    type Args = ImageArgs;

    fn name(&self) -> String {
        self.config.image_name.clone()
    }

    fn tag(&self) -> String {
        self.config.image_tag.clone()
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        self.config.ready_conditions.clone()
    }

    fn env_vars(&self) -> Box<dyn Iterator<Item = (&String, &String)> + '_> {
        Box::new(self.config.env_vars.iter())
    }

    fn expose_ports(&self) -> Vec<u16> {
        vec![self.config.exposed_port]
    }

    fn exec_after_start(&self, cs: ContainerState) -> Vec<ExecCommand> {
        let mut commands = vec![];
        let cmd = format!(
            "kafka-configs --alter --bootstrap-server 0.0.0.0:{} --entity-type brokers --entity-name 1 --add-config advertised.listeners=[PLAINTEXT://127.0.0.1:{},BROKER://localhost:9092]",
            KAFKA_LISTENER_PORT,
            cs.host_port_ipv4(KAFKA_ADVERTISED_LISTENER_PORT),
        );
        let ready_conditions = vec![WaitFor::message_on_stdout(
            "Checking need to trigger auto leader balancing",
        )];
        commands.push(ExecCommand {
            cmd,
            ready_conditions,
        });
        commands
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use rskafka::chrono::Utc;
    use rskafka::client::partition::UnknownTopicHandling;
    use rskafka::client::ClientBuilder;
    use rskafka::record::Record;
    use testcontainers::clients::Cli as DockerCli;

    use crate::wal_util::kafka::config::{Config, KAFKA_ADVERTISED_LISTENER_PORT};
    use crate::wal_util::kafka::image::Image;

    #[tokio::test]
    async fn test_image() {
        // Starts a Kafka container.
        let port = KAFKA_ADVERTISED_LISTENER_PORT;
        let config = Config::with_exposed_port(port);
        let docker = DockerCli::default();
        let container = docker.run(Image::new(config));

        // Creates a Kafka client.
        let bootstrap_brokers = vec![format!("127.0.0.1:{}", container.get_host_port_ipv4(port))];
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
            timestamp: Utc.timestamp_millis_opt(42).unwrap(),
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
