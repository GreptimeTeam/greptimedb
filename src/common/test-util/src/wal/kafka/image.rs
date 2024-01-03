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

use testcontainers::core::{ContainerState, ExecCommand, WaitFor};

const IMAGE_NAME: &str = "confluentinc/cp-kafka";
const IMAGE_TAG: &str = "7.4.3";
/// Through which port the Zookeeper node listens for external traffics, e.g. traffics from the Kafka node.
const ZOOKEEPER_PORT: u16 = 2181;
/// Through which port the Kafka node listens for internal traffics, i.e. traffics between Kafka nodes in the same Kafka cluster.
const KAFKA_LISTENER_PORT: u16 = 19092;
/// Through which port the Kafka node listens for external traffics, e.g. traffics from Kafka clients.
const KAFKA_ADVERTISED_LISTENER_PORT: u16 = 9092;

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

pub struct Image {
    env_vars: HashMap<String, String>,
}

impl Default for Image {
    fn default() -> Self {
        Self {
            env_vars: build_env_vars(),
        }
    }
}

impl testcontainers::Image for Image {
    type Args = ImageArgs;

    /// The name of the Kafka image hosted in the docker hub.
    fn name(&self) -> String {
        IMAGE_NAME.to_string()
    }

    /// The tag of the kafka image hosted in the docker hub.
    /// Warning: please use a tag with long-term support. Do not use `latest` or any other tags that
    /// the underlying image may suddenly change.
    fn tag(&self) -> String {
        IMAGE_TAG.to_string()
    }

    /// The runtime is regarded ready to be used if all ready conditions are met.
    /// Warning: be sure to update the conditions when necessary if the image is altered.
    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stdout(
            "started (kafka.server.KafkaServer)",
        )]
    }

    /// The environment variables required to run the runtime.
    /// Warning: be sure to update the environment variables when necessary if the image is altered.
    fn env_vars(&self) -> Box<dyn Iterator<Item = (&String, &String)> + '_> {
        Box::new(self.env_vars.iter())
    }

    /// The runtime is running in a docker container and has its own network. In order to be used by the host machine,
    /// the runtime must expose an internal port. For e.g. assume the runtime has an internal port 9092,
    /// and the `exposed_port` is set to 9092, then the host machine can get a mapped external port with
    /// `container.get_host_port_ipv4(exposed_port)`. With the mapped port, the host machine could connect with the runtime.
    fn expose_ports(&self) -> Vec<u16> {
        vec![KAFKA_ADVERTISED_LISTENER_PORT]
    }

    /// Specifies a collection of commands to be executed when the container is started.
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

fn build_env_vars() -> HashMap<String, String> {
    [
        (
            "KAFKA_ZOOKEEPER_CONNECT".to_string(),
            format!("localhost:{ZOOKEEPER_PORT}"),
        ),
        (
            "KAFKA_LISTENERS".to_string(),
            format!("PLAINTEXT://0.0.0.0:{KAFKA_ADVERTISED_LISTENER_PORT},BROKER://0.0.0.0:{KAFKA_LISTENER_PORT}"),
        ),
        (
            "KAFKA_ADVERTISED_LISTENERS".to_string(),
            format!("PLAINTEXT://localhost:{KAFKA_ADVERTISED_LISTENER_PORT},BROKER://localhost:{KAFKA_LISTENER_PORT}",),
        ),
        (
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP".to_string(),
            "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT".to_string(),
        ),
        (
            "KAFKA_INTER_BROKER_LISTENER_NAME".to_string(),
            "BROKER".to_string(),
        ),
        ("KAFKA_BROKER_ID".to_string(), "1".to_string()),
        (
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR".to_string(),
            "1".to_string(),
        ),
    ]
    .into()
}

#[cfg(test)]
mod tests {
    use rskafka::chrono::{TimeZone, Utc};
    use rskafka::client::partition::UnknownTopicHandling;
    use rskafka::client::ClientBuilder;
    use rskafka::record::Record;
    use testcontainers::clients::Cli as DockerCli;

    use super::*;

    #[tokio::test]
    async fn test_image() {
        // Starts a Kafka container.
        let port = KAFKA_ADVERTISED_LISTENER_PORT;
        let docker = DockerCli::default();
        let container = docker.run(Image::default());

        // Creates a Kafka client.
        let broker_endpoints = vec![format!("127.0.0.1:{}", container.get_host_port_ipv4(port))];
        let client = ClientBuilder::new(broker_endpoints).build().await.unwrap();

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
